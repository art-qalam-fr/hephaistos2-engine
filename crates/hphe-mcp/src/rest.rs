//! hphe-rest — passerelle REST de la structure unifiée.
//!
//! Expose TOUS les outils hphe-mcp en HTTP/JSON — c'est l'interface "REST"
//! demandée pour les briques absorbées (orchestrator, nim, memory, ingest).
//!
//! Endpoints :
//!   GET  /health                    -> statut serveur + pipe daemon (public)
//!   GET  /tools                     -> inventaire des outils exposés        [auth]
//!   POST /tools/{name}              -> exécute un outil, body = arguments JSON [auth]
//!   POST /call                      -> {"name": "...", "arguments": {...}}   [auth]
//!
//! Auth (revue sécu) : tout sauf /health exige `Authorization: Bearer <tok>`
//! ou `?token=<tok>`. Token = env HPHE_REST_TOKEN, sinon
//! <dir(HPHE_ORCH_DB)>/hphe-rest.token (lu ou généré+persisté au boot).

use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Json, Response};
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use serde_json::{json, Value};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use hphe_ipc::client::ClientBuilder;
use hphe_mcp::{tools, AccessPolicy};

/// Ingestion automatisée : racines surveillées, re-ingest incrémental
/// périodique. `HPHE_INGEST_ROOTS` = liste séparée par ';' ; la racine du
/// workspace (parent de `.agent`) est utilisée par défaut via `--ingest-root`.
const DEFAULT_INGEST_INTERVAL_S: u64 = 900; // 15 min

#[derive(Parser, Debug)]
#[command(name = "hphe-rest", about = "REST gateway over the unified hphe-mcp toolset")]
struct Args {
    /// Named pipe of the hphe-indexd daemon
    #[arg(long, default_value = r"\\.\pipe\hphe-index", env = "HPHE_PIPE_NAME")]
    pipe_name: String,

    /// Listen address
    #[arg(long, default_value = "127.0.0.1:8792", env = "HPHE_REST_ADDR")]
    addr: String,

    /// Agent profile for .mcp-access policy
    #[arg(long, env = "EVERYTHING_MCP_AGENT")]
    profile: Option<String>,

    /// Racine(s) à ingérer périodiquement (';' séparé). Défaut :
    /// `HPHE_INGEST_ROOTS`, sinon aucune boucle.
    #[arg(long, env = "HPHE_INGEST_ROOTS")]
    ingest_roots: Option<String>,

    /// Dossiers parents scannés pour marqueur `.agent/` (';' séparé) —
    /// tout workspace inité est découvert et ingéré automatiquement.
    #[arg(long, env = "HPHE_INGEST_SCAN")]
    ingest_scan: Option<String>,

    /// Intervalle d'ingestion en secondes (défaut 900 = 15 min)
    #[arg(long, default_value_t = DEFAULT_INGEST_INTERVAL_S, env = "HPHE_INGEST_INTERVAL_S")]
    ingest_interval_s: u64,

    #[arg(long, default_value = "info", env = "RUST_LOG")]
    log_level: String,
}

struct AppState {
    client: hphe_ipc::client::Client,
    policy: AccessPolicy,
    /// Sérialise les écritures de l'index Tantivy : un seul IndexWriter à la
    /// fois. La boucle d'ingestion et les appels manuels se mettent en file
    /// ici au lieu d'échouer en LockBusy.
    ingest_gate: Arc<tokio::sync::Mutex<()>>,
    /// Bearer token exigé sur /tools et /call (jamais logué).
    token: String,
}

/// Fichier token : <dir(HPHE_ORCH_DB)>/hphe-rest.token, sinon <HPHE_DATA_DIR>/.
fn token_file() -> Option<PathBuf> {
    if let Ok(db) = env::var("HPHE_ORCH_DB") {
        if let Some(dir) = std::path::Path::new(&db).parent() {
            return Some(dir.join("hphe-rest.token"));
        }
    }
    env::var("HPHE_DATA_DIR")
        .ok()
        .map(|d| PathBuf::from(d).join("hphe-rest.token"))
}

/// RandomState est seedé par l'OS (SipHash keys) — suffisant pour un token
/// localhost ; le secret vit surtout dans le fichier 0600-equivalent.
fn generate_token() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let nanos = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    };
    let mut s = String::with_capacity(64);
    for i in 0..4u64 {
        let mut h = RandomState::new().build_hasher();
        h.write_u64(i);
        h.write_u128(nanos());
        s.push_str(&format!("{:016x}", h.finish()));
    }
    s
}

/// Résolution : env > fichier existant > généré+persisté. Retourne
/// (token, persisté) — persisté=false si le fichier n'a pas pu être écrit
/// (les appelants ne pourront pas le relire → tout 401, warn au boot).
fn resolve_rest_token() -> (String, bool) {
    if let Ok(t) = env::var("HPHE_REST_TOKEN") {
        let t = t.trim().to_string();
        if !t.is_empty() {
            return (t, true);
        }
    }
    if let Some(f) = token_file() {
        if let Ok(t) = fs::read_to_string(&f) {
            let t = t.trim().to_string();
            if !t.is_empty() {
                return (t, true);
            }
        }
        let t = generate_token();
        if fs::write(&f, format!("{t}\n")).is_ok() {
            return (t, true);
        }
        return (t, false);
    }
    (generate_token(), false)
}

async fn require_token(
    State(st): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let bearer = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().strip_prefix("Bearer ").or_else(|| v.trim().strip_prefix("bearer ")))
        .map(str::trim)
        .map(str::to_string);
    let query = req
        .uri()
        .query()
        .and_then(|q| {
            q.split('&')
                .find_map(|kv| kv.strip_prefix("token="))
                .map(str::to_string)
        });
    let ok = [bearer.as_deref(), query.as_deref()]
        .into_iter()
        .flatten()
        .any(|t| !t.is_empty() && t == st.token);
    if ok {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Outils qui ouvrent un IndexWriter sur le content-index — à passer sous gate.
fn is_index_writer_tool(name: &str) -> bool {
    matches!(name, "content_ingest" | "hephaistos_update" | "hephaistos_update_force")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&args.log_level));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    let client = ClientBuilder::new().pipe_name(&args.pipe_name).build();
    let mut policy = AccessPolicy::load();
    if let Some(p) = args.profile {
        policy.profile = p;
    }
    let (token, token_persisted) = resolve_rest_token();
    if !token_persisted {
        tracing::warn!("hphe-rest: token non persisté dans hphe-rest.token — tout appel non authentifié sera rejeté");
    }
    let ingest_gate = Arc::new(tokio::sync::Mutex::new(()));
    let state = Arc::new(AppState {
        client,
        policy,
        ingest_gate: ingest_gate.clone(),
        token,
    });

    // /health reste public (sondes, scripts de boot) ; /tools + /call exigent
    // le bearer token — un process local quelconque ne doit pas pouvoir
    // invoquer fs/ingest/orchestrateur sans le secret.
    let secured = Router::new()
        .route("/tools", get(list_tools))
        .route("/tools/{name}", post(call_tool))
        .route("/call", post(call_generic))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_token));
    let app = Router::new()
        .route("/health", get(health))
        .merge(secured)
        .with_state(state);

    // Boucle d'ingestion automatisée — remplace la tâche planifiée
    // ingest-workspace.ps1. Racines résolues À CHAQUE cycle :
    //   1) HPHE_INGEST_ROOTS (liste explicite ';')
    //   2) registre persistant <data>/ingest-roots.txt (toute racine passée
    //      à content_ingest/hephaistos_update y est inscrite)
    //   3) HPHE_INGEST_SCAN : parents scannés pour marqueur `.agent/`
    //      (un workspace inité est découvert et ingéré tout seul)
    if args.ingest_roots.is_some() || args.ingest_scan.is_some() {
        let interval = std::time::Duration::from_secs(args.ingest_interval_s.max(30));
        let env_roots: Vec<std::path::PathBuf> = args
            .ingest_roots
            .unwrap_or_default()
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from)
            .collect();
        let scan_dirs: Vec<std::path::PathBuf> = args
            .ingest_scan
            .unwrap_or_default()
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from)
            .collect();
        let loop_gate = ingest_gate.clone();
        tokio::spawn(async move {
            loop {
                let mut roots: Vec<std::path::PathBuf> = env_roots.clone();
                roots.extend(hphe_kit::ingest::read_ingest_registry());
                for parent in &scan_dirs {
                    roots.extend(hphe_kit::ingest::scan_agent_workspaces(parent));
                }
                roots.sort();
                roots.dedup();
                for root in &roots {
                    if !root.exists() {
                        continue;
                    }
                    let tag = root
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("workspace")
                        .to_string();
                    let args = serde_json::json!({"root": root, "project_tag": tag});
                    // Gate par racine : un appel manuel attend au pire l'ingest
                    // d'UNE racine, pas le cycle complet.
                    let _g = loop_gate.lock().await;
                    match tools::execute_tool("content_ingest", &args, &hphe_ipc::client::ClientBuilder::new().build(), &AccessPolicy::load()).await {
                        Ok(v) => tracing::info!(root = %root.display(), result = %v, "auto-ingest done"),
                        Err(e) => tracing::warn!(root = %root.display(), error = %e, "auto-ingest failed"),
                    }
                }
                tokio::time::sleep(interval).await;
            }
        });
    }

    // Boucle de réconciliation orchestrateur — 60s.
    // 1) tâches in_progress/assigned dont le pid dispatché est mort → failed
    //    (l'agent n'a pas appelé update_task : crash, quota, kill utilisateur)
    // 2) ghost reaper toutes les 5 boucles (~5min) : MCP node legacy du kit
    //    (agentMemory/qdrant/zvec/orchestrator-server/monitor.js v1).
    // Le ws-bridge :7477 diffuse ensuite les changements au monitor+backend.
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        let mut round = 0u32;
        loop {
            tick.tick().await;
            match tokio::task::spawn_blocking(|| {
                hphe_kit::orch::reconcile_dead_tasks(180)
            })
            .await
            {
                Ok(ids) if !ids.is_empty() => {
                    tracing::info!(?ids, "reconcile: dead-pid tasks -> failed")
                }
                _ => {}
            }
            round += 1;
            if round % 5 == 0 {
                match tokio::task::spawn_blocking(hphe_kit::orch::reap_ghosts).await {
                    Ok(g) if !g.is_empty() => {
                        tracing::info!(?g, "ghost reaper: killed legacy processes")
                    }
                    _ => {}
                }
            }
        }
    });

    let addr: SocketAddr = args.addr.parse()?;
    tracing::info!(%addr, "hphe-rest listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(st): State<Arc<AppState>>) -> Json<Value> {
    let probe_client = ClientBuilder::new()
        .pipe_name(&st.client.config().pipe_name)
        .connect_timeout_ms(250)
        .request_timeout_ms(500)
        .max_retries(1)
        .build();
    // true si la boucle/manuel tient le writer d'index (appels ingest mis en file)
    let busy = st.ingest_gate.try_lock().is_err();
    bounded_health(
        tools::execute_tool("hphe_health", &json!({}), &probe_client, &st.policy),
        busy,
        std::time::Duration::from_secs(1),
    ).await
}

async fn bounded_health(
    probe: impl std::future::Future<Output = Result<Value, String>>,
    busy: bool,
    timeout: std::time::Duration,
) -> Json<Value> {
    match tokio::time::timeout(timeout, probe).await {
        Ok(Ok(mut v)) => {
            v.as_object_mut()
                .map(|o| o.insert("ingest_busy".into(), json!(busy)));
            Json(v)
        }
        Ok(Err(e)) => Json(json!({"status": "error", "error": e, "ingest_busy": busy})),
        Err(_) => Json(json!({"status": "degraded", "pipe_connected": false,
            "daemon_error": "Health probe timed out", "ingest_busy": busy})),
    }
}

#[cfg(test)]
mod health_tests {
    use super::*;

    #[tokio::test]
    async fn stalled_probe_returns_degraded_instead_of_stalling_http() {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            bounded_health(std::future::pending(), true, std::time::Duration::from_millis(10)),
        ).await.unwrap();
        assert_eq!(result.0["status"], "degraded");
        assert_eq!(result.0["pipe_connected"], false);
        assert_eq!(result.0["ingest_busy"], true);
    }

    #[tokio::test]
    async fn successful_probe_keeps_measured_health_fields() {
        let result = bounded_health(
            async { Ok(json!({"status": "healthy", "pipe_connected": true})) },
            false,
            std::time::Duration::from_secs(1),
        ).await;
        assert_eq!(result.0["status"], "healthy");
        assert_eq!(result.0["pipe_connected"], true);
        assert_eq!(result.0["ingest_busy"], false);
    }
}

async fn list_tools() -> Json<Value> {
    let defs = tools::get_tool_definitions();
    let names: Vec<Value> = defs
        .iter()
        .map(|t| json!({"name": t.name, "description": t.description}))
        .collect();
    Json(json!({"count": names.len(), "tools": names}))
}

async fn call_tool(
    State(st): State<Arc<AppState>>,
    Path(name): Path<String>,
    body: Option<Json<Value>>,
) -> (StatusCode, Json<Value>) {
    let args = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    run_tool(&st, &name, args).await
}

async fn call_generic(
    State(st): State<Arc<AppState>>,
    body: Json<Value>,
) -> (StatusCode, Json<Value>) {
    let name = body["name"].as_str().unwrap_or("").to_string();
    let args = body["arguments"].clone();
    run_tool(&st, &name, args).await
}

async fn run_tool(
    st: &Arc<AppState>,
    name: &str,
    args: Value,
) -> (StatusCode, Json<Value>) {
    if !tools::get_tool_definitions().iter().any(|t| t.name == name) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("unknown tool {}", name)})),
        );
    }
    // File d'attente d'écriture : si la boucle ingère, l'appel attend son tour
    // (au pire la durée d'ingest d'une racine) au lieu de LockBusy immédiat.
    let _ingest_guard = if is_index_writer_tool(name) {
        Some(st.ingest_gate.lock().await)
    } else {
        None
    };
    match tools::execute_tool(name, &args, &st.client, &st.policy).await {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e, "tool": name})),
        ),
    }
}
