//! Orchestrateur unifié — absorbé depuis `orchestrator-server` (Node).
//!
//! Même schéma, même fichier : `orchestrator.db` (rusqlite bundled).
//! Porte : tasks, agents, prompt_templates, provider_prefs, model_health,
//! worktrees, task_events, dispatch (spawn argv sans shell, shim .cmd).

use once_cell::sync::Lazy;
use rusqlite::{params, Connection};
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;

use crate::models::available_models;
use crate::orch_db_path;

static DB: Lazy<Mutex<Option<Connection>>> = Lazy::new(|| Mutex::new(None));

fn with_db<T>(f: impl FnOnce(&Connection) -> Result<T, String>) -> Result<T, String> {
    let mut guard = DB.lock().map_err(|_| "db lock")?;
    if guard.is_none() {
        let path = orch_db_path();
        let conn = Connection::open(&path)
            .map_err(|e| format!("open {}: {}", path.display(), e))?;
        conn.pragma_update(None, "busy_timeout", 5000)
            .map_err(|e| e.to_string())?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| e.to_string())?;
        seed_templates(&conn)?;
        ensure_schema(&conn)?;
        *guard = Some(conn);
    }
    f(guard.as_ref().unwrap())
}

/// Colonnes additives post-unification. Le schéma réel vit dans la DB legacy
/// (aucun CREATE TABLE côté Rust) — migration idempotente : PRAGMA table_info
/// garde l'ALTER TABLE ; table absente → no-op (pas de régression sur DB neuve).
fn ensure_schema(conn: &Connection) -> Result<(), String> {
    let cols = |table: &str| -> Result<Vec<String>, String> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({})", table))
            .map_err(|e| e.to_string())?;
        let names = stmt
            .query_map([], |r| r.get::<_, String>(1))
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        Ok(names)
    };
    let agent_cols = cols("agents")?;
    if !agent_cols.is_empty() && !agent_cols.iter().any(|c| c == "capabilities") {
        conn.execute("ALTER TABLE agents ADD COLUMN capabilities TEXT", [])
            .map_err(|e| e.to_string())?;
        // Seeds : workers NIM = pont texte sans outils FS ; les autres ont fs+cmd.
        conn.execute(
            "UPDATE agents SET capabilities='[\"text_only\"]' WHERE agent_name IN ('nim1','nim2')",
            [],
        )
        .map_err(|e| e.to_string())?;
        conn.execute(
            "UPDATE agents SET capabilities='[\"fs_read\",\"fs_write\",\"cmd\"]' WHERE capabilities IS NULL",
            [],
        )
        .map_err(|e| e.to_string())?;
    }
    let task_cols = cols("tasks_todo")?;
    if !task_cols.is_empty() && !task_cols.iter().any(|c| c == "expected_artifact") {
        conn.execute("ALTER TABLE tasks_todo ADD COLUMN expected_artifact TEXT", [])
            .map_err(|e| e.to_string())?;
    }
    if !task_cols.is_empty() && !task_cols.iter().any(|c| c == "verified_by") {
        conn.execute("ALTER TABLE tasks_todo ADD COLUMN verified_by TEXT", [])
            .map_err(|e| e.to_string())?;
    }
    // FR-08 : heartbeat worker (format datetime('now') — comparable aux
    // cutoffs datetime() du reconcile ; PAS now_iso() qui a 'T'/'Z').
    if !task_cols.is_empty() && !task_cols.iter().any(|c| c == "last_heartbeat") {
        conn.execute("ALTER TABLE tasks_todo ADD COLUMN last_heartbeat TEXT", [])
            .map_err(|e| e.to_string())?;
    }
    // Audit F3 : racine de confinement des expected_artifact (racine du projet
    // pour lequel la tache travaille — pas le repertoire du serveur MCP).
    if !task_cols.is_empty() && !task_cols.iter().any(|c| c == "workspace") {
        conn.execute("ALTER TABLE tasks_todo ADD COLUMN workspace TEXT", [])
            .map_err(|e| e.to_string())?;
    }
    // Audit C2 : registre disciplinaire historique — porte la logique de
    // l'ancien agent-discipline.mjs dans le tool MCP (schema identique).
    conn.execute(
        "CREATE TABLE IF NOT EXISTS agent_discipline (\
            agent_name TEXT PRIMARY KEY,\
            score INTEGER NOT NULL DEFAULT 100,\
            status TEXT NOT NULL DEFAULT 'nominal',\
            last_penalty INTEGER DEFAULT 0,\
            last_reason TEXT,\
            penalties_count INTEGER DEFAULT 0,\
            rewards_count INTEGER DEFAULT 0,\
            updated_at TEXT NOT NULL\
        )",
        [],
    )
    .map_err(|e| e.to_string())?;
    // FR-09 : pairage binôme durable — survit au restart IDE et au-delà du
    // TTL 15min des handshakes comms.jsonl (rejoin sans handshake).
    conn.execute(
        "CREATE TABLE IF NOT EXISTS pairs (\
            pair_id TEXT PRIMARY KEY,\
            agent_a TEXT NOT NULL,\
            agent_b TEXT NOT NULL,\
            session TEXT,\
            transport_a TEXT,\
            transport_b TEXT,\
            cascade_a TEXT,\
            cascade_b TEXT,\
            model TEXT,\
            established_at TEXT,\
            last_seen TEXT,\
            meta TEXT\
        )",
        [],
    )
    .map_err(|e| e.to_string())?;
    let pref_cols = cols("provider_prefs")?;
    if !pref_cols.is_empty() && !pref_cols.iter().any(|c| c == "disabled_at") {
        conn.execute("ALTER TABLE provider_prefs ADD COLUMN disabled_at TEXT", [])
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn now_iso() -> String {
    // ISO-8601 UTC sans chrono
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = secs / 86400;
    let rem = secs % 86400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, m, s)
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// task_id tolérant : accepte `"42"` (string) ou `42` (nombre).
fn arg_task_id(args: &Value) -> Result<String, String> {
    match &args["task_id"] {
        Value::String(s) if !s.is_empty() => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => args["id"]
            .as_str()
            .map(String::from)
            .or_else(|| args["id"].as_i64().map(|n| n.to_string()))
            .ok_or_else(|| "task_id requis".to_string()),
    }
}

fn log_event(conn: &Connection, task_id: &str, event: &str, detail: &str) {
    let _ = conn.execute(
        "INSERT INTO task_events (task_id, event, detail, ts) VALUES (?,?,?,?)",
        params![task_id, event, detail, now_iso()],
    );
}

fn task_json(r: &rusqlite::Row) -> rusqlite::Result<Value> {
    Ok(json!({
        "id": r.get::<_, i64>(0)?,
        "title": r.get::<_, String>(1)?,
        "description": r.get::<_, Option<String>>(2)?,
        "priority": r.get::<_, Option<String>>(3)?,
        "status": r.get::<_, Option<String>>(4)?,
        "due_date": r.get::<_, Option<String>>(5)?,
        "created_at": r.get::<_, Option<String>>(6)?,
        "dependencies": r.get::<_, Option<String>>(7)?,
        "assigned_to": r.get::<_, Option<i64>>(8)?,
        "result": r.get::<_, Option<String>>(9)?,
        "task_type": r.get::<_, Option<String>>(10)?,
        "model": r.get::<_, Option<String>>(11)?,
        "provider": r.get::<_, Option<String>>(12)?,
        "dispatcher": r.get::<_, Option<String>>(13)?,
        "expected_artifact": r.get::<_, Option<String>>(14)?,
        "workspace": r.get::<_, Option<String>>(15)?,
    }))
}

fn row_to_task(stmt: &mut rusqlite::Statement) -> Result<Vec<Value>, String> {
    let rows = stmt
        .query_map([], |r| task_json(r))
        .map_err(|e| e.to_string())?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

const TASK_COLS: &str =
    "id, title, description, priority, status, due_date, created_at, dependencies, \
     assigned_to, result, task_type, model, provider, dispatcher, expected_artifact, workspace";

// ===================== Agents =====================

const AGENT_COLS: &str = "id, agent_name, agent_type, config, active, capabilities";

fn row_to_agent(r: &rusqlite::Row) -> rusqlite::Result<Value> {
    Ok(json!({
        "id": r.get::<_, i64>(0)?,
        "agent_name": r.get::<_, String>(1)?,
        "agent_type": r.get::<_, Option<String>>(2)?,
        "config": r.get::<_, Option<String>>(3)?,
        "active": r.get::<_, i64>(4)?,
        "capabilities": r.get::<_, Option<String>>(5)?,
    }))
}

fn find_agent(conn: &Connection, name_or_id: &str) -> Option<Value> {
    conn.query_row(
        &format!("SELECT {} FROM agents WHERE id = ?1 OR agent_name = ?1", AGENT_COLS),
        params![name_or_id],
        row_to_agent,
    )
    .ok()
}

fn agent_cfg(agent: &Value) -> Value {
    agent["config"]
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_else(|| json!({}))
}

fn agent_priority(agent: &Value) -> i64 {
    agent_cfg(agent)["priority"].as_i64().unwrap_or(100)
}

/// Capabilities déclarées de l'agent (colonne JSON : liste de strings).
/// Absent/vide/invalide → permissif (capabilities non configurées = pas de
/// régression ; le gate ne s'applique qu'une fois la colonne renseignée).
fn agent_capabilities(agent: &Value) -> Vec<String> {
    agent["capabilities"]
        .as_str()
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default()
}

/// Capacité requise par task_type : lecture seule (explore/review/audit) ou
/// écriture fichiers (tout le reste — code, debug, test, docs...).
fn required_capability(task_type: &str) -> &'static str {
    match task_type {
        "explore" | "review" | "audit" => "fs_read",
        _ => "fs_write",
    }
}

/// Gate capabilities : refuse le dispatch si l'agent déclare des capabilities
/// qui ne couvrent pas la capacité requise. 'text_only' ne satisfait ni
/// fs_read ni fs_write ; '*' et 'cmd' impliquent les droits complets.
fn check_capabilities(agent: &Value, task_type: &str) -> Result<(), String> {
    let caps = agent_capabilities(agent);
    if caps.is_empty() {
        return Ok(());
    }
    if caps.iter().any(|c| c == "*" || c == "cmd") {
        return Ok(());
    }
    let need = required_capability(task_type);
    let ok = if need == "fs_read" {
        caps.iter().any(|c| c == "fs_read" || c == "fs_write")
    } else {
        caps.iter().any(|c| c == "fs_write")
    };
    if ok {
        Ok(())
    } else {
        Err(format!(
            "capabilities gate: agent '{}' caps={:?} insuffisant pour task_type '{}' (requis: {})",
            agent["agent_name"].as_str().unwrap_or("?"), caps, task_type, need
        ))
    }
}

/// Rang chaîne d'un agent = position dans provider_prefs de son premier
/// provider *activé* (providers[0] = rôle primaire déclaré — ex. kilo→kilo-free
/// même si nvidia-nim figure plus loin dans sa liste pour le dernier recours).
/// Agent sans provider activé → fin de file. `priority` en tiebreak.
fn agent_chain_rank(conn: &Connection, agent: &Value) -> i64 {
    let mut order: Vec<String> = Vec::new();
    if let Ok(mut stmt) = conn.prepare(
        "SELECT provider FROM provider_prefs WHERE enabled = 1 \
         ORDER BY CASE WHEN position IS NULL THEN 1 ELSE 0 END, position",
    ) {
        if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            order = rows.flatten().collect();
        }
    }
    let cfg = agent_cfg(agent);
    let Some(provs) = cfg["providers"].as_array() else {
        return i64::MAX;
    };
    provs
        .iter()
        .filter_map(|v| v.as_str())
        .find_map(|p| order.iter().position(|o| o == p))
        .map(|i| i as i64)
        .unwrap_or(i64::MAX)
}

fn pick_agent_by_skill(conn: &Connection, task_type: &str) -> Option<Value> {
    let mut stmt = conn
        .prepare(&format!("SELECT {} FROM agents WHERE active = 1", AGENT_COLS))
        .ok()?;
    let agents: Vec<Value> = stmt
        .query_map([], row_to_agent)
        .ok()?
        .filter_map(|r| r.ok())
        .collect();
    let has_skill = |a: &Value, s: &str| {
        agent_cfg(a)["skills"]
            .as_array()
            .map(|arr| arr.iter().any(|x| x.as_str() == Some(s)))
            .unwrap_or(false)
    };
    let rank = |a: &Value| {
        (
            agent_chain_rank(conn, a),
            agent_priority(a),
            a["id"].as_i64().unwrap_or(0),
        )
    };
    let mut by_rank = |a: &&Value, b: &&Value| rank(a).cmp(&rank(b));
    // 1. skill exact
    if let Some(a) = agents
        .iter()
        .filter(|a| has_skill(a, task_type))
        .min_by(&mut by_rank)
    {
        return Some(a.clone());
    }
    // 2. wildcard
    agents
        .iter()
        .filter(|a| has_skill(a, "*"))
        .min_by(&mut by_rank)
        .cloned()
}

pub fn tool_list_agents(_args: &Value) -> Result<Value, String> {
    with_db(|conn| {
        let mut stmt = conn
            .prepare(&format!("SELECT {} FROM agents", AGENT_COLS))
            .map_err(|e| e.to_string())?;
        let agents: Vec<Value> = stmt
            .query_map([], row_to_agent)
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        Ok(json!(agents))
    })
}

pub fn tool_register_agent(args: &Value) -> Result<Value, String> {
    let name = args["agent_name"].as_str().ok_or("agent_name requis")?;
    let atype = args["agent_type"].as_str().unwrap_or("cli");
    let config = args["config"].as_str().unwrap_or("{}");
    let caps = args["capabilities"].as_str();
    with_db(|conn| {
        if let Some(existing) = find_agent(conn, name) {
            conn.execute(
                "UPDATE agents SET agent_type=?, config=?, active=1 WHERE id=?",
                params![atype, config, existing["id"].as_i64().unwrap_or(0)],
            )
            .map_err(|e| e.to_string())?;
            if let Some(c) = caps {
                conn.execute(
                    "UPDATE agents SET capabilities=? WHERE id=?",
                    params![c, existing["id"].as_i64().unwrap_or(0)],
                )
                .map_err(|e| e.to_string())?;
            }
            return Ok(json!({"id": existing["id"], "agent_name": name, "updated": true}));
        }
        conn.execute(
            "INSERT INTO agents (agent_name, agent_type, config, active, capabilities) VALUES (?,?,?,1,?)",
            params![name, atype, config, caps],
        )
        .map_err(|e| e.to_string())?;
        Ok(json!({"id": conn.last_insert_rowid(), "agent_name": name, "created": true}))
    })
}

pub fn tool_update_agent(args: &Value) -> Result<Value, String> {
    let name = args["agent_name"].as_str().ok_or("agent_name requis")?;
    with_db(|conn| {
        let Some(agent) = find_agent(conn, name) else {
            return Err(format!("agent '{}' introuvable", name));
        };
        if let Some(c) = args["config"].as_str() {
            conn.execute(
                "UPDATE agents SET config=? WHERE id=?",
                params![c, agent["id"].as_i64().unwrap_or(0)],
            )
            .map_err(|e| e.to_string())?;
        }
        if let Some(t) = args["agent_type"].as_str() {
            conn.execute(
                "UPDATE agents SET agent_type=? WHERE id=?",
                params![t, agent["id"].as_i64().unwrap_or(0)],
            )
            .map_err(|e| e.to_string())?;
        }
        if let Some(c) = args["capabilities"].as_str() {
            conn.execute(
                "UPDATE agents SET capabilities=? WHERE id=?",
                params![c, agent["id"].as_i64().unwrap_or(0)],
            )
            .map_err(|e| e.to_string())?;
        }
        let active_val = args["active"]
            .as_i64()
            .or_else(|| args["active"].as_bool().map(|b| b as i64));
        if let Some(a) = active_val {
            conn.execute(
                "UPDATE agents SET active=? WHERE id=?",
                params![a, agent["id"].as_i64().unwrap_or(0)],
            )
            .map_err(|e| e.to_string())?;
        }
        Ok(json!({"updated": true, "agent_name": name}))
    })
}

// ===================== Tasks =====================

pub fn tool_create_task(args: &Value) -> Result<Value, String> {
    let title = args["title"].as_str().ok_or("title requis")?;
    let desc = args["description"].as_str().unwrap_or("");
    let priority = args["priority"].as_str().unwrap_or("medium");
    let deps = args["dependencies"]
        .as_array()
        .map(|a| serde_json::to_string(a).unwrap_or_else(|_| "[]".into()))
        .unwrap_or_else(|| "[]".into());
    let session = args["session"].as_str().unwrap_or("");
    let assigned: Option<i64> = args["assigned_to"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| args["assigned_to"].as_i64());
    with_db(|conn| {
        // assignation par nom possible
        let assigned = match (assigned, args["assigned_to"].as_str()) {
            (None, Some(name)) => find_agent(conn, name)
                .and_then(|a| a["id"].as_i64()),
            (id, _) => id,
        };
        let ws = args["workspace"].as_str().map(str::to_string).or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|p| p.to_string_lossy().to_string())
        });
        conn.execute(
            "INSERT INTO tasks_todo (title, description, priority, status, created_at, dependencies, assigned_to, dispatcher, expected_artifact, workspace) \
             VALUES (?,?,?,?,?,?,?,?,?,?)",
            params![title, desc, priority, "pending", now_iso(), deps, assigned, session,
                    args["expected_artifact"].as_str(), ws],
        )
        .map_err(|e| e.to_string())?;
        let id = conn.last_insert_rowid();
        log_event(conn, &id.to_string(), "created", &format!("session={}", session));
        Ok(json!({"id": id, "title": title, "status": "pending", "assigned_to": assigned}))
    })
}

/// FR-05 — gate de completion : si la tâche déclare un `expected_artifact`,
/// exiger un fichier existant dont le mtime est postérieur au dispatch
/// (created_at). Anti livrable-factice : pas de preuve, pas de 'completed'.
fn verify_expected_artifact(conn: &Connection, task_id: i64) -> Result<(), String> {
    let row: Option<(Option<String>, Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT expected_artifact, created_at, workspace FROM tasks_todo WHERE id=?",
            params![task_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .ok();
    let Some((ea, created, task_ws)) = row else { return Ok(()) };
    let Some(ea) = ea.filter(|s| !s.trim().is_empty()) else {
        return Ok(());
    };
    // expected_artifact = chemin de fichier ; sinon 1er token type chemin.
    let candidate = if Path::new(ea.trim()).exists() {
        ea.trim().to_string()
    } else {
        match ea.split_whitespace().find(|t| {
            let t = t.trim_matches(|c| c == '`' || c == '\'' || c == '"' || c == ';' || c == ',');
            (t.contains(":\\") || t.contains(":/") || t.contains('/'))
                && Path::new(t).exists()
        }) {
            Some(t) => t
                .trim_matches(|c| c == '`' || c == '\'' || c == '"' || c == ';' || c == ',')
                .to_string(),
            None => {
                return Err(format!(
                    "expected_artifact non vérifiable (aucun fichier existant): {}",
                    ea
                ))
            }
        }
    };
    // Audit F3+C1 : l'artefact doit etre un FICHIER REGULIER NON VIDE —
    // un dossier ou un placeholder vide ne vaut pas preuve.
    let meta = std::fs::metadata(&candidate)
        .map_err(|e| format!("expected_artifact illisible {}: {}", candidate, e))?;
    if !meta.is_file() {
        return Err(format!(
            "expected_artifact n'est pas un fichier regulier: {}",
            candidate
        ));
    }
    if meta.len() == 0 {
        return Err(format!("expected_artifact vide: {}", candidate));
    }
    // Audit F3 : confinement — si la tache porte un workspace, l'artefact
    // canonicalise doit rester sous cette racine (rejet `..` et absolu
    // pointant hors projet ; la canonicalisation resout aussi les junctions).
    if let Some(ws) = task_ws.filter(|s| !s.trim().is_empty()) {
        let canon = std::fs::canonicalize(&candidate)
            .map_err(|e| format!("expected_artifact incanonicalisable {}: {}", candidate, e))?;
        let ws_canon = std::fs::canonicalize(ws.trim())
            .map_err(|e| format!("workspace tache incanonicalisable {}: {}", ws, e))?;
        if !canon.starts_with(&ws_canon) {
            return Err(format!(
                "expected_artifact hors workspace ({} hors {}): path traversal refuse",
                canon.display(),
                ws_canon.display()
            ));
        }
    }
    let created_ms = created
        .as_deref()
        .and_then(|s| parse_sql_time(s).ok())
        .filter(|ms| *ms > 0);
    if let Some(created_ms) = created_ms {
        let mtime_ms = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        if mtime_ms <= created_ms {
            return Err(format!(
                "expected_artifact antérieur au dispatch (mtime <= created_at): {}",
                candidate
            ));
        }
    }
    Ok(())
}

/// FR-06 — auto verify_task : à la completion d'une tâche code-ish non
/// vérifiée, créer une tâche 'verify' vers un reviewer (capable fs_read).
/// Anti-boucle : la tâche verify elle-même ne re-déclenche jamais.
fn maybe_spawn_verify(conn: &Connection, task_id: i64) -> Result<(), String> {
    let row: Option<(Option<String>, Option<String>, String, Option<i64>, Option<String>)> = conn
        .query_row(
            "SELECT task_type, verified_by, title, assigned_to, dispatcher FROM tasks_todo WHERE id=?",
            params![task_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .ok();
    let Some((ttype, verified_by, title, assignee, disp)) = row else {
        return Ok(());
    };
    if verified_by.is_some()
        || ttype.as_deref() == Some("verify")
        || disp.as_deref() == Some("verify_task")
    {
        return Ok(());
    }
    let needs_verify = matches!(
        ttype.as_deref(),
        Some("code") | Some("debug") | Some("refactor") | Some("test")
    );
    if !needs_verify {
        return Ok(());
    }
    // Reviewer : meilleur agent skill 'review' passant le gate fs_read ;
    // jamais soi-même ; sinon tâche non assignée (ramassable).
    let rid = pick_agent_by_skill(conn, "review")
        .filter(|a| check_capabilities(a, "review").is_ok())
        .and_then(|a| a["id"].as_i64())
        .filter(|id| Some(*id) != assignee);
    conn.execute(
        "INSERT INTO tasks_todo (title, description, priority, status, created_at, dependencies, assigned_to, task_type, dispatcher) \
         VALUES (?,?,?,?,?,?,?,?,?)",
        params![
            format!("Verify task #{}: {}", task_id, title),
            format!("Relecture croisée de la tâche {} — vérifier l'artifact déclaré et la conformité.", task_id),
            "medium",
            "pending",
            now_iso(),
            format!("[{}]", task_id),
            rid,
            "verify",
            "verify_task",
        ],
    )
    .map_err(|e| e.to_string())?;
    let vid = conn.last_insert_rowid();
    conn.execute(
        "UPDATE tasks_todo SET verified_by=? WHERE id=?",
        params![format!("verify_task#{}", vid), task_id],
    )
    .map_err(|e| e.to_string())?;
    log_event(conn, &task_id.to_string(), "verify_spawned", &format!("verify_task#{}", vid));
    Ok(())
}

/// FR-07 — circuit breaker : 3 échecs consécutifs d'un provider dans
/// model_health → provider_prefs.enabled=0 + disabled_at + event. La
/// réactivation (cooldown 15 min) est gérée à la lecture dans pick_model.
fn circuit_breaker_check(conn: &Connection, provider: &str) {
    if provider.is_empty() {
        return;
    }
    let last3: Vec<i64> = conn
        .prepare("SELECT available FROM model_health WHERE provider=? ORDER BY id DESC LIMIT 3")
        .ok()
        .and_then(|mut s| {
            s.query_map(params![provider], |r| r.get::<_, i64>(0))
                .ok()
                .map(|rows| rows.flatten().collect())
        })
        .unwrap_or_default();
    if last3.len() == 3 && last3.iter().all(|a| *a == 0) {
        let enabled: Option<i64> = conn
            .query_row(
                "SELECT enabled FROM provider_prefs WHERE provider=?",
                params![provider],
                |r| r.get(0),
            )
            .ok();
        if enabled == Some(1) {
            let _ = conn.execute(
                "UPDATE provider_prefs SET enabled=0, disabled_at=? WHERE provider=?",
                params![now_iso(), provider],
            );
            log_event(
                conn,
                &format!("provider:{}", provider),
                "circuit_breaker",
                "3 échecs consécutifs → provider disabled (cooldown 15min)",
            );
        }
    }
}

pub fn tool_update_task(args: &Value) -> Result<Value, String> {
    let task_id = arg_task_id(args)?;
    let id_num: i64 = task_id.parse().map_err(|_| "task_id numérique requis")?;
    with_db(|conn| {
        if let Some(s) = args["status"].as_str() {
            if s == "completed" {
                verify_expected_artifact(conn, id_num)?;
            }
            conn.execute(
                "UPDATE tasks_todo SET status=? WHERE id=?",
                params![s, id_num],
            )
            .map_err(|e| e.to_string())?;
            log_event(conn, &task_id, "status", s);
            if s == "completed" {
                maybe_spawn_verify(conn, id_num)?;
            }
        }
        if let Some(d) = args["description"].as_str() {
            conn.execute(
                "UPDATE tasks_todo SET description=? WHERE id=?",
                params![d, id_num],
            )
            .map_err(|e| e.to_string())?;
        }
        if let Some(r) = args["result"].as_str() {
            conn.execute(
                "UPDATE tasks_todo SET result=? WHERE id=?",
                params![r, id_num],
            )
            .map_err(|e| e.to_string())?;
        }
        if let Some(e) = args["expected_artifact"].as_str() {
            conn.execute(
                "UPDATE tasks_todo SET expected_artifact=? WHERE id=?",
                params![e, id_num],
            )
            .map_err(|e| e.to_string())?;
        }
        if let Some(deps) = args["dependencies"].as_array() {
            conn.execute(
                "UPDATE tasks_todo SET dependencies=? WHERE id=?",
                params![serde_json::to_string(deps).unwrap_or_default(), id_num],
            )
            .map_err(|e| e.to_string())?;
        }
        if let Some(a) = args["assigned_to"].as_str() {
            let aid = a.parse::<i64>().ok().or_else(|| {
                find_agent(conn, a).and_then(|ag| ag["id"].as_i64())
            });
            conn.execute(
                "UPDATE tasks_todo SET assigned_to=? WHERE id=?",
                params![aid, id_num],
            )
            .map_err(|e| e.to_string())?;
        }
        Ok(json!({"updated": true, "task_id": id_num}))
    })
}

pub fn tool_list_tasks(args: &Value) -> Result<Value, String> {
    with_db(|conn| {
        // Audit F1 : valeurs 100% liees via params! (plus d'interpolation).
        let status = args["status"].as_str();
        let assigned: Option<i64> = args["assigned_to"]
            .as_str()
            .and_then(|a| a.parse().ok())
            .or_else(|| args["assigned_to"].as_i64());
        let mut sql = format!("SELECT {} FROM tasks_todo WHERE 1=1", TASK_COLS);
        let mut bind: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(s) = status {
            sql += " AND status = ?";
            bind.push(Box::new(s.to_string()));
        }
        if let Some(a) = assigned {
            sql += " AND assigned_to = ?";
            bind.push(Box::new(a));
        }
        sql += " ORDER BY id DESC LIMIT 500";
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(bind), task_json)
            .map_err(|e| e.to_string())?;
        let out: Vec<Value> = rows.filter_map(|r| r.ok()).collect();
        Ok(json!(out))
    })
}

pub fn tool_get_next_task(args: &Value) -> Result<Value, String> {
    with_db(|conn| {
        let agent_id = args["agent_id"].as_str().unwrap_or("");
        let aid: Option<i64> = agent_id
            .parse()
            .ok()
            .or_else(|| find_agent(conn, agent_id).and_then(|a| a["id"].as_i64()));
        let sql = format!("SELECT {} FROM tasks_todo WHERE status='pending' ORDER BY id", TASK_COLS);
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let tasks = row_to_task(&mut stmt)?;
        'outer: for t in tasks {
            // assignation : tâche libre ou assignée à cet agent
            if let Some(a) = t["assigned_to"].as_i64() {
                if Some(a) != aid {
                    continue;
                }
            }
            // dépendances : toutes les tâches listées doivent être completed
            let deps: Vec<Value> = serde_json::from_str(
                t["dependencies"].as_str().unwrap_or("[]"),
            )
            .unwrap_or_default();
            for d in deps {
                let dep_id = d.as_i64().or_else(|| d.as_str().and_then(|s| s.parse().ok()));
                if let Some(di) = dep_id {
                    let st: String = conn
                        .query_row(
                            "SELECT status FROM tasks_todo WHERE id=?",
                            params![di],
                            |r| r.get(0),
                        )
                        .unwrap_or_else(|_| "pending".into());
                    if st != "completed" {
                        continue 'outer;
                    }
                }
            }
            return Ok(json!({"found": true, "task": t}));
        }
        Ok(json!({"found": false}))
    })
}

pub fn tool_delete_task(args: &Value) -> Result<Value, String> {
    let task_id = arg_task_id(args)?;
    with_db(|conn| {
        let n = conn
            .execute("DELETE FROM tasks_todo WHERE id=? OR CAST(id AS TEXT)=?",
                     params![task_id.parse::<i64>().unwrap_or(-1), task_id])
            .map_err(|e| e.to_string())?;
        Ok(json!({"deleted": n > 0, "task_id": task_id}))
    })
}

// ===================== Prompt templates =====================

fn default_templates() -> Vec<(&'static str, &'static str, &'static str, &'static str)> {
    vec![
        ("code", "Coder", "Implementation propre et minimale (single-line, exec-safe)",
         "Tu es l'agent codeur. TACHE : {{task}}. CONTEXTE : {{context}}. FICHIERS : {{files}}. MODELE : {{model}}.\nSECURITE ABSOLUE: 1. Injections: utilise exclusivement spawn/argv, JAMAIS exec/execSync/cmd. 2. Anti-Backdoor: Aucun code obfusque, dynamique (eval) ou caractere furtif. Le code doit etre 100% lisible.\nCLEAN GARDEN: Privilegie la modif aux creations. Resume dans update_task status=completed."),
        ("debug", "Debugger", "Diagnostiquer et corriger un bug",
         "Tu es l'agent de debogage. TACHE : {{task}}. SYMPTOMES : {{context}}. FICHIERS : {{files}}. MODELE : {{model}}.\nSECURITE ABSOLUE: 1. Injections: utilise exclusivement spawn/argv, JAMAIS exec/execSync/cmd. 2. Anti-Backdoor: Aucun code obfusque, dynamique (eval) ou caractere furtif.\nIdentifie la root cause, corrige proprement, update_task status=completed."),
        ("review", "Reviewer", "Revue qualite/securite (single-line, exec-safe)",
         "Tu es le reviewer qualite/securite. TACHE : {{task}}. CONTEXTE : {{context}}. FICHIERS : {{files}}. MODELE : {{model}}.\nSECURITE ABSOLUE: 1. Injections: utilise exclusivement spawn/argv, JAMAIS exec/execSync/cmd. 2. Anti-Backdoor: Aucun code obfusque, dynamique (eval) ou caractere furtif.\nFais un audit sans concession, update_task status=completed."),
        ("docs", "Documentation", "Redaction/mise a jour doc (single-line, exec-safe)",
         "Tu es le redacteur technique. TACHE : {{task}}. CONTEXTE : {{context}}. FICHIERS : {{files}}. MODELE : {{model}}.\nCLEAN GARDEN: Privilegie la modification aux creations. Rends la doc claire. update_task status=completed."),
        ("test", "Tester", "Ecriture ou execution de tests",
         "Tu es l'ingenieur QA. TACHE : {{task}}. CONTEXTE : {{context}}. FICHIERS : {{files}}. MODELE : {{model}}.\nSECURITE ABSOLUE: 1. Injections: utilise exclusivement spawn/argv, JAMAIS exec/execSync/cmd. 2. Anti-Backdoor: Aucun code obfusque, dynamique (eval) ou caractere furtif.\nEcris/execute les tests, update_task status=completed."),
        ("explore", "Explorer", "Exploration/analyse de codebase (lecture seule)",
         "Tu es l'agent explorateur. TACHE : {{task}}. CONTEXTE : {{context}}. FICHIERS : {{files}}. MODELE : {{model}}.\nFais un rapport d'analyse precis. Read-only (pas de modif) sauf consigne. update_task status=completed."),
    ]
}

fn seed_templates(conn: &Connection) -> Result<(), String> {
    let mut stmt = conn
        .prepare("INSERT OR IGNORE INTO prompt_templates (role, name, template, description) VALUES (?,?,?,?)")
        .map_err(|e| e.to_string())?;
    for (role, name, tpl, desc) in default_templates() {
        stmt.execute(params![role, name, tpl, desc])
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub fn tool_list_prompt_templates(_args: &Value) -> Result<Value, String> {
    with_db(|conn| {
        let mut stmt = conn
            .prepare("SELECT role, name, template, description, updated_at FROM prompt_templates")
            .map_err(|e| e.to_string())?;
        let rows: Vec<Value> = stmt
            .query_map([], |r| {
                Ok(json!({
                    "role": r.get::<_, String>(0)?,
                    "name": r.get::<_, Option<String>>(1)?,
                    "template": r.get::<_, Option<String>>(2)?,
                    "description": r.get::<_, Option<String>>(3)?,
                    "updated_at": r.get::<_, Option<String>>(4)?,
                }))
            })
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        Ok(json!(rows))
    })
}

pub fn tool_set_prompt_template(args: &Value) -> Result<Value, String> {
    let role = args["role"].as_str().ok_or("role requis")?;
    let name = args["name"].as_str().unwrap_or(role);
    let template = args["template"].as_str().ok_or("template requis")?;
    let desc = args["description"].as_str().unwrap_or("");
    with_db(|conn| {
        conn.execute(
            "INSERT INTO prompt_templates (role, name, template, description, updated_at) VALUES (?,?,?,?,?) \
             ON CONFLICT(role) DO UPDATE SET name=excluded.name, template=excluded.template, description=excluded.description, updated_at=excluded.updated_at",
            params![role, name, template, desc, now_iso()],
        )
        .map_err(|e| e.to_string())?;
        Ok(json!({"role": role, "saved": true}))
    })
}

fn render_template(conn: &Connection, role: &str, vars: &Map<String, Value>) -> (String, &'static str) {
    let custom: Option<String> = conn
        .query_row(
            "SELECT template FROM prompt_templates WHERE role=?",
            params![role],
            |r| r.get(0),
        )
        .ok();
    let tpl = custom
        .clone()
        .or_else(|| {
            default_templates()
                .into_iter()
                .find(|(r, _, _, _)| r == &role)
                .map(|(_, _, t, _)| t.to_string())
        })
        .unwrap_or_else(|| "TÂCHE : {{task}}\nCONTEXTE : {{context}}\nFICHIERS : {{files}}".into());
    let get = |k: &str| vars.get(k).and_then(|v| v.as_str()).unwrap_or("");
    let rendered = tpl
        .replace("{{task}}", &get("task").to_string())
        .replace("{{context}}", &if get("context").is_empty() { "(non précisé)".to_string() } else { get("context").to_string() })
        .replace("{{files}}", &if get("files").is_empty() { "(non précisés)".to_string() } else { get("files").to_string() })
        .replace("{{model}}", &get("model").to_string())
        .replace("{{provider}}", &get("provider").to_string());
    (rendered, if custom.is_some() { "custom" } else { "builtin" })
}

// ===================== pickModel (router unifié) =====================

fn recently_failed(model: &str, health: &Map<String, Value>) -> bool {
    let Some(h) = health.get(&model.to_lowercase()) else { return false };
    if h["ok"].as_i64().unwrap_or(1) == 1 {
        return false;
    }
    let Some(at) = h["at"].as_str() else { return false };
    // "YYYY-MM-DD HH:MM:SS" (UTC) -> epoch approx
    let Ok(at_ms) = parse_sql_time(at) else { return false };
    now_ms() - at_ms < 30 * 60 * 1000
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn parse_sql_time(s: &str) -> Result<u64, ()> {
    // "YYYY-MM-DD HH:MM:SS" ou ISO
    let s = s.replace('T', " ").replace('Z', "");
    let parts: Vec<&str> = s.split([' ', '-', ':']).collect();
    if parts.len() < 6 {
        return Err(());
    }
    let n: Vec<u64> = parts.iter().filter_map(|p| p.parse().ok()).collect();
    if n.len() < 6 {
        return Err(());
    }
    let days = days_from_civil(n[0] as i64, n[1] as u32, n[2] as u32);
    Ok((days as u64) * 86400_000 + n[3] * 3600_000 + n[4] * 60_000 + n[5] * 1000)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Le coeur du routage — port direct de pickModel() du serveur Node.
pub fn pick_model(
    conn: &Connection,
    agent_cfg: &Value,
    available: &[Value],
    force_provider: Option<&str>,
    force_model: Option<&str>,
) -> Option<Value> {
    if let Some(m) = force_model {
        return Some(json!({"provider": force_provider.unwrap_or("forced"), "model": m}));
    }
    // provider_prefs : ordre UI + désactivés
    let mut ui_order: Vec<String> = Vec::new();
    let mut disabled: Vec<String> = Vec::new();
    if let Ok(mut stmt) = conn.prepare(
        "SELECT provider, enabled, position, disabled_at FROM provider_prefs \
         ORDER BY CASE WHEN position IS NULL THEN 1 ELSE 0 END, position",
    ) {
        let prefs: Vec<(String, i64, Option<String>)> = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<String>>(3)?,
                ))
            })
            .map(|rows| rows.flatten().collect())
            .unwrap_or_default();
        drop(stmt);
        for (prov, en, dis_at) in prefs {
            if en == 1 {
                ui_order.push(prov);
                continue;
            }
            // FR-07 : cooldown 15 min écoulé → réactivation automatique
            let reactivate = dis_at
                .as_deref()
                .and_then(|s| parse_sql_time(s).ok())
                .map(|t| now_ms().saturating_sub(t) > 15 * 60 * 1000)
                .unwrap_or(false);
            if reactivate {
                let _ = conn.execute(
                    "UPDATE provider_prefs SET enabled=1, disabled_at=NULL WHERE provider=?",
                    params![prov],
                );
                log_event(
                    conn,
                    &format!("provider:{}", prov),
                    "circuit_breaker_recover",
                    "cooldown 15min écoulé → provider réactivé",
                );
                ui_order.push(prov);
            } else {
                disabled.push(prov);
            }
        }
    }
    let base: Vec<String> = match force_provider {
        Some(p) => vec![p.to_string()],
        None => agent_cfg["providers"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .filter(|v: &Vec<String>| !v.is_empty())
            .unwrap_or_else(|| {
                vec![
                    "nvidia-nim".into(),
                    "nvidia-nim2".into(),
                    "ollama-cloud".into(),
                    "openrouter-free".into(),
                ]
            }),
    };
    let mut pref_providers: Vec<String> = base
        .into_iter()
        .filter(|p| !disabled.contains(p))
        .collect();
    pref_providers.sort_by_key(|p| {
        ui_order
            .iter()
            .position(|u| u == p)
            .map(|i| i as i64)
            .unwrap_or(i64::MAX)
    });

    // santé live
    let mut health: Map<String, Value> = Map::new();
    if let Ok(mut stmt) = conn.prepare(
        "SELECT model, available, latency_ms, checked_at FROM model_health \
         WHERE id IN (SELECT MAX(id) FROM model_health GROUP BY provider, model)",
    ) {
        if let Ok(rows) = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        }) {
            for r in rows.flatten() {
                health.insert(
                    r.0.to_lowercase(),
                    json!({"ok": r.1, "lat": r.2.unwrap_or(999999), "at": r.3.unwrap_or_default()}),
                );
            }
        }
    }
    // in-flight
    let mut in_flight: Vec<String> = Vec::new();
    if let Ok(mut stmt) =
        conn.prepare("SELECT DISTINCT model FROM tasks_todo WHERE status='in_progress' AND model IS NOT NULL")
    {
        if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            in_flight = rows.flatten().map(|m| m.to_lowercase()).collect();
        }
    }
    let pref_models: Vec<String> = agent_cfg["model_prefs"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    for prov in &pref_providers {
        let candidates: Vec<Value> = available
            .iter()
            .filter(|m| m["provider"].as_str() == Some(prov.as_str()))
            .filter(|m| {
                // Les modeles embedding/reranker (vecteurs 2048D) ne sont pas
                // des modeles de chat : exclusion systematique du dispatch
                // generatif. Cause racine des echecs #556/#557.
                let name = m["model"].as_str().unwrap_or("").to_lowercase();
                !(name.contains("embed") || name.contains("rerank") || name.contains("retriever"))
            })
            .cloned()
            .collect();
        if candidates.is_empty() {
            continue;
        }
        let healthy: Vec<Value> = candidates
            .iter()
            .filter(|c| !recently_failed(c["model"].as_str().unwrap_or(""), &health))
            .cloned()
            .collect();
        let free: Vec<Value> = healthy
            .iter()
            .filter(|c| {
                !in_flight.contains(&c["model"].as_str().unwrap_or("").to_lowercase())
            })
            .cloned()
            .collect();
        let mut pool = if !free.is_empty() {
            free
        } else if !healthy.is_empty() {
            healthy
        } else {
            candidates
        };
        pool.sort_by_key(|c| {
            health
                .get(&c["model"].as_str().unwrap_or("").to_lowercase())
                .and_then(|h| h["lat"].as_i64())
                .unwrap_or(999999)
        });
        for preference in &pref_models {
            if let Some(hit) = pool.iter().find(|c| {
                c["model"].as_str().unwrap_or("").to_lowercase().contains(&preference.to_lowercase())
            }) {
                return Some(hit.clone());
            }
        }
        return pool.first().cloned();
    }
    None
}

#[cfg(test)]
mod routing_tests {
    use super::*;

    #[test]
    fn model_preferences_keep_their_declared_order() {
        let conn = Connection::open_in_memory().unwrap();
        let config = json!({"providers": ["nvidia-nim"], "model_prefs": ["nemotron-3-ultra-550b", "super-120b"]});
        let available = vec![
            json!({"provider": "nvidia-nim", "model": "nvidia/nemotron-3-super-120b-a12b"}),
            json!({"provider": "nvidia-nim", "model": "nvidia/nemotron-3-ultra-550b-a55b"}),
        ];
        let picked = pick_model(&conn, &config, &available, None, None).unwrap();
        assert_eq!(picked["model"], "nvidia/nemotron-3-ultra-550b-a55b");
    }
}

// ===================== Dispatch + spawn =====================

/// Tokenizer identique au JS (quotes + re-fusion `--opt=` valeur).
fn tokenize_args(s: &str) -> Vec<String> {
    let re = regex::Regex::new(r#""([^"]*)"|([^\s"]+)"#).unwrap();
    let mut out: Vec<String> = Vec::new();
    for cap in re.captures_iter(s) {
        out.push(
            cap.get(1)
                .or_else(|| cap.get(2))
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
        );
    }
    let eq_re = regex::Regex::new(r#"^--?[^\s=]+=$"#).unwrap();
    let mut merged: Vec<String> = Vec::new();
    for t in out {
        if let Some(last) = merged.last_mut() {
            if eq_re.is_match(last) {
                *last = format!("{}{}", last, t);
                continue;
            }
        }
        merged.push(t);
    }
    merged
}

struct SpawnPlan {
    file: String,
    argv: Vec<String>,
    cwd: PathBuf,
    stdout_file: Option<PathBuf>,
}

fn template_to_spawn_plan(tpl: &str, vars: &Map<String, Value>) -> Option<SpawnPlan> {
    let sub = |v: &str| -> String {
        let mut out = v.to_string();
        for (k, val) in vars.iter() {
            if let Some(sv) = val.as_str() {
                out = out.replace(k.as_str(), sv);
            }
        }
        out
    };
    let mut s = tpl.to_string();
    let mut cwd = PathBuf::from(
        vars.get("{workspace}")
            .and_then(|x| x.as_str())
            .unwrap_or("."),
    );
    // `cd /d "X" && ...`
    let cd_re = regex::Regex::new(r#"(?i)^\s*cd\s+/d\s+("[^"]*"|\S+)\s*&&\s*"#).unwrap();
    if let Some(cap) = cd_re.captures(&s.clone()) {
        let raw = cap.get(1).unwrap().as_str().trim_matches('"');
        cwd = PathBuf::from(sub(raw));
        s = s[cap.get(0).unwrap().end()..].to_string();
    }
    // Un cwd vide (`cd /d ""` si {workspace} absent) fait échouer
    // `current_dir` avec os error 123 — hériter du cwd parent plutôt.
    if cwd.as_os_str().is_empty() {
        cwd = PathBuf::from(".");
    }
    // `< NUL` en fin -> stdin ignore
    let nul_re = regex::Regex::new(r"(?i)<\s*NUL\s*$").unwrap();
    s = nul_re.replace(&s, "").to_string();
    // `> file 2>&1` en fin -> stdout redirect
    let out_re = regex::Regex::new(r#"(?i)>\s*("[^"]*"|\S+)\s*(?:2>&1)?\s*$"#).unwrap();
    let mut stdout_file = None;
    if let Some(cap) = out_re.captures(&s.clone()) {
        let raw = cap.get(1).unwrap().as_str().trim_matches('"');
        stdout_file = Some(PathBuf::from(sub(raw)));
        s = s[..cap.get(0).unwrap().start()].to_string();
    }
    let tokens: Vec<String> = tokenize_args(&s).into_iter().map(|t| sub(&t)).collect();
    if tokens.is_empty() {
        return None;
    }
    Some(SpawnPlan {
        file: tokens[0].clone(),
        argv: tokens[1..].to_vec(),
        cwd,
        stdout_file,
    })
}

/// Node >= 20.12 refuse spawn(.cmd/.bat) sans shell — on extrait la cible
/// réelle depuis la ligne `%*` du shim (vrai exe ou script node).
fn resolve_cmd_shim(file: &str) -> Option<(String, Vec<String>)> {
    if !file.to_lowercase().ends_with(".cmd") && !file.to_lowercase().ends_with(".bat") {
        return None;
    }
    let path = Path::new(file);
    let content = std::fs::read_to_string(path).ok()?;
    let line = content.lines().filter(|l| l.contains("%*")).last()?;
    let re = regex::Regex::new(r#""([^"]+)""#).unwrap();
    let quoted: Vec<String> = re
        .captures_iter(line)
        .map(|c| c[1].to_string())
        .collect();
    let last = quoted.last()?.replace("%~dp0", "").replace("%dp0%", "");
    let dir = path.parent()?;
    let target = dir.join(&last).canonicalize().unwrap_or_else(|_| dir.join(&last));
    let target_str = target.to_string_lossy().to_string();
    if target_str.to_lowercase().ends_with(".exe") {
        Some((target_str, vec![]))
    } else {
        // script node -> node.exe <script>
        let node = std::env::var("HPHE_NODE").unwrap_or_else(|_| "node".into());
        Some((node, vec![target_str]))
    }
}

fn run_spawn_plan(plan: &SpawnPlan) -> Result<u32, String> {
    // Le `2>&1` des templates est strippé du spawn — stderr doit être
    // fusionné dans le même fichier log, sinon toute erreur worker est muette.
    let (out, err): (Stdio, Stdio) = match &plan.stdout_file {
        Some(f) => {
            let path = if f.is_absolute() {
                f.clone()
            } else {
                plan.cwd.join(f)
            };
            if let Some(dir) = path.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|e| format!("log redirect impossible: {}", e))?;
            let err_file = file
                .try_clone()
                .map_err(|e| format!("stderr clone impossible: {}", e))?;
            (Stdio::from(file), Stdio::from(err_file))
        }
        None => (Stdio::null(), Stdio::null()),
    };
    let (file, argv) = match resolve_cmd_shim(&plan.file) {
        Some((f, prefix)) => {
            let mut a = prefix;
            a.extend(plan.argv.clone());
            (f, a)
        }
        None => (plan.file.clone(), plan.argv.clone()),
    };
    let mut cmd = Command::new(&file);
    cmd.args(&argv)
        .current_dir(&plan.cwd)
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(err);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NO_WINDOW | DETACHED_PROCESS
        cmd.creation_flags(0x08000000 | 0x00000008);
    }
    cmd.spawn()
        .map(|c| c.id())
        .map_err(|e| format!("spawn {}: {}", file, e))
}

fn cli_provider_mapping() -> Map<String, Value> {
    serde_json::from_value(json!({
        "nvidia-nim":      {"kilo": "nvidia"},
        "nvidia-nim2":     {"kilo": "nvidia2", "hermes": "custom"},
        "kilo-free":       {"kilo": "kilo"},
        "openrouter-free": {"kilo": "kilo", "hermes": "openrouter"},
        "hermes-free":     {"kilo": "kilo", "hermes": "nous"},
        "ollama":          {"kilo": "ollama-local"},
        "ollama-cloud":    {"kilo": "ollama", "hermes": "ollama-cloud"},
        "mistral-vibe":    {},
        "antigravity-subscription": {},
        "devin-subscription": {},
    }))
    .unwrap_or_default()
}

fn cli_family(agent_name: &str, cfg: &Value) -> String {
    // Transport explicite en priorité — le nom de l'agent reflète le provider
    // (ex: "nim2" = protocole NIM), pas le CLI qui le transporte.
    if let Some(t) = cfg["transport"].as_str() {
        let t = t.to_lowercase();
        if t == "kilo" || t == "hermes" {
            return t;
        }
    }
    let name = agent_name.to_lowercase();
    if name.contains("kilo") {
        "kilo".into()
    } else if name.contains("hermes") {
        "hermes".into()
    } else {
        String::new()
    }
}

fn cli_model_name(family: &str, provider: &str, model: &str) -> String {
    let map = cli_provider_mapping();
    if let Some(cli) = map.get(provider).and_then(|p| p.get(family)).and_then(|v| v.as_str()) {
        match family {
            // kilo exige un préfixe gateway — ne jamais le doubler si l'appelant
            // a déjà passé "nvidia2/xxx" ou "kilo/xxx".
            "kilo" => {
                if model.starts_with(&format!("{}/", cli)) {
                    model.to_string()
                } else {
                    format!("{}/{}", cli, model)
                }
            }
            _ => model.to_string(),
        }
    } else {
        model.to_string()
    }
}

#[allow(clippy::too_many_arguments)]
/// Workspace de secours : racine du kit = parent du `.agent/` contenant la db
/// orchestrateur (fallback si ni la tâche ni l'agent ni l'env n'en fournissent).
fn db_workspace_root(conn: &Connection) -> Option<String> {
    let path: String = conn
        .query_row(
            "SELECT file FROM pragma_database_list WHERE name='main'",
            [],
            |r| r.get(0),
        )
        .ok()?;
    let dir = Path::new(&path).parent()?;
    if dir.file_name().and_then(|n| n.to_str()) == Some(".agent") {
        dir.parent().map(|p| p.to_string_lossy().to_string())
    } else {
        Some(dir.to_string_lossy().to_string())
    }
}

fn notify_agent(
    conn: &Connection,
    task_id: i64,
    title: &str,
    assigned_to: Option<i64>,
    model: &str,
    provider: &str,
    workspace: Option<&str>,
    full_prompt: Option<&str>,
) -> Result<Value, String> {
    let Some(aid) = assigned_to else {
        return Ok(json!({"notified": false, "reason": "no_agent"}));
    };
    let Some(agent) = find_agent(conn, &aid.to_string()) else {
        return Ok(json!({"notified": false, "reason": "agent_unknown"}));
    };
    let agent_name = agent["agent_name"].as_str().unwrap_or("").to_string();
    let cfg = agent_cfg(&agent);
    let command_tpl = cfg["command"].as_str().unwrap_or("").to_string();
    if command_tpl.is_empty() {
        return Ok(json!({"notified": false, "reason": "no_command_template", "agent": agent_name}));
    }
    let safe_title = title.replace('"', "\\\"");
    let default_msg = format!(
        "Tu as une nouvelle tâche en attente dans l'orchestrateur. Tâche #{} : \"{}\". Utilise tes outils MCP pour la lire en détail et la traiter.",
        task_id, safe_title
    );
    // Workspace effectif : argument > config agent > env > racine du kit
    // (parent du `.agent/` hébergeant la db). Sans fallback, `{workspace}`
    // se rendait en "" → `cd /d ""` → os error 123 sous Windows.
    let ws_eff: Option<String> = workspace
        .filter(|w| !w.trim().is_empty())
        .map(String::from)
        .or_else(|| cfg["default_workspace"].as_str().map(String::from))
        .or_else(|| cfg["workspace"].as_str().map(String::from))
        .or_else(|| std::env::var("HPHE_DEFAULT_WORKSPACE").ok())
        .filter(|w| !w.trim().is_empty())
        .or_else(|| db_workspace_root(conn));
    let ws_fallback = workspace.map(|w| w.trim().is_empty()).unwrap_or(true)
        && ws_eff.is_some();
    if ws_fallback {
        log_event(
            conn,
            &task_id.to_string(),
            "workspace_fallback",
            &format!("agent={} ws={}", agent_name, ws_eff.as_deref().unwrap_or("")),
        );
    }
    let ws_rule = ws_eff
        .as_deref()
        .map(|w| {
            format!(
                " WORKSPACE={}. REGLE ABSOLUE : tout fichier à écrire (livrable, log, rapport) DOIT utiliser un chemin absolu sous {} — jamais de chemin relatif.",
                w, w
            )
        })
        .unwrap_or_default();
    let full = cfg["full_prompt"].as_bool().unwrap_or(false);
    let raw = format!("{}{}", if full { full_prompt.unwrap_or(&default_msg) } else { &default_msg }, ws_rule);
    let message = raw
        .replace('\n', " ")
        .replace('\r', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace('"', "'");
    let family = cli_family(&agent_name, &cfg);
    let cli_model = cli_model_name(&family, provider, model);
    let cli_provider = if family.is_empty() {
        provider.to_string()
    } else {
        cli_provider_mapping()
            .get(provider)
            .and_then(|p| p.get(family.as_str()))
            .and_then(|v| v.as_str())
            .unwrap_or(provider)
            .to_string()
    };

    let mut vars = Map::new();
    vars.insert("{model}".into(), json!(cli_model));
    vars.insert("{provider}".into(), json!(cli_provider));
    vars.insert("{provider_arg}".into(), json!(cli_provider));
    vars.insert("{workspace}".into(), json!(ws_eff.as_deref().unwrap_or("")));
    vars.insert("{message}".into(), json!(message));
    vars.insert("{task_id}".into(), json!(task_id.to_string()));
    vars.insert("{iso_now}".into(), json!(now_iso()));
    let Some(plan) = template_to_spawn_plan(&command_tpl, &vars) else {
        return Ok(json!({"notified": false, "reason": "empty_plan", "agent": agent_name}));
    };
    // Chemin du log stdout du worker — signal de vie exploitable par le
    // heartbeat tick du bridge (mtime frais = progression réelle, un pid
    // vivant mais gelé n'écrit rien).
    let log_path = plan.stdout_file.as_ref().map(|f| {
        let p = if f.is_absolute() { f.clone() } else { plan.cwd.join(f) };
        p.to_string_lossy().replace('\\', "/")
    });
    match run_spawn_plan(&plan) {
        Ok(pid) => {
            log_event(conn, &task_id.to_string(), "dispatched",
                      &format!("agent={} pid={} model={} log={}", agent_name, pid, cli_model, log_path.unwrap_or_default()));
            Ok(json!({"notified": true, "agent": agent_name, "pid": pid, "model": cli_model, "provider": cli_provider, "workspace": ws_eff, "workspace_fallback": ws_fallback}))
        }
        Err(e) => {
            log_event(conn, &task_id.to_string(), "dispatch_error", &e);
            Ok(json!({"notified": false, "reason": e, "agent": agent_name}))
        }
    }
}

// ===================== Worktrees =====================

fn create_worktree(conn: &Connection, base_repo: &str, task_id: i64) -> Result<Value, String> {
    let wt_path = format!("{}/.kilo/worktrees/task-{}", base_repo, task_id).replace('\\', "/");
    let branch = format!("agent/task-{}", task_id);
    let out = Command::new("git")
        .args(["-C", base_repo, "worktree", "add", &wt_path, "-b", &branch])
        .output()
        .map_err(|e| format!("git worktree add: {}", e))?;
    if !out.status.success() {
        return Err(format!(
            "git worktree add: {}",
            String::from_utf8_lossy(&out.stderr).chars().take(300).collect::<String>()
        ));
    }
    conn.execute(
        "INSERT INTO worktrees (task_id, path, branch, base, status, created_at) VALUES (?,?,?,?,'active',?)",
        params![task_id.to_string(), wt_path, branch, base_repo, now_iso()],
    )
    .map_err(|e| e.to_string())?;
    Ok(json!({"path": wt_path, "branch": branch}))
}

fn remove_worktree(conn: &Connection, row: &Value) -> Value {
    let base = row["base"].as_str().unwrap_or("");
    let path = row["path"].as_str().unwrap_or("");
    let branch = row["branch"].as_str().unwrap_or("");
    let out = Command::new("git")
        .args(["-C", base, "worktree", "remove", "--force", path])
        .output();
    match out {
        Err(e) => json!({"removed": false, "error": format!("worktree remove: {}", e)}),
        Ok(o) if !o.status.success() => json!({
            "removed": false,
            "error": format!("worktree remove: {}", String::from_utf8_lossy(&o.stderr).chars().take(300).collect::<String>()),
        }),
        Ok(_) => {
            let _ = Command::new("git")
                .args(["-C", base, "branch", "-D", branch])
                .output();
            let _ = conn.execute(
                "UPDATE worktrees SET status='removed' WHERE id=?",
                params![row["id"].as_i64().unwrap_or(0)],
            );
            json!({"removed": true, "branch_deleted": true})
        }
    }
}

pub fn tool_list_worktrees(args: &Value) -> Result<Value, String> {
    let status = args["status"].as_str().unwrap_or("active");
    with_db(|conn| {
        let sql = if status == "all" {
            "SELECT id, task_id, path, branch, base, status, created_at FROM worktrees ORDER BY id DESC".to_string()
        } else {
            format!(
                "SELECT id, task_id, path, branch, base, status, created_at FROM worktrees WHERE status='{}' ORDER BY id DESC",
                status.replace('\'', "''")
            )
        };
        let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
        let rows: Vec<Value> = stmt
            .query_map([], |r| {
                Ok(json!({
                    "id": r.get::<_, i64>(0)?,
                    "task_id": r.get::<_, String>(1)?,
                    "path": r.get::<_, String>(2)?,
                    "branch": r.get::<_, String>(3)?,
                    "base": r.get::<_, String>(4)?,
                    "status": r.get::<_, String>(5)?,
                    "created_at": r.get::<_, Option<String>>(6)?,
                }))
            })
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        Ok(json!(rows))
    })
}

pub fn tool_worktree_cleanup(args: &Value) -> Result<Value, String> {
    with_db(|conn| {
        let mut results = Vec::new();
        if args["all"].as_bool().unwrap_or(false) {
            let mut stmt = conn
                .prepare("SELECT id, task_id, path, branch, base, status FROM worktrees WHERE status='active'")
                .map_err(|e| e.to_string())?;
            let rows: Vec<Value> = stmt
                .query_map([], |r| {
                    Ok(json!({
                        "id": r.get::<_, i64>(0)?, "task_id": r.get::<_, String>(1)?,
                        "path": r.get::<_, String>(2)?, "branch": r.get::<_, String>(3)?,
                        "base": r.get::<_, String>(4)?,
                    }))
                })
                .map_err(|e| e.to_string())?
                .filter_map(|r| r.ok())
                .collect();
            for row in rows {
                results.push(json!({"task_id": row["task_id"], "result": remove_worktree(conn, &row)}));
            }
        } else {
            let tid = arg_task_id(args).map_err(|_| "task_id ou all=true requis".to_string())?;
            let row: Option<Value> = conn
                .query_row(
                    "SELECT id, task_id, path, branch, base FROM worktrees WHERE task_id=? AND status='active'",
                    params![tid],
                    |r| {
                        Ok(json!({
                            "id": r.get::<_, i64>(0)?, "task_id": r.get::<_, String>(1)?,
                            "path": r.get::<_, String>(2)?, "branch": r.get::<_, String>(3)?,
                            "base": r.get::<_, String>(4)?,
                        }))
                    },
                )
                .ok();
            match row {
                Some(r) => results.push(json!({"task_id": tid, "result": remove_worktree(conn, &r)})),
                None => return Ok(json!({"results": [], "note": "aucun worktree actif"})),
            }
        }
        Ok(json!({"results": results}))
    })
}

// ===================== dispatch_task =====================

pub async fn tool_dispatch_task(args: &Value) -> Result<Value, String> {
    let title = args["title"].as_str().ok_or("title requis")?;
    let task_type = args["task_type"].as_str().unwrap_or("code");
    let context = args["context"].as_str().unwrap_or("");
    let files = args["files"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(", "))
        .unwrap_or_default();
    let priority = args["priority"].as_str().unwrap_or("medium");
    let session = args["session"].as_str().unwrap_or("");
    let workspace = args["workspace"].as_str();
    let worktree = args["worktree"].as_bool().unwrap_or(false);
    let force_agent = args["agent_name"].as_str();
    let force_provider = args["provider"].as_str();
    let force_model = args["model"].as_str();

    let available = available_models(false).await;

    with_db(|conn| {
        // 1. Agent
        let agent = match force_agent {
            Some(name) => find_agent(conn, name),
            None => pick_agent_by_skill(conn, task_type),
        };
        let Some(agent) = agent else {
            return Err(format!("aucun agent actif pour task_type '{}'", task_type));
        };
        // FR-04 : le pick filtre deja active=1, mais un dispatch force
        // (agent_name explicite) doit refuser un agent euthanise/en quarantaine.
        if agent["active"].as_i64().unwrap_or(1) == 0 {
            return Err(format!(
                "dispatch refusé: agent '{}' inactif (euthanized/quarantaine)",
                agent["agent_name"].as_str().unwrap_or("")
            ));
        }
        let agent_name = agent["agent_name"].as_str().unwrap_or("").to_string();
        let acfg = agent_cfg(&agent);

        // 2. Modèle (router unifié)
        let picked = pick_model(conn, &acfg, &available, force_provider, force_model);
        let (model, provider) = picked
            .as_ref()
            .map(|p| {
                (
                    p["model"].as_str().unwrap_or("").to_string(),
                    p["provider"].as_str().unwrap_or("").to_string(),
                )
            })
            .unwrap_or_default();

        // 2b. Validation dispatch : model+provider obligatoires, provider
        // non désactivé dans provider_prefs, gate capabilities de l'agent.
        if model.is_empty() || provider.is_empty() {
            return Err(format!(
                "dispatch refusé: aucun modèle/provider disponible pour agent '{}' (task_type '{}')",
                agent_name, task_type
            ));
        }
        let enabled: Option<i64> = conn
            .query_row(
                "SELECT enabled FROM provider_prefs WHERE provider=?",
                params![provider],
                |r| r.get(0),
            )
            .ok();
        if enabled == Some(0) {
            return Err(format!(
                "dispatch refusé: provider '{}' désactivé dans provider_prefs",
                provider
            ));
        }
        if let Err(reason) = check_capabilities(&agent, task_type) {
            return Err(format!("dispatch refusé: {}", reason));
        }

        // 3. Tâche
        let mut vars = Map::new();
        vars.insert("task".into(), json!(title));
        vars.insert("context".into(), json!(context));
        vars.insert("files".into(), json!(files));
        vars.insert("model".into(), json!(model));
        vars.insert("provider".into(), json!(provider));
        let (prompt, tpl_used) = render_template(conn, task_type, &vars);

        conn.execute(
            "INSERT INTO tasks_todo (title, description, priority, status, created_at, dependencies, assigned_to, task_type, model, provider, dispatcher, expected_artifact, workspace) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params![
                title, context, priority, "pending", now_iso(), "[]",
                agent["id"].as_i64().unwrap_or(0), task_type, model, provider, session,
                args["expected_artifact"].as_str(), workspace
            ],
        )
        .map_err(|e| e.to_string())?;
        let task_id = conn.last_insert_rowid();

        // 4. Worktree isolé optionnel
        let mut ws = workspace.map(String::from);
        let mut wt_info = Value::Null;
        if worktree {
            let base = workspace.unwrap_or(".").to_string();
            match create_worktree(conn, &base, task_id) {
                Ok(w) => {
                    ws = Some(w["path"].as_str().unwrap_or(&base).to_string());
                    wt_info = w;
                }
                Err(e) => {
                    log_event(conn, &task_id.to_string(), "worktree_error", &e);
                    wt_info = json!({"error": e});
                }
            }
        }

        // 5. Notify (spawn CLI)
        let notify = notify_agent(
            conn, task_id, title, agent["id"].as_i64(), &model, &provider,
            ws.as_deref(), Some(&prompt),
        )?;

        // Le statut dépend du résultat réel du spawn — sinon une tâche dont le
        // worker n'a jamais démarré reste 'in_progress' éternellement (zombie
        // invisible au reconcile, qui joint sur l'event 'dispatched').
        let notified = notify["notified"].as_bool().unwrap_or(false);
        let reason = notify["reason"].as_str().unwrap_or("");
        let no_spawn = matches!(reason, "no_agent" | "agent_unknown" | "no_command_template" | "empty_plan");
        if notified {
            // FR-08 : beat initial au spawn — le reaper heartbeat part de là.
            conn.execute(
                "UPDATE tasks_todo SET status='in_progress', last_heartbeat=datetime('now') WHERE id=?",
                params![task_id],
            )
            .map_err(|e| e.to_string())?;
        } else if no_spawn {
            // Aucun spawn tenté (pas de commande configurée) : la tâche reste
            // en file, ramassable par un agent via get_next_task.
            conn.execute(
                "UPDATE tasks_todo SET status='pending' WHERE id=?",
                params![task_id],
            )
            .map_err(|e| e.to_string())?;
        } else {
            // Spawn tenté et raté → failed immédiat avec la raison réelle.
            conn.execute(
                "UPDATE tasks_todo SET status='failed', result=? WHERE id=?",
                params![format!("dispatch impossible: {}", reason), task_id],
            )
            .map_err(|e| e.to_string())?;
        }
        let ev = if notified { "assigned" } else if no_spawn { "queued" } else { "dispatch_failed" };
        log_event(conn, &task_id.to_string(), ev,
                  &format!("agent={} model={} provider={} template={} notified={} reason={}", agent_name, model, provider, tpl_used, notified, reason));

        Ok(json!({
            "task_id": task_id,
            "agent": agent_name,
            "model": model,
            "provider": provider,
            "template_used": tpl_used,
            "workspace": ws,
            "worktree": wt_info,
            "notify": notify,
        }))
    })
}

/// Discipline : ajuste le score d'un agent (dans config JSON, défaut 100,
/// borné 0-200) et trace l'événement. Sanction = delta négatif, reward = positif.
pub fn tool_discipline_agent(args: &Value) -> Result<Value, String> {
    let name = args["agent_name"].as_str().ok_or("agent_name requis")?;
    let delta = args["points_delta"]
        .as_i64()
        .or_else(|| args["points_delta"].as_str().and_then(|s| s.parse().ok()))
        .ok_or("points_delta requis (entier)")?;
    let reason = args["reason"].as_str().unwrap_or("");
    with_db(|conn| {
        let Some(agent) = find_agent(conn, name) else {
            return Err(format!("agent '{}' introuvable", name));
        };
        let mut cfg = agent_cfg(&agent);
        // Audit C2 : la source de verite du score est la table agent_discipline
        // (portee de agent-discipline.mjs) ; agents.config.score = miroir.
        let reg: Option<(i64, String, i64, i64)> = conn
            .query_row(
                "SELECT score, status, penalties_count, rewards_count FROM agent_discipline WHERE agent_name=?",
                params![name],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .ok();
        let (reg_score, reg_status, mut penalties, mut rewards) =
            reg.unwrap_or((cfg["score"].as_i64().unwrap_or(100), "nominal".into(), 0, 0));
        let prev = reg_score;
        let next = (prev + delta).clamp(0, 200);
        if delta < 0 {
            penalties += 1;
        } else if delta > 0 {
            rewards += 1;
        }
        // Seuils FR-04 : <80 probation, <50 quarantine, <20 euthanized + actif=0 ;
        // remontee >=50 reactive l'agent (reactivation manuelle/sur merite).
        let aid = agent["id"].as_i64().unwrap_or(0);
        let was_active = agent["active"].as_i64().unwrap_or(1);
        let (status, active) = if next < 20 {
            ("euthanized", 0i64)
        } else if next < 50 {
            ("quarantine", 0i64)
        } else if next < 80 {
            ("probation", was_active)
        } else if next >= 50 && was_active == 0 {
            // Reintegration : score recupere au-dessus du seuil quarantaine.
            ("nominal", 1i64)
        } else {
            ("nominal", was_active)
        };
        let status = if next >= 80 && was_active == 1 {
            "nominal"
        } else {
            status
        };
        conn.execute(
            "INSERT INTO agent_discipline (agent_name, score, status, last_penalty, last_reason, penalties_count, rewards_count, updated_at) \
             VALUES (?,?,?,?,?,?,?,datetime('now')) \
             ON CONFLICT(agent_name) DO UPDATE SET score=excluded.score, status=excluded.status, \
             last_penalty=excluded.last_penalty, last_reason=excluded.last_reason, \
             penalties_count=excluded.penalties_count, rewards_count=excluded.rewards_count, updated_at=excluded.updated_at",
            params![name, next, status, delta.min(0), reason, penalties, rewards],
        )
        .map_err(|e| e.to_string())?;
        cfg["score"] = json!(next);
        cfg["discipline_status"] = json!(status);
        conn.execute(
            "UPDATE agents SET config=?, active=? WHERE id=?",
            params![
                serde_json::to_string(&cfg).unwrap_or_default(),
                active,
                aid
            ],
        )
        .map_err(|e| e.to_string())?;
        log_event(
            conn,
            &format!("agent:{}", name),
            "discipline",
            &format!(
                "score {} -> {} (delta {}) status {} -> {} active={} [{}]",
                prev, next, delta, reg_status, status, active, reason
            ),
        );
        Ok(json!({
            "agent": name, "score_before": prev, "score_after": next,
            "delta": delta, "reason": reason,
            "status": status, "active": active,
            "penalties_count": penalties, "rewards_count": rewards,
        }))
    })
}

/// Status d'un agent : fiche + config + score + capabilities + comptage tâches.
pub fn tool_agent_status(args: &Value) -> Result<Value, String> {
    let name = args["agent_name"].as_str().ok_or("agent_name requis")?;
    with_db(|conn| {
        let Some(agent) = find_agent(conn, name) else {
            return Err(format!("agent '{}' introuvable", name));
        };
        let aid = agent["id"].as_i64().unwrap_or(0);
        let mut counts = Map::new();
        let mut stmt = conn
            .prepare("SELECT status, COUNT(*) FROM tasks_todo WHERE assigned_to=? GROUP BY status")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(params![aid], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(|e| e.to_string())?;
        for r in rows.flatten() {
            counts.insert(r.0, json!(r.1));
        }
        let cfg = agent_cfg(&agent);
        let reg: Option<(i64, String, i64, i64, Option<String>)> = conn
            .query_row(
                "SELECT score, status, penalties_count, rewards_count, updated_at FROM agent_discipline WHERE agent_name=?",
                params![name],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .ok();
        let (score, disc_status, penalties, rewards, updated) = reg.unwrap_or((
            cfg["score"].as_i64().unwrap_or(100),
            "nominal".into(),
            0,
            0,
            None,
        ));
        Ok(json!({
            "agent_name": agent["agent_name"],
            "id": aid,
            "active": agent["active"],
            "capabilities": agent["capabilities"],
            "score": score,
            "discipline_status": disc_status,
            "penalties_count": penalties,
            "rewards_count": rewards,
            "discipline_updated_at": updated,
            "config": cfg,
            "task_counts": counts,
        }))
    })
}

pub fn tool_suggest_agent(args: &Value) -> Result<Value, String> {
    let task_type = args["task_type"].as_str().ok_or("task_type requis")?;
    with_db(|conn| {
        match pick_agent_by_skill(conn, task_type) {
            Some(a) => Ok(json!({
                "found": true,
                "agent_name": a["agent_name"],
                "id": a["id"],
                "skills": agent_cfg(&a)["skills"],
            })),
            None => Ok(json!({"found": false, "task_type": task_type})),
        }
    })
}

/// Enregistre un probe de santé modèle (remplace la tâche model-discovery).
pub fn record_model_health(
    conn: &Connection,
    supplier: &str,
    provider: &str,
    model: &str,
    available: bool,
    latency_ms: i64,
) {
    let _ = conn.execute(
        "INSERT INTO model_health (supplier, provider, model, available, latency_ms, checked_at) VALUES (?,?,?,?,?,?)",
        params![supplier, provider, model, available as i64, latency_ms, now_iso().replace('T', " ").trim_end_matches('Z').to_string()],
    );
    circuit_breaker_check(conn, provider);
}

// ===================== Reconcile + ghost reaper =====================
//
// Le dispatch spawn le CLI agent et note `pid=N` dans task_events('dispatched'),
// mais rien ne réconcilie le statut si le process meurt sans update_task.
// Boucle hphe-rest (~60s) : pid mort → status='failed' + event 'reaped'
// → le bridge ws diffuse le changement au monitor et au backend.

/// PID Windows vivant ? tasklist CSV (pas de crate windows-sys dans ce module).
/// En cas de doute (erreur) → considéré vivant : jamais de faux `failed`.
fn pid_alive(pid: u32) -> bool {
    let mut cmd = Command::new("tasklist");
    cmd.args(["/FI", &format!("PID eq {}", pid), "/FO", "CSV", "/NH"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    match cmd.output() {
        Ok(o) => {
            let s = String::from_utf8_lossy(&o.stdout);
            // "node.exe","1234","Console","1","50,000 K" — champ 2 = pid
            s.lines().any(|l| {
                let f: Vec<&str> = l.split("\",\"").collect();
                f.get(1)
                    .map(|p| p.trim_matches('"') == pid.to_string())
                    .unwrap_or(false)
            })
        }
        Err(_) => true,
    }
}

/// Réconcilie les tâches `in_progress`/`assigned` dont le process dispatché
/// est mort depuis plus de `grace_secs`. Retourne les ids marquées `failed`.
pub fn reconcile_dead_tasks(grace_secs: u64) -> Vec<String> {
    let mut reaped = Vec::new();
    let _ = with_db(|conn| {
        // Dernier event 'dispatched' par tâche active (pid dans detail),
        // datant de plus de grace_secs (ts UTC 'YYYY-MM-DD HH:MM:SS').
        let cutoff = format!("-{} seconds", grace_secs);
        let mut stmt = conn
            .prepare(
                "SELECT t.id, e.detail FROM tasks_todo t
                 JOIN task_events e ON e.id = (
                     SELECT MAX(id) FROM task_events
                     WHERE task_id = CAST(t.id AS TEXT) AND event = 'dispatched')
                 WHERE t.status IN ('in_progress','assigned')
                   AND e.ts <= datetime('now', ?1)",
            )
            .map_err(|e| e.to_string())?;
        let rows: Vec<(i64, String)> = stmt
            .query_map(params![cutoff], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);
        let re_pid = regex::Regex::new(r"pid=(\d+)").map_err(|e| e.to_string())?;
        for (tid, detail) in rows {
            let Some(cap) = re_pid.captures(&detail) else { continue };
            let pid: u32 = cap[1].parse().unwrap_or(0);
            if pid == 0 || pid_alive(pid) {
                continue;
            }
            conn.execute(
                "UPDATE tasks_todo SET status='failed', result=? WHERE id=?",
                params![
                    format!("process pid={} terminé sans résultat (reap)", pid),
                    tid
                ],
            )
            .map_err(|e| e.to_string())?;
            log_event(
                conn,
                &tid.to_string(),
                "reaped",
                &format!("pid={} mort — statut reconcilié -> failed", pid),
            );
            reaped.push(format!("task#{}:pid{}", tid, pid));
        }

        // Deuxième passe : tâches dont le dernier event est 'dispatch_error'
        // (spawn tenté, jamais parti — aucun pid à tester). Sans ça elles
        // restent in_progress éternellement : le JOIN ci-dessus exige un
        // event 'dispatched' que ces tâches n'ont pas.
        let mut stmt2 = conn
            .prepare(
                "SELECT t.id FROM tasks_todo t
                 JOIN task_events e ON e.id = (
                     SELECT MAX(id) FROM task_events
                     WHERE task_id = CAST(t.id AS TEXT))
                 WHERE t.status IN ('in_progress','assigned')
                   AND e.event = 'dispatch_error'
                   AND e.ts <= datetime('now', ?1)",
            )
            .map_err(|e| e.to_string())?;
        let err_rows: Vec<i64> = stmt2
            .query_map(params![cutoff], |r| r.get(0))
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt2);
        for tid in err_rows {
            conn.execute(
                "UPDATE tasks_todo SET status='failed', result=? WHERE id=?",
                params!["spawn raté (dispatch_error) — reconcilié", tid],
            )
            .map_err(|e| e.to_string())?;
            log_event(conn, &tid.to_string(), "reaped", "dispatch_error — jamais spawné");
            reaped.push(format!("task#{}:dispatch_error", tid));
        }

        // Troisième passe FR-08 : heartbeat expiré (>10 min sans tick).
        // Couvre les workers gelés-mais-vivants (pid OK, zéro progression) :
        // last_heartbeat ne monte que si le wrapper/bridge voit du signal.
        let mut stmt3 = conn
            .prepare(
                "SELECT id FROM tasks_todo
                 WHERE status IN ('in_progress','assigned')
                   AND last_heartbeat IS NOT NULL AND last_heartbeat != ''
                   AND last_heartbeat <= datetime('now', '-600 seconds')",
            )
            .map_err(|e| e.to_string())?;
        let hb_rows: Vec<i64> = stmt3
            .query_map([], |r| r.get(0))
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt3);
        for tid in hb_rows {
            conn.execute(
                "UPDATE tasks_todo SET status='failed', result=? WHERE id=?",
                params!["heartbeat expiré (>10min sans tick) — worker gelé ou mort (reap)", tid],
            )
            .map_err(|e| e.to_string())?;
            log_event(conn, &tid.to_string(), "reaped", "heartbeat_timeout >600s — reconcilié");
            reaped.push(format!("task#{}:heartbeat_timeout", tid));
        }
        Ok(())
    });
    reaped
}

/// Outil MCP : réconciliation manuelle (appelée aussi par la boucle rest).
pub fn tool_reconcile_tasks(args: &Value) -> Result<Value, String> {
    let grace = args["grace_secs"].as_u64().unwrap_or(120);
    let reaped = reconcile_dead_tasks(grace);
    Ok(json!({"reaped": reaped, "count": reaped.len()}))
}

/// FR-08 : tick heartbeat — le wrapper/bridge frappe toutes les ~30s tant que
/// le worker spawné donne signe de vie. Format datetime('now') pour rester
/// comparable aux cutoffs du reconcile.
pub fn tool_task_heartbeat(args: &Value) -> Result<Value, String> {
    let task_id = arg_task_id(args)?;
    let id_num: i64 = task_id.parse().map_err(|_| "task_id numérique requis")?;
    with_db(|conn| {
        let n = conn
            .execute(
                "UPDATE tasks_todo SET last_heartbeat=datetime('now') WHERE id=? AND status IN ('in_progress','assigned')",
                params![id_num],
            )
            .map_err(|e| e.to_string())?;
        Ok(json!({"task_id": id_num, "heartbeat": n > 0}))
    })
}

// ===================== Paires binôme durables (FR-09) =====================

fn row_to_pair(r: &rusqlite::Row) -> rusqlite::Result<Value> {
    Ok(json!({
        "pair_id": r.get::<_, String>(0)?,
        "agent_a": r.get::<_, String>(1)?,
        "agent_b": r.get::<_, String>(2)?,
        "session": r.get::<_, Option<String>>(3)?,
        "transport_a": r.get::<_, Option<String>>(4)?,
        "transport_b": r.get::<_, Option<String>>(5)?,
        "cascade_a": r.get::<_, Option<String>>(6)?,
        "cascade_b": r.get::<_, Option<String>>(7)?,
        "model": r.get::<_, Option<String>>(8)?,
        "established_at": r.get::<_, Option<String>>(9)?,
        "last_seen": r.get::<_, Option<String>>(10)?,
        "meta": r.get::<_, Option<String>>(11)?,
    }))
}

/// FR-09 : persistance d'une paire établie. established_at n'est écrit qu'à
/// la création ; les re-saves rafraîchissent last_seen sans le casser.
pub fn tool_pair_save(args: &Value) -> Result<Value, String> {
    let pair_id = args["pair_id"].as_str().ok_or("pair_id requis")?;
    let agent_a = args["agent_a"].as_str().ok_or("agent_a requis")?;
    let agent_b = args["agent_b"].as_str().ok_or("agent_b requis")?;
    let meta = args["meta"].as_str().map(|m| m.chars().take(4000).collect::<String>());
    with_db(|conn| {
        conn.execute(
            "INSERT INTO pairs (pair_id, agent_a, agent_b, session, transport_a, transport_b, cascade_a, cascade_b, model, established_at, last_seen, meta) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?) \
             ON CONFLICT(pair_id) DO UPDATE SET \
               agent_a=excluded.agent_a, agent_b=excluded.agent_b, session=excluded.session, \
               transport_a=excluded.transport_a, transport_b=excluded.transport_b, \
               cascade_a=excluded.cascade_a, cascade_b=excluded.cascade_b, \
               model=excluded.model, last_seen=excluded.last_seen, meta=excluded.meta",
            params![
                pair_id, agent_a, agent_b,
                args["session"].as_str(), args["transport_a"].as_str(), args["transport_b"].as_str(),
                args["cascade_a"].as_str(), args["cascade_b"].as_str(), args["model"].as_str(),
                args["established_at"].as_str().map(|s| s.to_string()).unwrap_or_else(now_iso),
                now_iso(), meta,
            ],
        )
        .map_err(|e| e.to_string())?;
        Ok(json!({"saved": true, "pair_id": pair_id}))
    })
}

/// FR-09 : rappel d'une paire persistée — par pair_id, ou dernière paire vue
/// pour (agent, peer[, session]). Permet le rejoin sans handshake.
pub fn tool_pair_get(args: &Value) -> Result<Value, String> {
    const SEL: &str = "SELECT pair_id, agent_a, agent_b, session, transport_a, transport_b, cascade_a, cascade_b, model, established_at, last_seen, meta FROM pairs";
    with_db(|conn| {
        if let Some(pid) = args["pair_id"].as_str() {
            return conn
                .query_row(&format!("{} WHERE pair_id=?", SEL), params![pid], row_to_pair)
                .map_err(|_| format!("pair '{}' introuvable", pid));
        }
        let a = args["agent"].as_str().ok_or("agent ou pair_id requis")?;
        let b = args["peer"].as_str().unwrap_or("");
        let res = if let Some(s) = args["session"].as_str() {
            conn.query_row(
                &format!("{} WHERE ((agent_a=? AND agent_b=?) OR (agent_a=? AND agent_b=?)) AND session=? ORDER BY last_seen DESC LIMIT 1", SEL),
                params![a, b, b, a, s],
                row_to_pair,
            )
        } else {
            conn.query_row(
                &format!("{} WHERE (agent_a=? AND agent_b=?) OR (agent_a=? AND agent_b=?) ORDER BY last_seen DESC LIMIT 1", SEL),
                params![a, b, b, a],
                row_to_pair,
            )
        };
        res.map_err(|_| format!("aucune paire persistée pour {}/{}", a, b))
    })
}

// ---- Ghost reaper : process legacy/doublons créés par le kit ----

/// Signatures cmdline des process appartenant au kit LEGACY (v1) ou aux
/// artefacts obsolètes spawnés par les agents. Case-insensitive.
/// NE PAS ajouter : mcp-mux, *.devin*, context7, puppeteer, opencli,
/// mcp-quick-sqlite3, datacloud, hphe-* (live), vsix bridge.js (live :7477).
const GHOST_PATTERNS: &[&str] = &[
    "agentmemory/out/mcp-server",   // memory MCP node v1 (remplacé par hphe-mcp.exe)
    r"agentmemory\out\mcp-server",
    "qdrant-mcp-server",            // qdrant node v1 (vecteurs → hphe-vec)
    "zvec-mcp",
    "zvec_mcp",
    "servers/orchestrator-server/", // orchestrateur Node v1 (absorbé)
    r"servers\orchestrator-server\",
    "model-discovery",              // absorbé → tool available_models
    "nim-router",                   // absorbé → tools nim_*
    ".agent/scripts/monitor.js",    // monitor standalone v1 (→ extension :7477)
    r".agent\scripts\monitor.js",
    "everything-mcp/server.py",     // serveur Python v1
    r"everything-mcp\server.py",
    "everything-mcp/fs_tools.py",
    r"everything-mcp\fs_tools.py",
    "rag_rest_bridge.py",           // pont REST RAG python v1 (8001/8002)
];

/// Tue les process fantômes du kit (patterns cmdline legacy). Retourne
/// la liste `exe-pid` des victimes. Restrictif par conception.
pub fn reap_ghosts() -> Vec<String> {
    let mut killed = Vec::new();
    let mut cmd = Command::new("wmic");
    cmd.args([
        "process",
        "where",
        "name='node.exe' or name='cmd.exe' or name='python.exe' or name='pythonw.exe'",
        "get",
        "CommandLine,ProcessId",
        "/format:csv",
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW — le reaper ne poppe pas de console
    }
    let Ok(out) = cmd.output() else {
        return killed;
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    for line in stdout.lines() {
        let line = line.trim();
        // CSV wmic : Node,CommandLine,ProcessId
        let mut parts = line.rsplitn(2, ',');
        let Some(pid_s) = parts.next() else { continue };
        let Some(rest) = parts.next() else { continue };
        let Ok(pid) = pid_s.trim().parse::<u32>() else { continue };
        if pid == std::process::id() {
            continue;
        }
        // cmdline = tout après le 1er ',' (nom du noeud)
        let Some((_, cmdline)) = rest.split_once(',') else { continue };
        let cl = cmdline.to_lowercase();
        if GHOST_PATTERNS.iter().any(|p| cl.contains(&p.to_lowercase())) {
            let mut tk = Command::new("taskkill");
            tk.args(["/PID", &pid.to_string(), "/F"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                tk.creation_flags(0x08000000);
            }
            if tk.status().map(|s| s.success()).unwrap_or(false) {
                let exe = cl
                    .split_whitespace()
                    .next()
                    .unwrap_or("proc")
                    .rsplit(['/', '\\'])
                    .next()
                    .unwrap_or("proc")
                    .to_string();
                killed.push(format!("{}-{}", exe.trim_matches('"'), pid));
            }
        }
    }
    killed
}

/// Outil MCP : purge manuelle des process fantômes du kit.
pub fn tool_reap_ghosts(_args: &Value) -> Result<Value, String> {
    let killed = reap_ghosts();
    Ok(json!({"killed": killed, "count": killed.len()}))
}
