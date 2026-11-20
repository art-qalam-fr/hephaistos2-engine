//! hphe-orch — pilotage complet de l'orchestrateur depuis un shell/cron/CI.
//!
//! Appelle `hphe-kit::orch` en bibliotheque (decision 4.1 : pas de spawn
//! stdio de hphe-mcp.exe). Toute ecriture passe par les memes tool_* que le
//! serveur MCP : les gates FR-01..FR-09 s'appliquent a l'identique.
//!
//! Exit codes : 0 succes, 1 validation/metier, 2 erreur systeme.

use clap::{Parser, Subcommand};
use hphe_kit::orch;
use serde_json::{json, Value};
use std::process::exit;

#[derive(Parser)]
#[command(name = "hphe-orch", version, about = "Hephaistos orchestrator CLI")]
struct Cli {
    /// Sortie JSON (defaut : JSON deja ; ce flag force pretty=false + raw)
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Liste les taches (filtres optionnels)
    List {
        /// Filtre de statut (pending|in_progress|completed|failed|cancelled)
        #[arg(long)]
        status: Option<String>,
        /// Filtre agent assigne (id numerique)
        #[arg(long)]
        agent: Option<String>,
    },
    /// Liste les agents enregistres
    Agents,
    /// Cree une tache (payload JSON identique a create_task)
    Create {
        /// Titre de la tache
        #[arg(long)]
        title: String,
        /// Description
        #[arg(long)]
        description: Option<String>,
        /// Artefact attendu (gate FR-05)
        #[arg(long, name = "expected-artifact")]
        expected_artifact: Option<String>,
        /// Type de tache (code|docs|explore|verify|...)
        #[arg(long, name = "task-type")]
        task_type: Option<String>,
        /// Dependances JSON (ex: "[501,502]")
        #[arg(long)]
        deps: Option<String>,
    },
    /// Dispatch une tache vers un provider/agent (gates FR-01/02/03 actifs)
    Dispatch {
        /// Payload JSON brut (prioritaire si fourni)
        #[arg(long)]
        payload: Option<String>,
        /// Task id a dispatcher
        #[arg(long)]
        id: Option<i64>,
        /// Provider cible
        #[arg(long)]
        provider: Option<String>,
        /// Modele cible
        #[arg(long)]
        model: Option<String>,
        /// Agent cible
        #[arg(long)]
        agent: Option<String>,
    },
    /// Met a jour une tache (status/result/expected_artifact/deps/assigned_to)
    Update {
        /// Task id
        #[arg(long)]
        id: i64,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        result: Option<String>,
        #[arg(long, name = "expected-artifact")]
        expected_artifact: Option<String>,
        #[arg(long)]
        deps: Option<String>,
        #[arg(long, name = "assigned-to")]
        assigned_to: Option<String>,
    },
    /// Tue une tache in_progress (status -> failed, event kill)
    Kill {
        /// Task id
        #[arg(long)]
        id: i64,
        /// Raison (journalisee)
        #[arg(long)]
        reason: Option<String>,
    },
    /// Re-verifie l'expected_artifact d'une tache sur disque
    Verify {
        /// Task id
        #[arg(long)]
        id: i64,
    },
    /// Tableau de bord : compteurs par statut + dernieres failed
    Status,
    /// Fiche agent (score, capabilities, compteurs de taches)
    AgentStatus {
        /// Nom de l'agent
        #[arg(long)]
        name: String,
    },
    /// Applique une sanction/bonus disciplinaire a un agent
    Discipline {
        /// Nom de l'agent
        #[arg(long)]
        agent: String,
        /// Delta de points (ex: -20)
        #[arg(long)]
        delta: i64,
        /// Raison
        #[arg(long)]
        reason: Option<String>,
    },
    /// Envoie un heartbeat pour une tache in_progress
    Heartbeat {
        /// Task id
        #[arg(long)]
        id: i64,
    },
    /// Reconcilie les taches zombie (worker mort/heartbeat stale)
    Reconcile,
    /// Persiste une paire binome
    PairSave {
        #[arg(long)]
        payload: String,
    },
    /// Rappelle une paire binome persistee
    PairGet {
        /// pair_id, ou recherche par agent+peer
        #[arg(long)]
        payload: String,
    },
}

fn parse_json(s: &str) -> Result<Value, i32> {
    serde_json::from_str(s).map_err(|e| {
        eprintln!("JSON invalide: {e}");
        1
    })
}

fn emit(v: Result<Value, String>, json_mode: bool) -> Result<(), i32> {
    match v {
        Ok(val) => {
            if json_mode {
                println!("{}", serde_json::to_string(&val).unwrap_or_default());
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&val).unwrap_or_default()
                );
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("{e}");
            Err(if e.contains("requis") || e.contains("refus") || e.contains("invalide") {
                1
            } else {
                2
            })
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();
    let code = match run(cli).await {
        Ok(()) => 0,
        Err(c) => c,
    };
    exit(code);
}

async fn run(cli: Cli) -> Result<(), i32> {
    let j = cli.json;
    match cli.command {
        Commands::List { status, agent } => {
            let mut args = json!({});
            if let Some(s) = status {
                args["status"] = json!(s);
            }
            if let Some(a) = agent {
                args["assigned_to"] = json!(a);
            }
            emit(orch::tool_list_tasks(&args), j)
        }
        Commands::Agents => emit(orch::tool_list_agents(&json!({})), j),
        Commands::Create {
            title,
            description,
            expected_artifact,
            task_type,
            deps,
        } => {
            let mut args = json!({ "title": title });
            if let Some(d) = description {
                args["description"] = json!(d);
            }
            if let Some(e) = expected_artifact {
                args["expected_artifact"] = json!(e);
            }
            if let Some(t) = task_type {
                args["task_type"] = json!(t);
            }
            if let Some(d) = deps {
                args["dependencies"] = parse_json(&d)?;
            }
            emit(orch::tool_create_task(&args), j)
        }
        Commands::Dispatch {
            payload,
            id,
            provider,
            model,
            agent,
        } => {
            let args = if let Some(p) = payload {
                parse_json(&p)?
            } else {
                let mut a = json!({});
                if let Some(i) = id {
                    a["task_id"] = json!(i.to_string());
                }
                if let Some(p) = provider {
                    a["provider"] = json!(p);
                }
                if let Some(m) = model {
                    a["model"] = json!(m);
                }
                if let Some(g) = agent {
                    a["agent"] = json!(g);
                }
                a
            };
            emit(orch::tool_dispatch_task(&args).await, j)
        }
        Commands::Update {
            id,
            status,
            result,
            expected_artifact,
            deps,
            assigned_to,
        } => {
            let mut args = json!({ "id": id });
            if let Some(s) = status {
                args["status"] = json!(s);
            }
            if let Some(r) = result {
                args["result"] = json!(r);
            }
            if let Some(e) = expected_artifact {
                args["expected_artifact"] = json!(e);
            }
            if let Some(d) = deps {
                args["dependencies"] = parse_json(&d)?;
            }
            if let Some(a) = assigned_to {
                args["assigned_to"] = json!(a);
            }
            emit(orch::tool_update_task(&args), j)
        }
        Commands::Kill { id, reason } => {
            let r = reason.unwrap_or_else(|| "killed via hphe-orch".into());
            emit(
                orch::tool_update_task(&json!({"id": id, "status": "failed", "result": r})),
                j,
            )
        }
        Commands::Verify { id } => {
            // Re-verification locale : lire la tache puis controler l'artefact.
            let tasks = orch::tool_list_tasks(&json!({})).map_err(|e| {
                eprintln!("{e}");
                2
            })?;
            let task = tasks
                .as_array()
                .and_then(|a| a.iter().find(|t| t["id"].as_i64() == Some(id)));
            let Some(task) = task else {
                eprintln!("tache {id} introuvable");
                return Err(1);
            };
            let artifact = task["expected_artifact"].as_str().unwrap_or("");
            if artifact.is_empty() {
                return emit(
                    Ok(json!({"task_id": id, "verified": true, "note": "pas d'expected_artifact"})),
                    j,
                );
            }
            let p = std::path::Path::new(artifact);
            let ok = p.is_file()
                && p.metadata()
                    .map(|m| m.len() > 0)
                    .unwrap_or(false);
            emit(
                Ok(json!({
                    "task_id": id,
                    "expected_artifact": artifact,
                    "exists": p.exists(),
                    "is_file": p.is_file(),
                    "size": p.metadata().map(|m| m.len()).unwrap_or(0),
                    "verified": ok,
                })),
                j,
            )
        }
        Commands::Status => {
            let tasks = orch::tool_list_tasks(&json!({})).map_err(|e| {
                eprintln!("{e}");
                2
            })?;
            let mut counts = serde_json::Map::new();
            let mut recent_failed: Vec<Value> = Vec::new();
            if let Some(arr) = tasks.as_array() {
                for t in arr {
                    let s = t["status"].as_str().unwrap_or("?").to_string();
                    *counts.entry(s.clone()).or_insert(json!(0)) =
                        json!(counts.get(&s).and_then(|v| v.as_i64()).unwrap_or(0) + 1);
                    if s == "failed" && recent_failed.len() < 10 {
                        recent_failed.push(json!({"id": t["id"], "title": t["title"]}));
                    }
                }
            }
            emit(
                Ok(json!({"counts": counts, "recent_failed": recent_failed})),
                j,
            )
        }
        Commands::AgentStatus { name } => {
            emit(orch::tool_agent_status(&json!({"agent_name": name})), j)
        }
        Commands::Discipline {
            agent,
            delta,
            reason,
        } => emit(
            orch::tool_discipline_agent(&json!({
                "agent_name": agent,
                "points_delta": delta,
                "reason": reason.unwrap_or_default(),
            })),
            j,
        ),
        Commands::Heartbeat { id } => {
            emit(orch::tool_task_heartbeat(&json!({"task_id": id.to_string()})), j)
        }
        Commands::Reconcile => emit(orch::tool_reconcile_tasks(&json!({})), j),
        Commands::PairSave { payload } => emit(orch::tool_pair_save(&parse_json(&payload)?), j),
        Commands::PairGet { payload } => emit(orch::tool_pair_get(&parse_json(&payload)?), j),
    }
}
