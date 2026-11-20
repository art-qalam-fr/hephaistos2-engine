//! hphe-kit tools — la couche unifiée Hephaistos-Kit.
//!
//! Absorbe : orchestrator-server, model-discovery, nim-router, agentMemory,
//! et la pipeline d'ingestion .agent/rag (Python) — en un seul MCP Rust.

use serde_json::{json, Value};

use crate::protocol::ToolDefinition;

fn schema(props: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": props,
        "required": required,
    })
}

pub fn kit_tool_definitions() -> Vec<ToolDefinition> {
    let ro = Some(json!({"readOnlyHint": true, "idempotentHint": true}));
    let wr = Some(json!({"destructiveHint": true}));
    vec![
        // ===== Orchestration (ex orchestrator-server) =====
        ToolDefinition {
            name: "create_task".into(),
            description: "Create a new task in the unified orchestrator. ID auto-generated.".into(),
            input_schema: schema(json!({
                "title": {"type": "string"},
                "description": {"type": "string"},
                "dependencies": {"type": "array", "items": {"type": "string"}},
                "assigned_to": {"type": "string", "description": "Agent ID or name"},
                "workspace": {"type": "string"},
                "priority": {"type": "string", "enum": ["low","medium","high"], "default": "medium"},
                "session": {"type": "string", "description": "Tag session de l'expéditeur"},
                "expected_artifact": {"type": "string", "description": "Livrable exigé pour la clôture (diff, build, log test)"},
            }), &["title"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "update_task".into(),
            description: "Update an existing task (status, description, result, deps, assignment).".into(),
            input_schema: schema(json!({
                "task_id": {"type": "string"},
                "status": {"type": "string", "enum": ["pending","in_progress","completed","failed"]},
                "description": {"type": "string"},
                "result": {"type": "string"},
                "dependencies": {"type": "array", "items": {"type": "string"}},
                "assigned_to": {"type": "string"},
                "expected_artifact": {"type": "string"},
            }), &["task_id"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "get_next_task".into(),
            description: "Get the next pending task, respecting dependencies and assignment.".into(),
            input_schema: schema(json!({
                "agent_id": {"type": "string", "description": "ID or name of requesting agent"},
            }), &[]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "list_tasks".into(),
            description: "List tasks with optional status/assignment filters.".into(),
            input_schema: schema(json!({
                "status": {"type": "string", "enum": ["pending","in_progress","completed","failed"]},
                "assigned_to": {"type": "string"},
            }), &[]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "delete_task".into(),
            description: "Delete a task by id.".into(),
            input_schema: schema(json!({"task_id": {"type": "string"}}), &["task_id"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "list_agents".into(),
            description: "List all registered agents.".into(),
            input_schema: schema(json!({}), &[]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "register_agent".into(),
            description: "Register or update an agent (name, type, config JSON).".into(),
            input_schema: schema(json!({
                "agent_name": {"type": "string"},
                "agent_type": {"type": "string"},
                "config": {"type": "string", "description": "JSON: {skills, providers, command, model_prefs, priority, full_prompt}"},
                "capabilities": {"type": "string", "description": "JSON array ex: [\"fs_read\",\"fs_write\",\"cmd\"] ou [\"text_only\"]"},
            }), &["agent_name"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "update_agent".into(),
            description: "Update an agent config/type/active flag.".into(),
            input_schema: schema(json!({
                "agent_name": {"type": "string"},
                "agent_type": {"type": "string"},
                "config": {"type": "string"},
                "capabilities": {"type": "string", "description": "JSON array ex: [\"fs_read\",\"fs_write\",\"cmd\"]"},
                "active": {"type": "integer"},
            }), &["agent_name"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "suggest_agent".into(),
            description: "Suggest an agent for a task_type based on registered skills.".into(),
            input_schema: schema(json!({
                "task_type": {"type": "string", "description": "code|debug|review|docs|test|explore"},
            }), &["task_type"]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "set_prompt_template".into(),
            description: "Create/update a prompt template for a role ({{task}} {{context}} {{files}} {{model}}).".into(),
            input_schema: schema(json!({
                "role": {"type": "string"},
                "name": {"type": "string"},
                "template": {"type": "string"},
                "description": {"type": "string"},
            }), &["role", "template"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "list_prompt_templates".into(),
            description: "List prompt templates by role.".into(),
            input_schema: schema(json!({}), &[]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "dispatch_task".into(),
            description: "Smart dispatch: pick agent by skill, pick live free model (unified router), render role prompt, create+assign task, notify agent CLI. Optional git worktree isolation.".into(),
            input_schema: schema(json!({
                "title": {"type": "string"},
                "task_type": {"type": "string", "description": "code|debug|review|docs|test|explore"},
                "context": {"type": "string"},
                "files": {"type": "array", "items": {"type": "string"}},
                "priority": {"type": "string", "enum": ["low","medium","high"]},
                "agent_name": {"type": "string"},
                "provider": {"type": "string"},
                "model": {"type": "string"},
                "workspace": {"type": "string"},
                "worktree": {"type": "boolean", "description": "Isolated git worktree .kilo/worktrees/task-<id>"},
                "session": {"type": "string"},
                "expected_artifact": {"type": "string", "description": "Livrable exigé pour la clôture"},
            }), &["title", "task_type"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "discipline_agent".into(),
            description: "Adjust an agent's discipline score (default 100, clamped 0-200). Sanction = negative points_delta, reward = positive. Logged in task_events.".into(),
            input_schema: schema(json!({
                "agent_name": {"type": "string"},
                "points_delta": {"type": "integer", "description": "Ex: -20 sanction, +10 reward"},
                "reason": {"type": "string"},
            }), &["agent_name", "points_delta"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "agent_status".into(),
            description: "Agent dossier: config, capabilities, score, task counts by status.".into(),
            input_schema: schema(json!({
                "agent_name": {"type": "string"},
            }), &["agent_name"]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "list_worktrees".into(),
            description: "List agent worktrees (task_id, path, branch, status).".into(),
            input_schema: schema(json!({
                "status": {"type": "string", "default": "active"},
            }), &[]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "worktree_cleanup".into(),
            description: "Remove an agent worktree (git worktree remove --force + branch -D).".into(),
            input_schema: schema(json!({
                "task_id": {"type": "string"},
                "all": {"type": "boolean"},
            }), &[]),
            annotations: wr.clone(),
        },
        // ===== Model discovery (ex model-discovery MCP) =====
        ToolDefinition {
            name: "available_models".into(),
            description: "Live check of free/local models (NVIDIA NIM x2, Ollama local+cloud, OpenRouter :free). Cached 60s. Absorbs model-discovery.".into(),
            input_schema: schema(json!({
                "refresh": {"type": "boolean"},
            }), &[]),
            annotations: ro.clone(),
        },
        // ===== NIM router (ex nim-router-mcp) =====
        ToolDefinition {
            name: "nim_domains".into(),
            description: "List routed NIM domains with primary model + fallbacks (embedded registry).".into(),
            input_schema: schema(json!({}), &[]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "nim_models".into(),
            description: "Live list of model_ids served by NVIDIA NIM (GET /models).".into(),
            input_schema: schema(json!({}), &[]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "nim_chat".into(),
            description: "Chat routed by domain (primary + automatic fallbacks on failure).".into(),
            input_schema: schema(json!({
                "domain": {"type": "string"},
                "prompt": {"type": "string"},
                "system": {"type": "string"},
                "temperature": {"type": "number", "default": 0.6},
                "max_tokens": {"type": "integer", "default": 2048},
            }), &["domain", "prompt"]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "nim_chat_model".into(),
            description: "Direct chat by explicit model_id (bypasses domain routing).".into(),
            input_schema: schema(json!({
                "model": {"type": "string"},
                "prompt": {"type": "string"},
                "system": {"type": "string"},
                "temperature": {"type": "number", "default": 0.6},
                "max_tokens": {"type": "integer", "default": 2048},
                "account": {"type": "integer", "default": 1},
            }), &["model", "prompt"]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "nim_embed".into(),
            description: "Embeddings via NIM (rag_embedding domain, 2048D — same vectors hphe-vec uses).".into(),
            input_schema: schema(json!({
                "text": {"type": "string"},
                "texts": {"type": "array", "items": {"type": "string"}},
                "domain": {"type": "string", "default": "rag_embedding"},
                "input_type": {"type": "string", "enum": ["query","passage"], "default": "query"},
            }), &[]),
            annotations: ro.clone(),
        },
        // ===== Memory (ex agentMemory MCP) =====
        ToolDefinition {
            name: "project_init".into(),
            description: "Initialize project memory storage and scaffold the workspace (idempotent): installs .agent/skills/reactive-orchestrator/watchdog.ps1, .agent/rules/rapport-dysfonctionnement.md and creates 'Rapport de dysfonctionnement et correction/'. The watchdog auto-starts via the orchestrator-monitor extension (no .vscode injection). Pass 'workspace' to target a root (default: HPHE env/cwd).".into(),
            input_schema: schema(json!({"projectId": {"type": "string"}, "workspace": {"type": "string"}}), &["projectId"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "memory_write".into(),
            description: "Store new memory in the memory bank (unified store — same files as legacy agentMemory).".into(),
            input_schema: schema(json!({
                "projectId": {"type": "string"},
                "key": {"type": "string"},
                "type": {"type": "string", "enum": ["architecture","pattern","feature","api","bug","decision"]},
                "content": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "relationships": {"type": "object"},
            }), &["projectId", "key", "type", "content"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "memory_read".into(),
            description: "Read memory by exact key.".into(),
            input_schema: schema(json!({
                "projectId": {"type": "string"},
                "key": {"type": "string"},
            }), &["projectId", "key"]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "memory_search".into(),
            description: "Search memories by keyword, tags, or type.".into(),
            input_schema: schema(json!({
                "projectId": {"type": "string"},
                "query": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "type": {"type": "string"},
                "limit": {"type": "integer", "default": 10},
            }), &["projectId"]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "memory_list".into(),
            description: "List all memories (optionally by type).".into(),
            input_schema: schema(json!({
                "projectId": {"type": "string"},
                "type": {"type": "string"},
            }), &["projectId"]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "memory_update".into(),
            description: "Update an existing memory.".into(),
            input_schema: schema(json!({
                "projectId": {"type": "string"},
                "key": {"type": "string"},
                "content": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "relationships": {"type": "object"},
            }), &["projectId", "key"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "memory_stats".into(),
            description: "Storage statistics for one project or all projects.".into(),
            input_schema: schema(json!({
                "projectId": {"type": "string"},
            }), &[]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "beacon_ingest".into(),
            description: "Incrementally ingest redacted, workspace-scoped Beacon session summaries into project memory. Defaults to dry-run.".into(),
            input_schema: schema(json!({
                "workspace": {"type": "string"},
                "projectId": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "dryRun": {"type": "boolean", "default": true},
                "logPath": {"type": "string", "description": "Optional path under the Beacon endpoint logs directory."},
            }), &["workspace", "projectId"]),
            annotations: wr.clone(),
        },
        // ===== Ingestion (ex .agent/rag Python pipeline) =====
        ToolDefinition {
            name: "content_ingest".into(),
            description: "Ingest a directory tree into the Tantivy content index (respects .ragignore + safety rules; incremental unless force).".into(),
            input_schema: schema(json!({
                "root": {"type": "string"},
                "force": {"type": "boolean", "default": false},
                "max_files": {"type": "integer", "default": 100000},
                "project_tag": {"type": "string"},
                "index_dir": {"type": "string"},
            }), &[]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "reconcile_tasks".into(),
            description: "Reconcile task status: in_progress/assigned tasks whose dispatched pid is dead get marked failed + 'reaped' event. Run periodically by hphe-rest; callable manually.".into(),
            input_schema: schema(json!({
                "grace_secs": {"type": "integer", "default": 120, "description": "Secondes de grâce après le dernier dispatch avant de tester le pid"},
            }), &[]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "task_heartbeat".into(),
            description: "FR-08 worker heartbeat tick: refresh tasks_todo.last_heartbeat for an in_progress task. Called ~30s by the spawn wrapper/bridge while the worker is alive; >10min stale gets reaped as failed.".into(),
            input_schema: schema(json!({
                "task_id": {"type": "string"},
            }), &["task_id"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "pair_save".into(),
            description: "FR-09 persist an established binome pair into orchestrator.db (pairs table). Survives IDE restart and the 15min comms handshake TTL — enables rejoin without handshake.".into(),
            input_schema: schema(json!({
                "pair_id": {"type": "string"},
                "agent_a": {"type": "string"},
                "agent_b": {"type": "string"},
                "session": {"type": "string"},
                "transport_a": {"type": "string"},
                "transport_b": {"type": "string"},
                "cascade_a": {"type": "string"},
                "cascade_b": {"type": "string"},
                "model": {"type": "string"},
                "meta": {"type": "string"},
            }), &["pair_id", "agent_a", "agent_b"]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "pair_get".into(),
            description: "FR-09 recall a persisted pair: by pair_id, or latest pair for (agent, peer[, session]). Used for rejoin without handshake.".into(),
            input_schema: schema(json!({
                "pair_id": {"type": "string"},
                "agent": {"type": "string"},
                "peer": {"type": "string"},
                "session": {"type": "string"},
            }), &[]),
            annotations: ro.clone(),
        },
        ToolDefinition {
            name: "reap_ghosts".into(),
            description: "Kill kit-owned ghost processes: legacy node MCPs (agentMemory/qdrant/zvec/orchestrator-server/model-discovery/nim-router), legacy monitor.js, v1 python servers. Pattern-allowlist only — never touches live kit or user processes.".into(),
            input_schema: schema(json!({}), &[]),
            annotations: wr.clone(),
        },
        ToolDefinition {
            name: "hephaistos_update".into(),
            description: "Hephaistos update force: full pipeline — Tantivy content ingest + optional NIM-embedded vectors into hphe-vec. Replaces .agent/scripts/ingest-workspace.ps1 + .agent/rag/cli.py.".into(),
            input_schema: schema(json!({
                "root": {"type": "string"},
                "force": {"type": "boolean", "default": false},
                "embed": {"type": "boolean", "default": false, "description": "Also embed chunks into usearch collection (requires NVIDIA_API_KEY)"},
                "collection": {"type": "string", "default": "workspace_index"},
                "max_files": {"type": "integer", "default": 100000},
                "project_tag": {"type": "string"},
            }), &[]),
            annotations: wr.clone(),
        },
    ]
}

pub async fn execute_kit_tool(name: &str, args: &Value) -> Result<Value, String> {
    use hphe_kit::{beacon, ingest, memory, models, nim, orch};
    match name {
        "create_task" => orch::tool_create_task(args),
        "update_task" => orch::tool_update_task(args),
        "get_next_task" => orch::tool_get_next_task(args),
        "list_tasks" => orch::tool_list_tasks(args),
        "delete_task" => orch::tool_delete_task(args),
        "list_agents" => orch::tool_list_agents(args),
        "register_agent" => orch::tool_register_agent(args),
        "update_agent" => orch::tool_update_agent(args),
        "suggest_agent" => orch::tool_suggest_agent(args),
        "set_prompt_template" => orch::tool_set_prompt_template(args),
        "list_prompt_templates" => orch::tool_list_prompt_templates(args),
        "dispatch_task" => orch::tool_dispatch_task(args).await,
        "discipline_agent" => orch::tool_discipline_agent(args),
        "agent_status" => orch::tool_agent_status(args),
        "list_worktrees" => orch::tool_list_worktrees(args),
        "worktree_cleanup" => orch::tool_worktree_cleanup(args),
        "reconcile_tasks" => orch::tool_reconcile_tasks(args),
        "task_heartbeat" => orch::tool_task_heartbeat(args),
        "pair_save" => orch::tool_pair_save(args),
        "pair_get" => orch::tool_pair_get(args),
        "reap_ghosts" => orch::tool_reap_ghosts(args),
        "available_models" | "models_available" => models::tool_models_available(args).await,
        "nim_domains" => nim::tool_nim_domains(),
        "nim_models" => nim::tool_nim_models().await,
        "nim_chat" => nim::tool_nim_chat(args).await,
        "nim_chat_model" => nim::tool_nim_chat_model(args).await,
        "nim_embed" => nim::tool_nim_embed(args).await,
        "project_init" => memory::tool_project_init(args),
        "memory_write" => memory::tool_memory_write(args),
        "memory_read" => memory::tool_memory_read(args),
        "memory_search" => memory::tool_memory_search(args),
        "memory_list" => memory::tool_memory_list(args),
        "memory_update" => memory::tool_memory_update(args),
        "memory_stats" => memory::tool_memory_stats(args),
        "beacon_ingest" => beacon::tool_beacon_ingest(args),
        "content_ingest" => ingest::tool_content_ingest(args),
        "hephaistos_update" | "hephaistos_update_force" => {
            ingest::tool_hephaistos_update(args).await
        }
        other => Err(format!("unknown tool {}", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::kit_tool_definitions;
    use serde_json::json;

    #[test]
    fn beacon_ingest_is_registered_with_safe_dry_run_default() {
        let tool = kit_tool_definitions()
            .into_iter()
            .find(|tool| tool.name == "beacon_ingest")
            .expect("beacon_ingest tool definition");

        assert_eq!(
            tool.input_schema["required"],
            json!(["workspace", "projectId"])
        );
        assert_eq!(tool.input_schema["properties"]["dryRun"]["default"], true);
        assert_eq!(tool.annotations.unwrap()["destructiveHint"], true);
    }
}
