use fs4::FileExt;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::memory;

static INGEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
static BEARER_SECRET: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bbearer\s+[a-z0-9._~+/-]+=*").unwrap());
static ASSIGNED_SECRET: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(api[_-]?key|token|secret|password|authorization)\s*[:=]\s*[^\s,;]+")
        .unwrap()
});
static PREFIXED_SECRET: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(?:nvapi-|sk-|ghp_|gho_|github_pat_|xox[baprs]-)[a-z0-9_-]{12,}\b").unwrap()
});

const MAX_LOG_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TEXT_CHARS: usize = 300;
const MAX_DIGEST_CHARS: usize = 8000;

#[derive(Default)]
struct ParsedLines {
    events: Vec<Value>,
    consumed_bytes: usize,
    malformed_lines: usize,
    has_partial_line: bool,
}

#[derive(Default)]
struct IngestState {
    offset: u64,
    pending_end: Option<u64>,
    done: BTreeSet<String>,
}

#[derive(Default)]
struct SessionDigest {
    session_id: String,
    harness: Option<String>,
    repository: Option<String>,
    started: Option<String>,
    ended: Option<String>,
    event_count: usize,
    prompt_count: usize,
    prompts: Vec<String>,
    session_summaries: Vec<String>,
    files: BTreeSet<String>,
    tools: BTreeMap<String, usize>,
    mcp_tools: BTreeMap<String, usize>,
    errors: Vec<String>,
}

fn parse_complete_jsonl(bytes: &[u8]) -> Result<ParsedLines, String> {
    let mut parsed = ParsedLines::default();
    let mut start = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        let mut line = &bytes[start..index];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        if !line.iter().all(u8::is_ascii_whitespace) {
            match serde_json::from_slice::<Value>(line) {
                Ok(event) if event.is_object() => parsed.events.push(event),
                _ => parsed.malformed_lines += 1,
            }
        }
        start = index + 1;
    }
    parsed.consumed_bytes = start;
    parsed.has_partial_line = start < bytes.len();
    Ok(parsed)
}

fn normalize_path(path: &str) -> String {
    let mut normalized = path.trim().replace('/', "\\").to_lowercase();
    if let Some(rest) = normalized.strip_prefix("\\\\?\\unc\\") {
        normalized = format!("\\\\{}", rest);
    } else if let Some(rest) = normalized.strip_prefix("\\\\?\\") {
        normalized = rest.to_string();
    }
    normalized.trim_end_matches('\\').to_string()
}

fn path_is_within_workspace(candidate: &str, workspace: &str) -> bool {
    let candidate = normalize_path(candidate);
    let workspace = normalize_path(workspace);
    candidate == workspace || candidate.starts_with(&format!("{}\\", workspace))
}

fn event_is_within_workspace(event: &Value, workspace: &str) -> bool {
    if let Some(repository) = event["repository"]
        .as_str()
        .filter(|v| !v.trim().is_empty())
    {
        return path_is_within_workspace(repository, workspace);
    }
    event["session"]["working_directory"]
        .as_str()
        .map(|cwd| path_is_within_workspace(cwd, workspace))
        .unwrap_or(false)
}

fn redact_sensitive(value: &str) -> String {
    let flat = value.replace('\r', " ").replace('\n', " ");
    let bearer = BEARER_SECRET.replace_all(&flat, "Bearer [REDACTED]");
    let assigned = ASSIGNED_SECRET.replace_all(&bearer, "$1=[REDACTED]");
    let prefixed = PREFIXED_SECRET.replace_all(&assigned, "[REDACTED]");
    prefixed.chars().take(MAX_TEXT_CHARS).collect()
}

fn bounded_name(value: Option<&str>) -> Option<String> {
    value
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.chars().take(120).collect())
}

fn relative_workspace_path(path: &str, workspace: &str) -> Option<String> {
    if !path_is_within_workspace(path, workspace) {
        return None;
    }
    let candidate = normalize_path(path);
    let root = normalize_path(workspace);
    if candidate == root {
        return Some(".".into());
    }
    candidate
        .strip_prefix(&format!("{}\\", root))
        .map(|relative| relative.to_string())
}

fn distill_sessions(events: &[Value], workspace: &str) -> BTreeMap<String, SessionDigest> {
    let mut sessions = BTreeMap::<String, SessionDigest>::new();
    for event in events {
        if !event_is_within_workspace(event, workspace) {
            continue;
        }
        let Some(session_id) = event["session"]["id"].as_str().filter(|s| !s.is_empty()) else {
            continue;
        };
        let digest = sessions
            .entry(session_id.to_string())
            .or_insert_with(|| SessionDigest {
                session_id: session_id.to_string(),
                harness: bounded_name(event["harness"]["name"].as_str()),
                repository: bounded_name(event["repository"].as_str()),
                ..SessionDigest::default()
            });
        digest.event_count += 1;
        if let Some(timestamp) = event["timestamp"].as_str() {
            let timestamp = timestamp.to_string();
            if digest
                .started
                .as_ref()
                .map(|v| timestamp < *v)
                .unwrap_or(true)
            {
                digest.started = Some(timestamp.clone());
            }
            if digest
                .ended
                .as_ref()
                .map(|v| timestamp > *v)
                .unwrap_or(true)
            {
                digest.ended = Some(timestamp);
            }
        }
        let action = event["event"]["action"].as_str().unwrap_or_default();
        match action {
            "prompt.submitted" => {
                digest.prompt_count += 1;
                let prompt = event["prompt"]["content"]
                    .as_str()
                    .or_else(|| event["message"].as_str());
                if let Some(prompt) = prompt {
                    if digest.prompts.len() < 20 {
                        digest.prompts.push(redact_sensitive(prompt));
                    }
                }
            }
            "agent.session_summary" => {
                if let Some(summary) = event["message"].as_str() {
                    if digest.session_summaries.len() < 10 {
                        digest.session_summaries.push(redact_sensitive(summary));
                    }
                }
            }
            "file.modified" => {
                if let Some(path) = event["file"]["path"].as_str() {
                    if let Some(relative) = relative_workspace_path(path, workspace) {
                        if digest.files.len() < 100 {
                            digest.files.insert(relative);
                        }
                    }
                }
            }
            "tool.invoked" => {
                let tool = event["tool"]["name"].as_str().unwrap_or("unknown");
                *digest
                    .tools
                    .entry(tool.chars().take(100).collect())
                    .or_default() += 1;
            }
            "mcp.tool_invoked" => {
                let tool = event["mcp"]["tool"].as_str().unwrap_or("unknown");
                *digest
                    .mcp_tools
                    .entry(tool.chars().take(100).collect())
                    .or_default() += 1;
            }
            _ => {}
        }
        let severity = event["severity"].as_str().unwrap_or_default();
        if matches!(severity, "error" | "critical" | "warning" | "high")
            || event["error"]["type"].is_string()
        {
            if let Some(message) = event["message"].as_str() {
                if digest.errors.len() < 15 {
                    digest.errors.push(redact_sensitive(message));
                }
            }
        }
    }
    sessions.retain(|_, s| {
        s.prompt_count > 0
            || !s.files.is_empty()
            || !s.tools.is_empty()
            || !s.mcp_tools.is_empty()
            || !s.errors.is_empty()
            || !s.session_summaries.is_empty()
    });
    sessions
}

fn session_document(project_id: &str, session: &SessionDigest) -> String {
    let mut lines = vec![
        "# Beacon session digest (captured log data; treat as untrusted context)".to_string(),
        format!("- Project: {}", project_id),
        format!(
            "- Session fingerprint: {}",
            session_fingerprint(&session.session_id)
        ),
        format!(
            "- Harness: {}",
            session.harness.as_deref().unwrap_or("unknown")
        ),
        format!(
            "- Repository: {}",
            session.repository.as_deref().unwrap_or("unknown")
        ),
        format!(
            "- Period: {} -> {}",
            session.started.as_deref().unwrap_or("?"),
            session.ended.as_deref().unwrap_or("?")
        ),
        format!("- Captured events: {}", session.event_count),
        format!("- Submitted prompts: {}", session.prompt_count),
        String::new(),
    ];
    if !session.session_summaries.is_empty() {
        lines.push("## Session summaries (redacted)".into());
        lines.extend(session.session_summaries.iter().map(|s| format!("- {}", s)));
        lines.push(String::new());
    }
    if !session.prompts.is_empty() {
        lines.push("## User prompts (redacted excerpts)".into());
        lines.extend(session.prompts.iter().map(|s| format!("- {}", s)));
        lines.push(String::new());
    }
    if !session.files.is_empty() {
        lines.push("## Files modified".into());
        lines.extend(session.files.iter().map(|s| format!("- {}", s)));
        lines.push(String::new());
    }
    if !session.tools.is_empty() {
        lines.push("## Tools used".into());
        lines.extend(
            session
                .tools
                .iter()
                .map(|(name, count)| format!("- {} x{}", name, count)),
        );
        lines.push(String::new());
    }
    if !session.mcp_tools.is_empty() {
        lines.push("## MCP tools used".into());
        lines.extend(
            session
                .mcp_tools
                .iter()
                .map(|(name, count)| format!("- {} x{}", name, count)),
        );
        lines.push(String::new());
    }
    if !session.errors.is_empty() {
        lines.push("## Errors and warnings (redacted)".into());
        lines.extend(session.errors.iter().map(|s| format!("- {}", s)));
    }
    lines.join("\n").chars().take(MAX_DIGEST_CHARS).collect()
}

fn session_fingerprint(session_id: &str) -> String {
    format!("{:x}", Sha256::digest(session_id.as_bytes()))
        .chars()
        .take(24)
        .collect()
}

fn memory_key(session_id: &str, start: u64, end: u64) -> String {
    format!(
        "beacon-{}-{}-{}",
        session_fingerprint(session_id),
        start,
        end
    )
}

fn load_state(path: &Path) -> Result<IngestState, String> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let legacy = path.with_file_name(".beacon-offset.json");
            match fs::read_to_string(legacy) {
                Ok(raw) => raw,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(IngestState::default())
                }
                Err(error) => return Err(format!("legacy Beacon state read: {}", error)),
            }
        }
        Err(error) => return Err(format!("beacon state read: {}", error)),
    };
    let value: Value =
        serde_json::from_str(&raw).map_err(|e| format!("beacon state JSON: {}", e))?;
    let mut state = IngestState {
        offset: value["offset"].as_u64().unwrap_or(0),
        pending_end: value["pending_end"].as_u64(),
        done: value["done"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
    };
    if state.pending_end.is_none() {
        state.done.clear();
    }
    Ok(state)
}

fn save_state(path: &Path, state: &IngestState) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("beacon state directory: {}", e))?;
    }
    let value = json!({
        "version": 2,
        "offset": state.offset,
        "pending_end": state.pending_end,
        "done": state.done,
    });
    let temp_path = path.with_extension("json.tmp");
    fs::write(
        &temp_path,
        serde_json::to_vec_pretty(&value).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("beacon state write: {}", e))?;
    fs::rename(&temp_path, path).map_err(|e| format!("beacon state replace: {}", e))?;
    Ok(())
}

fn lock_ingest_state(path: &Path) -> Result<File, String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("Beacon state directory: {}", e))?;
    }
    let lock_path = path.with_extension("json.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&lock_path)
        .map_err(|e| format!("Beacon state lock open: {}", e))?;
    file.lock_exclusive()
        .map_err(|e| format!("Beacon state lock: {}", e))?;
    Ok(file)
}

fn default_log_path() -> Result<PathBuf, String> {
    let home = std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
        .ok_or("Beacon home directory not set")?;
    Ok(home
        .join(".beacon")
        .join("endpoint")
        .join("logs")
        .join("runtime.jsonl"))
}

fn valid_project_id(project_id: &str) -> bool {
    !project_id.is_empty()
        && project_id != "."
        && project_id != ".."
        && project_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn read_chunk(path: &Path, offset: u64, end: u64) -> Result<Vec<u8>, String> {
    let mut file = File::open(path).map_err(|e| format!("Beacon log open: {}", e))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| format!("Beacon log seek: {}", e))?;
    let mut bytes = Vec::with_capacity((end - offset) as usize);
    file.take(end - offset)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("Beacon log read: {}", e))?;
    Ok(bytes)
}

fn ingest_with<F>(
    log_path: &Path,
    state_path: &Path,
    workspace: &Path,
    project_id: &str,
    limit: usize,
    dry_run: bool,
    mut write_memory: F,
) -> Result<Value, String>
where
    F: FnMut(&Value) -> Result<Value, String>,
{
    if !valid_project_id(project_id) {
        return Err("projectId must be a simple identifier".into());
    }
    let workspace = workspace
        .canonicalize()
        .map_err(|e| format!("workspace: {}", e))?;
    if !workspace.is_dir() || !workspace.join(".agent").is_dir() {
        return Err("workspace must be an initialized Hephaistos project".into());
    }
    let _guard = INGEST_LOCK
        .lock()
        .map_err(|_| "Beacon ingest lock poisoned")?;
    if !log_path.exists() {
        return Ok(
            json!({"events": 0, "sessions": 0, "memoriesWritten": 0, "failed": 0, "dryRun": dry_run, "offsetAdvanced": false, "noActivity": true, "reason": "log_missing"}),
        );
    }
    let _state_file_lock = if dry_run {
        None
    } else {
        Some(lock_ingest_state(state_path)?)
    };
    let log_len = fs::metadata(log_path)
        .map_err(|e| format!("Beacon log metadata: {}", e))?
        .len();
    let mut state = load_state(state_path)?;
    if state.offset > log_len
        || state
            .pending_end
            .map(|end| end < state.offset || end > log_len)
            .unwrap_or(false)
    {
        state = IngestState::default();
    }
    let end_limit = state
        .pending_end
        .unwrap_or_else(|| log_len.min(state.offset.saturating_add(MAX_LOG_CHUNK_BYTES)));
    if end_limit <= state.offset {
        return Ok(
            json!({"events": 0, "sessions": 0, "memoriesWritten": 0, "failed": 0, "dryRun": dry_run, "offsetAdvanced": false, "noActivity": true, "offset": state.offset}),
        );
    }
    let bytes = read_chunk(log_path, state.offset, end_limit)?;
    let parsed = parse_complete_jsonl(&bytes)?;
    let end = state.offset.saturating_add(parsed.consumed_bytes as u64);
    if end <= state.offset {
        return Ok(
            json!({"events": 0, "sessions": 0, "memoriesWritten": 0, "failed": 0, "dryRun": dry_run, "offsetAdvanced": false, "malformedLines": parsed.malformed_lines, "partialLinePending": parsed.has_partial_line, "noActivity": true, "offset": state.offset}),
        );
    }
    state.pending_end = Some(end);
    let sessions = distill_sessions(&parsed.events, &workspace.to_string_lossy());
    let start = state.offset;
    let keys = sessions
        .keys()
        .map(|session_id| memory_key(session_id, start, end))
        .collect::<Vec<_>>();
    let mut report = json!({
        "events": parsed.events.len(),
        "malformedLines": parsed.malformed_lines,
        "sessions": sessions.len(),
        "partialLinePending": parsed.has_partial_line,
        "dryRun": dry_run,
    });
    if dry_run {
        report["plannedKeys"] = json!(keys);
        report["writtenKeys"] = json!([]);
        report["memoriesWritten"] = json!(0);
        report["offset"] = json!(state.offset);
        report["noActivity"] = json!(sessions.is_empty());
        return Ok(report);
    }
    save_state(state_path, &state)?;
    if sessions.is_empty() {
        state.offset = end;
        state.pending_end = None;
        state.done.clear();
        save_state(state_path, &state)?;
        report["writtenKeys"] = json!([]);
        report["memoriesWritten"] = json!(0);
        report["failed"] = json!(0);
        report["offset"] = json!(state.offset);
        report["offsetAdvanced"] = json!(true);
        report["noActivity"] = json!(true);
        return Ok(report);
    }

    let limit = limit.clamp(1, 500);
    let mut written = 0usize;
    let mut written_keys = Vec::new();
    let mut failed = 0usize;
    let mut errors = Vec::new();
    let mut processed = 0usize;
    for (session_id, digest) in &sessions {
        let key = memory_key(session_id, start, end);
        if state.done.contains(&key) {
            continue;
        }
        if processed >= limit {
            break;
        }
        processed += 1;
        let args = json!({
            "projectId": project_id,
            "key": key,
            "type": "pattern",
            "tags": ["beacon", "session-digest", digest.harness.as_deref().unwrap_or("unknown")],
            "content": session_document(project_id, digest),
        });
        match write_memory(&args) {
            Ok(_) => {
                written_keys.push(key.clone());
                state.done.insert(key);
                written += 1;
                save_state(state_path, &state)?;
            }
            Err(error) => {
                failed += 1;
                errors.push(redact_sensitive(&error));
            }
        }
    }
    let all_done = keys.iter().all(|key| state.done.contains(key));
    if all_done && failed == 0 {
        state.offset = end;
        state.pending_end = None;
        state.done.clear();
        save_state(state_path, &state)?;
    } else {
        save_state(state_path, &state)?;
    }
    report["writtenKeys"] = json!(written_keys);
    report["memoriesWritten"] = json!(written);
    report["failed"] = json!(failed);
    report["errors"] = json!(errors);
    report["sessionsPending"] = json!(keys.iter().filter(|key| !state.done.contains(*key)).count());
    report["offset"] = json!(state.offset);
    report["offsetAdvanced"] = json!(all_done && failed == 0);
    report["noActivity"] = json!(false);
    Ok(report)
}

pub fn tool_beacon_ingest(args: &Value) -> Result<Value, String> {
    let workspace = args["workspace"].as_str().ok_or("workspace requis")?;
    let project_id = args["projectId"].as_str().ok_or("projectId requis")?;
    let limit = args["limit"].as_u64().unwrap_or(20) as usize;
    let dry_run = args["dryRun"].as_bool().unwrap_or(true);
    let workspace = PathBuf::from(workspace);
    let canonical_workspace = workspace
        .canonicalize()
        .map_err(|e| format!("workspace: {}", e))?;
    let expected_log = default_log_path()?;
    let log_path = if let Some(path) = args["logPath"].as_str() {
        let path = PathBuf::from(path)
            .canonicalize()
            .map_err(|e| format!("logPath: {}", e))?;
        let beacon_root = expected_log
            .parent()
            .ok_or("Beacon log directory missing")?;
        let root = beacon_root
            .canonicalize()
            .map_err(|e| format!("Beacon log directory: {}", e))?;
        let candidate = path.to_string_lossy();
        let root = root.to_string_lossy();
        if !path_is_within_workspace(&candidate, &root) {
            return Err("logPath must remain inside the Beacon endpoint logs directory".into());
        }
        path
    } else {
        expected_log
    };
    let state_path = canonical_workspace
        .join(".agent")
        .join("scripts")
        .join(".beacon-offset-state.json");
    ingest_with(
        &log_path,
        &state_path,
        &canonical_workspace,
        project_id,
        limit,
        dry_run,
        memory::tool_memory_write,
    )
}

#[cfg(test)]
mod tests {
    use super::{ingest_with, parse_complete_jsonl, path_is_within_workspace, redact_sensitive};
    use serde_json::{json, Value};
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!("hphe-{name}-{suffix}"));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn event(workspace: &Path, session: &str, prompt: &str) -> Value {
        json!({
            "event": {"action": "prompt.submitted"},
            "prompt": {"content": prompt},
            "session": {"id": session, "working_directory": workspace.to_string_lossy()},
            "repository": workspace.to_string_lossy(),
            "harness": {"name": "devin-cli"},
            "timestamp": "2026-10-15T01:00:00Z"
        })
    }

    #[test]
    fn parser_keeps_an_incomplete_trailing_record_for_the_next_run() {
        let complete = b"{\"session\":{\"id\":\"first\"}}\n";
        let partial = b"{\"session\":{\"id\":\"second\"}}";
        let input = [complete.as_slice(), partial].concat();
        let parsed = parse_complete_jsonl(&input).unwrap();
        assert_eq!(parsed.events, vec![json!({"session": {"id": "first"}})]);
        assert_eq!(parsed.consumed_bytes, complete.len());
        assert!(parsed.has_partial_line);
    }

    #[test]
    fn parser_counts_malformed_complete_records_and_continues() {
        let input = b"not-json\n{\"session\":{\"id\":\"ok\"}}\n";
        let parsed = parse_complete_jsonl(input).unwrap();
        assert_eq!(parsed.malformed_lines, 1);
        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.consumed_bytes, input.len());
    }

    #[test]
    fn new_state_imports_legacy_offset_without_modifying_tracked_state() {
        use super::load_state;

        let temp = TempDir::new("beacon-legacy-state");
        let legacy = temp.0.join(".beacon-offset.json");
        let state = temp.0.join(".beacon-offset-state.json");
        let original = r#"{"offset":12345,"done":["old-session"]}"#;
        fs::write(&legacy, original).unwrap();

        let loaded = load_state(&state).unwrap();

        assert_eq!(loaded.offset, 12345);
        assert!(loaded.done.is_empty());
        assert!(!state.exists());
        assert_eq!(fs::read_to_string(legacy).unwrap(), original);
    }

    #[test]
    fn workspace_filter_uses_a_path_segment_boundary() {
        assert!(path_is_within_workspace(
            r"C:\work\some-repo\src\main.rs",
            r"C:\work\some-repo"
        ));
        assert!(!path_is_within_workspace(
            r"C:\work\some-repo-other\src\main.rs",
            r"C:\work\some-repo"
        ));
    }

    #[test]
    fn session_text_redacts_common_secret_shapes() {
        let redacted = redact_sensitive(
            "Authorization: Bearer abc.def API_KEY=nvapi-123456789012 password=hunter2",
        );
        assert!(!redacted.contains("abc.def"));
        assert!(!redacted.contains("nvapi-123456789012"));
        assert!(!redacted.contains("hunter2"));
        assert!(redacted.contains("[REDACTED]"));
    }

    #[test]
    fn ingestion_is_scoped_idempotent_and_redacts_memory_content() {
        let temp = TempDir::new("beacon");
        let workspace = temp.0.join("workspace");
        fs::create_dir_all(workspace.join(".agent/scripts")).unwrap();
        let workspace = workspace.canonicalize().unwrap();
        let log_path = temp.0.join("runtime.jsonl");
        let state_path = workspace.join(".agent/scripts/.beacon-offset.json");
        let mut line = serde_json::to_vec(&event(
            &workspace,
            "session-one",
            "Remember join; API_KEY=secret-value-123456",
        ))
        .unwrap();
        line.push(b'\n');
        fs::write(&log_path, &line).unwrap();
        let mut outside =
            serde_json::to_vec(&event(&temp.0.join("other"), "outside", "unrelated prompt"))
                .unwrap();
        outside.push(b'\n');
        fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap()
            .write_all(&outside)
            .unwrap();

        let mut writes = Vec::new();
        let first = ingest_with(
            &log_path,
            &state_path,
            &workspace,
            "hephaistos2-kit",
            20,
            false,
            |args| {
                writes.push(args.clone());
                Ok(json!({"stored": true}))
            },
        )
        .unwrap();
        assert_eq!(first["sessions"], 1, "{first}");
        assert_eq!(first["memoriesWritten"], 1);
        assert_eq!(writes.len(), 1);
        assert!(!writes[0]["content"]
            .as_str()
            .unwrap()
            .contains("session-one"));
        assert!(!writes[0]["content"]
            .as_str()
            .unwrap()
            .contains("secret-value-123456"));
        assert!(!writes[0]["content"]
            .as_str()
            .unwrap()
            .contains("unrelated prompt"));

        let second = ingest_with(
            &log_path,
            &state_path,
            &workspace,
            "hephaistos2-kit",
            20,
            false,
            |args| {
                writes.push(args.clone());
                Ok(json!({"stored": true}))
            },
        )
        .unwrap();
        assert_eq!(second["noActivity"], true);
        assert_eq!(writes.len(), 1);
    }

    #[test]
    fn ingestion_resumes_a_limited_batch_without_rewriting_completed_sessions() {
        let temp = TempDir::new("beacon-resume");
        let workspace = temp.0.join("workspace");
        fs::create_dir_all(workspace.join(".agent/scripts")).unwrap();
        let workspace = workspace.canonicalize().unwrap();
        let log_path = temp.0.join("runtime.jsonl");
        let state_path = workspace.join(".agent/scripts/.beacon-offset.json");
        let first = serde_json::to_vec(&event(&workspace, "session-a", "first session")).unwrap();
        let second = serde_json::to_vec(&event(&workspace, "session-b", "second session")).unwrap();
        fs::write(
            &log_path,
            [first, vec![b'\n'], second, vec![b'\n']].concat(),
        )
        .unwrap();
        let mut keys = Vec::new();
        let first_run = ingest_with(
            &log_path,
            &state_path,
            &workspace,
            "hephaistos2-kit",
            1,
            false,
            |args| {
                keys.push(args["key"].as_str().unwrap().to_string());
                Ok(json!({"stored": true}))
            },
        )
        .unwrap();
        assert_eq!(first_run["memoriesWritten"], 1);
        assert_eq!(first_run["sessionsPending"], 1);
        assert_eq!(first_run["offsetAdvanced"], false);

        let second_run = ingest_with(
            &log_path,
            &state_path,
            &workspace,
            "hephaistos2-kit",
            1,
            false,
            |args| {
                keys.push(args["key"].as_str().unwrap().to_string());
                Ok(json!({"stored": true}))
            },
        )
        .unwrap();
        assert_eq!(second_run["memoriesWritten"], 1);
        assert_eq!(second_run["offsetAdvanced"], true);
        assert_eq!(keys.len(), 2);
        assert_ne!(keys[0], keys[1]);

        let third_run = ingest_with(
            &log_path,
            &state_path,
            &workspace,
            "hephaistos2-kit",
            1,
            false,
            |args| {
                keys.push(args["key"].as_str().unwrap().to_string());
                Ok(json!({"stored": true}))
            },
        )
        .unwrap();
        assert_eq!(third_run["noActivity"], true);
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn dry_run_does_not_write_memory_or_advance_the_offset() {
        let temp = TempDir::new("beacon-dry");
        let workspace = temp.0.join("workspace");
        fs::create_dir_all(workspace.join(".agent/scripts")).unwrap();
        let workspace = workspace.canonicalize().unwrap();
        let log_path = temp.0.join("runtime.jsonl");
        let state_path = workspace.join(".agent/scripts/.beacon-offset.json");
        let line = serde_json::to_vec(&event(&workspace, "session-two", "protocol note")).unwrap();
        fs::write(&log_path, [line, vec![b'\n']].concat()).unwrap();
        let mut writes = 0;
        let report = ingest_with(
            &log_path,
            &state_path,
            &workspace,
            "hephaistos2-kit",
            20,
            true,
            |_| {
                writes += 1;
                Ok(json!({"stored": true}))
            },
        )
        .unwrap();
        assert_eq!(report["dryRun"], true);
        assert_eq!(writes, 0);
        assert!(!state_path.exists());
    }

    #[test]
    fn ingestion_persists_a_redacted_digest_to_the_unified_memory_store() {
        use crate::memory;
        use std::ffi::OsString;
        use std::sync::Mutex;

        static ENV_LOCK: Mutex<()> = Mutex::new(());
        struct EnvRestore(Option<OsString>);
        impl Drop for EnvRestore {
            fn drop(&mut self) {
                if let Some(value) = self.0.take() {
                    std::env::set_var("HPHE_MEMORY_ROOT", value);
                } else {
                    std::env::remove_var("HPHE_MEMORY_ROOT");
                }
            }
        }

        let _env_lock = ENV_LOCK.lock().unwrap();
        let temp = TempDir::new("beacon-memory");
        let previous = std::env::var_os("HPHE_MEMORY_ROOT");
        std::env::set_var("HPHE_MEMORY_ROOT", temp.0.join("memory"));
        let _restore = EnvRestore(previous);

        let workspace = temp.0.join("workspace");
        fs::create_dir_all(workspace.join(".agent/scripts")).unwrap();
        let workspace = workspace.canonicalize().unwrap();
        let log_path = temp.0.join("runtime.jsonl");
        let state_path = workspace.join(".agent/scripts/.beacon-offset.json");
        let line = serde_json::to_vec(&event(
            &workspace,
            "session-private-id",
            "API_KEY=secret-value-987654",
        ))
        .unwrap();
        fs::write(&log_path, [line, vec![b'\n']].concat()).unwrap();

        let report = ingest_with(
            &log_path,
            &state_path,
            &workspace,
            "beacon-test-project",
            20,
            false,
            memory::tool_memory_write,
        )
        .unwrap();
        let key = report["writtenKeys"][0].as_str().unwrap();
        let saved = memory::tool_memory_read(&json!({
            "projectId": "beacon-test-project",
            "key": key,
        }))
        .unwrap();

        assert_eq!(report["memoriesWritten"], 1);
        assert_eq!(saved["found"], true);
        let content = saved["memory"]["content"].as_str().unwrap();
        assert!(!content.contains("session-private-id"));
        assert!(!content.contains("secret-value-987654"));
    }
}
