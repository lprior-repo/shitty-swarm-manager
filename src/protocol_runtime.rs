#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::literal_string_with_formatting_args)]
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::unnecessary_box_returns)]
#![allow(clippy::branches_sharing_code)]
#![allow(clippy::too_many_lines)]

use crate::config::database_url_candidates_for_cli;
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, RepoId, SwarmDb, SwarmError, CANONICAL_COORDINATOR_SCHEMA_PATH};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

mod db_resolution;
mod handlers;
mod helpers;
mod input_parsing;
mod parsing;
mod validation;

const EMBEDDED_COORDINATOR_SCHEMA_SQL: &str = include_str!("../schema.sql");
const EMBEDDED_COORDINATOR_SCHEMA_REF: &str = "embedded:crates/swarm-coordinator/schema.sql";
const DEFAULT_DB_CONNECT_TIMEOUT_MS: u64 = 3_000;
const MIN_DB_CONNECT_TIMEOUT_MS: u64 = 100;
const MAX_DB_CONNECT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_HISTORY_LIMIT: i64 = 100;
const MAX_HISTORY_LIMIT: i64 = 10_000;
const MAX_REGISTER_COUNT: u32 = 100;
const MAX_EXTERNAL_OUTPUT_CAPTURE_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct ProtocolRequest {
    pub cmd: String,
    pub rid: Option<String>,
    pub dry: Option<bool>,
    #[serde(flatten)]
    pub args: Map<String, Value>,
}

pub use input_parsing::{ParseError, ParseInput};

fn bounded_history_limit(limit: Option<i64>) -> i64 {
    parsing::bounded_history_limit(limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT)
}

/// Main protocol loop for processing commands
///
/// # Errors
/// Returns `SwarmError` if:
/// - I/O errors occur while reading input
/// - Protocol processing errors occur
pub async fn run_protocol_loop() -> std::result::Result<(), SwarmError> {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut processed_non_empty_line = false;

    while let Some(line) = lines.next_line().await.map_err(SwarmError::IoError)? {
        if line.trim().is_empty() {
            continue;
        }

        processed_non_empty_line = true;
        process_protocol_line(&line).await?;
    }

    if !processed_non_empty_line {
        emit_no_input_envelope().await?;
    }

    Ok(())
}

async fn emit_no_input_envelope() -> std::result::Result<(), SwarmError> {
    let mut stdout = tokio::io::stdout();
    let envelope = ProtocolEnvelope::error(
        None,
        code::INVALID.to_string(),
        "No input received on stdin".to_string(),
    )
    .with_fix(
        "Provide one JSON command per line. Example: echo '{\"cmd\":\"doctor\"}' | swarm"
            .to_string(),
    )
    .with_ctx(json!({"stdin": "empty"}))
    .with_ms(0);

    let response_text = serde_json::to_string(&envelope).map_err(SwarmError::SerializationError)?;
    stdout
        .write_all(response_text.as_bytes())
        .await
        .map_err(SwarmError::IoError)?;
    stdout.write_all(b"\n").await.map_err(SwarmError::IoError)
}

/// Processes a single protocol line/command
///
/// # Errors
/// Returns `SwarmError` if:
/// - JSON parsing fails
/// - Protocol execution fails
/// - I/O errors occur while writing output
pub async fn process_protocol_line(line: &str) -> std::result::Result<(), SwarmError> {
    let mut stdout = tokio::io::stdout();
    let started = Instant::now();
    let maybe_rid = parse_rid(line);
    let parsed = serde_json::from_str::<ProtocolRequest>(line).map_err(|err| {
        ProtocolEnvelope::error(
            maybe_rid.clone(),
            code::INVALID.to_string(),
            format!("Invalid request JSON: {err}"),
        )
        .with_fix("Ensure request is valid JSON with a 'cmd' field. Example: echo '{\"cmd\":\"doctor\"}' | swarm".to_string())
        .with_ctx(json!({"line": line}))
    });

    let (envelope, audit_cmd, audit_args) = match parsed {
        Ok(request) => {
            let command_name = request.cmd.clone();
            let command_args = Value::Object(request.args.clone());
            let rid = request.rid.clone();
            let result = execute_request(request).await;
            let env = match result {
                Ok(success) => ProtocolEnvelope::success(rid, success.data)
                    .with_next(success.next)
                    .with_state(success.state),
                Err(failure) => *failure,
            };
            (
                env.with_ms(i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)),
                command_name,
                command_args,
            )
        }
        Err(env) => (
            env.with_ms(i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)),
            "invalid".to_string(),
            json!({"raw": line}),
        ),
    };

    let response_text = serde_json::to_string(&envelope).map_err(SwarmError::SerializationError)?;
    stdout
        .write_all(response_text.as_bytes())
        .await
        .map_err(SwarmError::IoError)?;
    stdout.write_all(b"\n").await.map_err(SwarmError::IoError)?;

    let mut audit_args = audit_args;
    if let Some(obj) = audit_args.as_object_mut() {
        if let Some(url_val) = obj.get_mut("database_url") {
            if let Some(url_str) = url_val.as_str() {
                if let Ok(mut url) = url::Url::parse(url_str) {
                    if url.password().is_some() {
                        let _ = url.set_password(Some("********"));
                        *url_val = json!(url.to_string());
                    }
                }
            }
        }
        if let Some(url_val) = obj.get_mut("url") {
            if let Some(url_str) = url_val.as_str() {
                if let Ok(mut url) = url::Url::parse(url_str) {
                    if url.password().is_some() {
                        let _ = url.set_password(Some("********"));
                        *url_val = json!(url.to_string());
                    }
                }
            }
        }
    }

    let candidates = database_url_candidates_for_cli();
    let audit_result = audit_request(
        &audit_cmd,
        maybe_rid.as_deref(),
        audit_args,
        envelope.ok,
        started.elapsed().as_millis() as u64,
        envelope.err.as_ref().map(|e| e.code.as_str()),
        &candidates,
    )
    .await;

    if let Err(e) = audit_result {
        // Log audit failure but don't fail the request
        eprintln!("WARN: Audit trail recording failed: {e}");
    }

    // CRITICAL FIX: Return error if the protocol request itself failed
    // This ensures proper exit codes are propagated to the caller
    if !envelope.ok {
        return Err(SwarmError::Internal(envelope.err.as_ref().map_or_else(
            || "Unknown protocol error".to_string(),
            |e| e.msg.clone(),
        )));
    }

    Ok(())
}

async fn execute_request(
    request: ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    validate_request_null_bytes(&request)?;

    match request.cmd.as_str() {
        "batch" => handlers::batch_ops::handle_batch(&request).await,
        _ => execute_request_no_batch(request).await,
    }
}

async fn execute_request_no_batch(
    request: ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    validate_request_args(&request)?;

    dispatch_request(&request).await
}

async fn dispatch_request(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let cmd = request.cmd.as_str();

    match cmd {
        "batch" => handlers::batch_ops::handle_batch(request).await,
        other => dispatch_no_batch(request, other).await,
    }
}

#[allow(clippy::large_stack_frames)]
async fn dispatch_no_batch(
    request: &ProtocolRequest,
    cmd: &str,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    match cmd {
        "?" | "help" => handlers::batch_ops::handle_help(request).await,
        "state" => handlers::state_ops::handle_state(request).await,
        "history" => handlers::state_ops::handle_history(request).await,
        "lock" => handlers::lock_ops::handle_lock(request).await,
        "unlock" => handlers::lock_ops::handle_unlock(request).await,
        "agents" => handlers::state_ops::handle_agents(request).await,
        "broadcast" => handlers::messaging_ops::handle_broadcast(request).await,
        "monitor" => handle_monitor(request).await,
        "register" => handle_register(request).await,
        "agent" => handle_agent(request).await,
        "status" => handle_status(request).await,
        "next" => handlers::qa_ops::handle_next(request).await,
        "claim-next" => handle_claim_next(request).await,
        "assign" => handle_assign(request).await,
        "run-once" => handle_run_once(request).await,
        "qa" => handlers::qa_ops::handle_qa(request).await,
        "resume" => handle_resume(request).await,
        "resume-context" => handle_resume_context(request).await,
        "artifacts" => handle_artifacts(request).await,
        "release" => handle_release(request).await,
        "init-db" => handlers::swarm_ops::handle_init_db(request).await,
        "init-local-db" => handlers::swarm_ops::handle_init_local_db(request).await,
        "spawn-prompts" => handle_spawn_prompts(request).await,
        "smoke" => handle_smoke(request).await,
        "prompt" => handle_prompt(request).await,
        "doctor" => handle_doctor(request).await,
        "load-profile" => handle_load_profile(request).await,
        "bootstrap" => handlers::swarm_ops::handle_bootstrap(request).await,
        "init" => handlers::swarm_ops::handle_init(request).await,
        other => Err(Box::new(ProtocolEnvelope::error(
            request.rid.clone(),
            code::INVALID.to_string(),
            format!("Unknown command: {other}"),
        ).with_fix("Use a valid command: init, doctor, status, next, claim-next, assign, run-ononce, qa, resume, artifacts, resume-context, agent, smoke, prompt, register, release, monitor, init-db, init-local-db, spawn-prompts, batch, bootstrap, state, or ?/help for help".to_string())
        .with_ctx(json!({"cmd": other}))))
    }
}

fn validate_request_args(
    request: &ProtocolRequest,
) -> std::result::Result<(), Box<ProtocolEnvelope>> {
    validation::validate_request_args(request)
}

fn validate_request_null_bytes(
    request: &ProtocolRequest,
) -> std::result::Result<(), Box<ProtocolEnvelope>> {
    validation::validate_request_null_bytes(request)
}

struct CommandSuccess {
    data: Value,
    next: String,
    state: Value,
}

fn project_next_recommendation(payload: &Value) -> Value {
    if payload.get("id").is_some() {
        return payload.clone();
    }

    if let Some(next) = payload.get("next") {
        return next.clone();
    }

    if let Some(recommendation) = payload.get("recommendation") {
        return recommendation.clone();
    }

    payload
        .pointer("/triage/quick_ref/top_picks/0")
        .cloned()
        .unwrap_or_else(|| payload.clone())
}

fn bead_id_from_recommendation(recommendation: &Value) -> Option<String> {
    recommendation
        .get("id")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
}

async fn current_repo_root() -> std::result::Result<PathBuf, Box<ProtocolEnvelope>> {
    Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| helpers::to_protocol_failure(e, None))
        .and_then(|output| {
            if output.status.success() {
                Ok(PathBuf::from(
                    String::from_utf8_lossy(&output.stdout).trim().to_string(),
                ))
            } else {
                Err(Box::new(
                    ProtocolEnvelope::error(
                        None,
                        code::INVALID.to_string(),
                        "Not in git repository".to_string(),
                    )
                    .with_fix("Run bootstrap from repository root".to_string()),
                ))
            }
        })
}

async fn run_external_json_command(
    program: &str,
    args: &[&str],
    rid: Option<String>,
    fix: &str,
) -> std::result::Result<Value, Box<ProtocolEnvelope>> {
    let timeout_ms = 15_000_u64;
    let mut child = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            Box::new(
                ProtocolEnvelope::error(
                    rid.clone(),
                    code::INTERNAL.to_string(),
                    format!("Failed to execute {program}: {err}"),
                )
                .with_fix(fix.to_string()),
            )
        })?;

    let stdout = child.stdout.take().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                code::INTERNAL.to_string(),
                format!("Failed to capture {program} stdout"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stderr = child.stderr.take().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                code::INTERNAL.to_string(),
                format!("Failed to capture {program} stderr"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stdout_task = tokio::spawn(async move {
        capture_stream_limited(stdout, MAX_EXTERNAL_OUTPUT_CAPTURE_BYTES).await
    });
    let stderr_task = tokio::spawn(async move {
        capture_stream_limited(stderr, MAX_EXTERNAL_OUTPUT_CAPTURE_BYTES).await
    });

    let status = if let Ok(wait_result) =
        tokio::time::timeout(Duration::from_millis(timeout_ms), child.wait()).await
    {
        wait_result.map_err(SwarmError::IoError).map_err(|err| {
            Box::new(
                ProtocolEnvelope::error(
                    rid.clone(),
                    code::INTERNAL.to_string(),
                    format!("Failed to wait for {program}: {err}"),
                )
                .with_fix(fix.to_string()),
            )
        })?
    } else {
        let _ = child.kill().await;
        return Err(Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                code::INTERNAL.to_string(),
                format!("{program} command timed out"),
            )
            .with_fix(fix.to_string())
            .with_ctx(json!({"program": program, "args": args, "timeout_ms": timeout_ms})),
        ));
    };

    let stdout_capture = stdout_task.await.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                code::INTERNAL.to_string(),
                format!("Failed to read {program} stdout: {err}"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stderr_capture = stderr_task.await.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                code::INTERNAL.to_string(),
                format!("Failed to read {program} stderr: {err}"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stdout_capture = stdout_capture.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                code::INTERNAL.to_string(),
                format!("Failed to read {program} stdout: {err}"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stderr_capture = stderr_capture.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                code::INTERNAL.to_string(),
                format!("Failed to read {program} stderr: {err}"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    if !status.success() {
        let exit_code = status.code().map_or(1, |code| code);
        let stderr = String::from_utf8_lossy(&stderr_capture.bytes)
            .trim()
            .to_string();
        return Err(Box::new(
            ProtocolEnvelope::error(
                rid,
                code::INTERNAL.to_string(),
                if stderr.is_empty() {
                    format!("{program} command failed")
                } else {
                    format!("{program} command failed: {stderr}")
                },
            )
            .with_fix(fix.to_string())
            .with_ctx(json!({
                "program": program,
                "exit_code": exit_code,
                "stderr": stderr,
                "stderr_truncated": stderr_capture.truncated,
            })),
        ));
    }

    let raw = String::from_utf8_lossy(&stdout_capture.bytes)
        .trim()
        .to_string();
    serde_json::from_str::<Value>(&raw).map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid,
                code::INVALID.to_string(),
                format!("{program} returned non-JSON output: {err}"),
            )
            .with_fix(fix.to_string())
            .with_ctx(json!({"raw": raw, "stdout_truncated": stdout_capture.truncated})),
        )
    })
}

#[derive(Debug, Clone)]
struct StreamCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

async fn capture_stream_limited<R>(
    mut stream: R,
    max_bytes: usize,
) -> std::result::Result<StreamCapture, SwarmError>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut truncated = false;
    let mut chunk = [0_u8; 8_192];

    loop {
        let read = stream.read(&mut chunk).await.map_err(SwarmError::IoError)?;
        if read == 0 {
            break;
        }

        let remaining = max_bytes.saturating_sub(bytes.len());
        if remaining == 0 {
            truncated = true;
            continue;
        }

        let to_copy = remaining.min(read);
        bytes.extend_from_slice(&chunk[..to_copy]);
        if to_copy < read {
            truncated = true;
        }
    }

    Ok(StreamCapture { bytes, truncated })
}

async fn run_external_json_command_with_ms(
    program: &str,
    args: &[&str],
    rid: Option<String>,
    fix: &str,
) -> std::result::Result<(Value, u64), Box<ProtocolEnvelope>> {
    let start = Instant::now();
    run_external_json_command(program, args, rid, fix)
        .await
        .map(|value| (value, elapsed_ms(start)))
}

fn elapsed_ms(start: Instant) -> u64 {
    let ms = start.elapsed().as_millis();
    u64::try_from(ms).map_or(u64::MAX, |value| value)
}

async fn handle_claim_next(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::orchestration::handle_claim_next(request).await
}

async fn handle_assign(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::orchestration::handle_assign(request).await
}

async fn handle_run_once(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::orchestration::handle_run_once(request).await
}

#[allow(clippy::too_many_lines)]
async fn handle_monitor(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::monitoring::handle_monitor(request).await
}

async fn handle_register(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::agent_lifecycle::handle_register(request).await
}

async fn handle_agent(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::agent_lifecycle::handle_agent(request).await
}

async fn handle_status(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::monitoring::handle_status(request).await
}

async fn handle_resume(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::resume::handle_resume(request).await
}

async fn handle_resume_context(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::resume::handle_resume_context(request).await
}

async fn handle_artifacts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::artifacts::handle_artifacts(request).await
}

async fn handle_release(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::agent_lifecycle::handle_release(request).await
}

async fn handle_spawn_prompts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::prompts::handle_spawn_prompts(request).await
}

async fn handle_prompt(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::prompts::handle_prompt(request).await
}

async fn handle_smoke(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::prompts::handle_smoke(request).await
}

async fn handle_doctor(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::doctor::handle_doctor(request).await
}

async fn handle_load_profile(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::load_profile::handle_load_profile(request).await
}

fn required_string_arg(
    request: &ProtocolRequest,
    key: &str,
) -> std::result::Result<String, Box<ProtocolEnvelope>> {
    helpers::required_string_arg(request, key)
}

async fn db_from_request(
    request: &ProtocolRequest,
) -> std::result::Result<SwarmDb, Box<ProtocolEnvelope>> {
    db_resolution::db_from_request(
        request,
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
    .await
}

async fn resolve_database_url_for_init(
    request: &ProtocolRequest,
) -> std::result::Result<String, Box<ProtocolEnvelope>> {
    db_resolution::resolve_database_url_for_init(
        request,
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
    .await
}

async fn try_connect_candidates(
    candidates: &[String],
    timeout_ms: u64,
) -> (Option<(SwarmDb, String)>, Vec<String>) {
    db_resolution::try_connect_candidates(candidates, timeout_ms).await
}

fn database_connect_timeout_ms() -> u64 {
    parsing::parse_database_connect_timeout_ms(
        std::env::var("SWARM_DB_CONNECT_TIMEOUT_MS").ok().as_deref(),
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
}

#[cfg(test)]
fn parse_database_connect_timeout_ms(raw: Option<&str>) -> u64 {
    parsing::parse_database_connect_timeout_ms(
        raw,
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
}

fn request_connect_timeout_ms(
    request: &ProtocolRequest,
) -> std::result::Result<u64, Box<ProtocolEnvelope>> {
    parsing::request_connect_timeout_ms(
        request,
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
}

async fn minimal_state_for_request(request: &ProtocolRequest) -> Value {
    helpers::minimal_state_for_request(
        request,
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
    .await
}

fn minimal_state_from_progress(progress: &crate::ProgressSummary) -> Value {
    helpers::minimal_state_from_progress(progress)
}

async fn check_command(command: &str) -> Value {
    match Command::new("bash")
        .arg("-lc")
        .arg(format!("command -v {command}"))
        .output()
        .await
    {
        Ok(output) => {
            if output.status.success() {
                json!({"name": command, "ok": true})
            } else {
                json!({"name": command, "ok": false, "fix": format!("Install '{}' and ensure it is on PATH.", command)})
            }
        }
        Err(_) => json!({
            "name": command,
            "ok": false,
            "fix": format!("Install '{}' and ensure it is on PATH.", command),
        }),
    }
}

async fn check_database_connectivity(request: &ProtocolRequest) -> Value {
    let explicit_database_url = request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let candidates =
        compose_database_url_candidates(explicit_database_url, database_url_candidates_for_cli());
    let timeout_ms =
        request_connect_timeout_ms(request).unwrap_or_else(|_| database_connect_timeout_ms());
    let (connected, failures) = try_connect_candidates(&candidates, timeout_ms).await;

    match connected {
        Some((_db, connected_url)) => {
            let source = if explicit_database_url == Some(connected_url.as_str()) {
                "request.database_url"
            } else {
                "discovered"
            };
            json!({"name": "database", "ok": true, "url": mask_database_url(&connected_url), "source": source})
        }
        None => json!({
            "name": "database",
            "ok": false,
            "source": if explicit_database_url.is_some() { "request.database_url+fallback" } else { "discovered" },
            "fix": if explicit_database_url.is_some() {
                "Check request.database_url, set DATABASE_URL, verify postgres is reachable, or run 'swarm init-local-db'"
            } else {
                "Set DATABASE_URL, verify postgres is reachable, or run 'swarm init-local-db'"
            },
            "errors": failures,
        }),
    }
}

fn compose_database_url_candidates(
    explicit_database_url: Option<&str>,
    discovered_candidates: Vec<String>,
) -> Vec<String> {
    let mut candidates = Vec::new();

    if let Some(explicit) = explicit_database_url {
        let trimmed = explicit.trim();
        if !trimmed.is_empty() {
            candidates.push(trimmed.to_string());
        }
    }

    for candidate in discovered_candidates {
        if !candidates.iter().any(|existing| existing == &candidate) {
            candidates.push(candidate);
        }
    }

    candidates
}

#[cfg(test)]
mod tests;

fn mask_database_url(url: &str) -> String {
    db_resolution::mask_database_url(url)
}

async fn audit_request(
    cmd: &str,
    rid: Option<&str>,
    args: Value,
    ok: bool,
    ms: u64,
    error_code: Option<&str>,
    candidates: &[String],
) -> std::result::Result<(), SwarmError> {
    let (connected, _failures) =
        try_connect_candidates(candidates, database_connect_timeout_ms()).await;
    match connected {
        Some((db, _used_url)) => {
            db.record_command_audit(cmd, rid, args, ok, ms, error_code)
                .await
        }
        None => Err(SwarmError::DatabaseError(
            "Audit database connection failed: no candidates succeeded".to_string(),
        )),
    }
}

fn dry_run_success(_request: &ProtocolRequest, steps: Vec<Value>, next: &str) -> CommandSuccess {
    CommandSuccess {
        data: json!({
            "dry": true,
            "would_do": steps,
            "estimated_ms": 250,
            "reversible": true,
            "side_effects": [],
        }),
        next: next.to_string(),
        state: json!({"total": 0, "active": 0}),
    }
}

fn to_protocol_failure(error: SwarmError, rid: Option<String>) -> Box<ProtocolEnvelope> {
    helpers::to_protocol_failure(error, rid)
}

fn parse_rid(raw: &str) -> Option<String> {
    parsing::parse_rid(raw)
}

fn dry_flag(request: &ProtocolRequest) -> bool {
    helpers::dry_flag(request)
}

async fn load_schema_sql(
    rid: Option<String>,
    schema: Option<&str>,
) -> std::result::Result<(String, String), Box<ProtocolEnvelope>> {
    match schema {
        Some(path) => fs::read_to_string(path)
            .await
            .map(|sql| (sql, path.to_string()))
            .map_err(|err| {
                Box::new(
                    ProtocolEnvelope::error(
                        rid,
                        code::INVALID.to_string(),
                        format!("Failed to read schema: {err}"),
                    )
                    .with_fix(format!(
                        "Run from swarm repo root or pass --schema <path> (canonical: {CANONICAL_COORDINATOR_SCHEMA_PATH})"
                    ))
                    .with_ctx(json!({"schema": path})),
                )
            }),
        None => Ok((
            EMBEDDED_COORDINATOR_SCHEMA_SQL.to_string(),
            EMBEDDED_COORDINATOR_SCHEMA_REF.to_string(),
        )),
    }
}

fn repo_id_from_request(request: &ProtocolRequest) -> RepoId {
    db_resolution::repo_id_from_request(request)
}
