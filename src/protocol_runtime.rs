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

use crate::agent_runtime::{run_agent, run_smoke_once};
use crate::config::{database_url_candidates_for_cli, load_config};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{
    code, AgentId, RepoId, ResumeContextContract, SwarmDb, SwarmError,
    CANONICAL_COORDINATOR_SCHEMA_PATH,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
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

fn parse_optional_non_negative_u64(
    request: &ProtocolRequest,
    field: &str,
) -> Result<Option<u64>, ParseError> {
    parsing::parse_optional_non_negative_u64(request, field)
}

fn parse_optional_non_negative_i64(
    request: &ProtocolRequest,
    field: &str,
) -> Result<Option<i64>, ParseError> {
    parsing::parse_optional_non_negative_i64(request, field)
}

fn parse_optional_non_negative_u32(
    request: &ProtocolRequest,
    field: &str,
) -> Result<Option<u32>, ParseError> {
    parsing::parse_optional_non_negative_u32(request, field)
}

fn bounded_history_limit(limit: Option<i64>) -> i64 {
    parsing::bounded_history_limit(limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT)
}


#[derive(Clone, Debug, Default)]
struct BatchAcc {
    pass: i64,
    fail: i64,
    items: Vec<Value>,
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
        "batch" => handle_batch(&request).await,
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
        "batch" => handle_batch(request).await,
        other => dispatch_no_batch(request, other).await,
    }
}

#[allow(clippy::large_stack_frames)]
async fn dispatch_no_batch(
    request: &ProtocolRequest,
    cmd: &str,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    match cmd {
        "?" | "help" => handle_help(request).await,
        "state" => handle_state(request).await,
        "history" => handle_history(request).await,
        "lock" => handle_lock(request).await,
        "unlock" => handle_unlock(request).await,
        "agents" => handle_agents(request).await,
        "broadcast" => handle_broadcast(request).await,
        "monitor" => handle_monitor(request).await,
        "register" => handle_register(request).await,
        "agent" => handle_agent(request).await,
        "status" => handle_status(request).await,
        "next" => handle_next(request).await,
        "claim-next" => handle_claim_next(request).await,
        "assign" => handle_assign(request).await,
        "run-once" => handle_run_once(request).await,
        "qa" => handle_qa(request).await,
        "resume" => handle_resume(request).await,
        "resume-context" => handle_resume_context(request).await,
        "artifacts" => handle_artifacts(request).await,
        "release" => handle_release(request).await,
        "init-db" => handle_init_db(request).await,
        "init-local-db" => handle_init_local_db(request).await,
        "spawn-prompts" => handle_spawn_prompts(request).await,
        "smoke" => handle_smoke(request).await,
        "prompt" => handle_prompt(request).await,
        "doctor" => handle_doctor(request).await,
        "load-profile" => handle_load_profile(request).await,
        "bootstrap" => handle_bootstrap(request).await,
        "init" => handle_init(request).await,
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
async fn handle_help(
    _request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    // Compact help format (default)
    let commands = vec![
        ("init", "Initialize swarm (bootstrap + init-db + register)"),
        ("doctor", "Environment health check"),
        ("status", "Show swarm state"),
        ("next", "Get top bead recommendation"),
        ("claim-next", "Select and claim top bead"),
        ("assign", "Assign explicit bead to agent"),
        ("run-once", "Run one compact orchestration cycle"),
        ("qa", "Run deterministic QA checks"),
        ("resume", "Show resumable context projections"),
        ("resume-context", "Show deep resume context payload"),
        ("artifacts", "Retrieve artifact records"),
        ("agent", "Run single agent"),
        ("monitor", "View agents/progress"),
        ("register", "Register agents"),
        ("release", "Release agent claim"),
        ("prompt", "Return agent/skill prompt"),
        ("smoke", "Run smoke test"),
        ("init-db", "Initialize database"),
        ("bootstrap", "Bootstrap repo"),
        ("batch", "Execute multiple commands"),
        ("state", "Full coordinator state"),
        ("?", "This help"),
    ];

    let command_map = commands
        .iter()
        .map(|(cmd, description)| (cmd.to_string(), Value::String(description.to_string())))
        .collect::<serde_json::Map<String, Value>>();

    Ok(CommandSuccess {
        data: json!({
            "n": "swarm",
            "v": env!("CARGO_PKG_VERSION"),
            "commands": command_map,
            "cmds": commands,
            "batch_input": {
                "required": "ops",
                "not": "cmds",
                "example": "echo '{\"cmd\":\"batch\",\"ops\":[{\"cmd\":\"doctor\"},{\"cmd\":\"status\"}]}' | swarm",
            }
        }),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(_request).await,
    })
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

async fn handle_next(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let total_start = Instant::now();
    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "bv_robot_next", "target": "bv --robot-next"})],
            "swarm next",
        ));
    }

    let (parsed, bv_ms) = run_external_json_command_with_ms(
        "bv",
        &["--robot-next"],
        request.rid.clone(),
        "Run `bv --robot-next` manually and verify beads index is available",
    )
    .await?;

    Ok(CommandSuccess {
        data: json!({
            "source": "bv --robot-next",
            "next": project_next_recommendation(&parsed),
            "timing": {
                "external": {
                    "bv_robot_next_ms": bv_ms,
                },
                "total_ms": elapsed_ms(total_start),
            }
        }),
        next: "br update <bead-id> --status in_progress".to_string(),
        state: minimal_state_for_request(request).await,
    })
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

async fn handle_qa(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let target = request
        .args
        .get("target")
        .and_then(Value::as_str)
        .map_or("smoke", |value| value);
    let agent_id = request
        .args
        .get("id")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .map_or(1_u32, |value| value);

    if target != "smoke" {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("Unknown qa target: {target}"),
            )
            .with_fix("Use `swarm qa --target smoke`".to_string())
            .with_ctx(json!({"target": target})),
        ));
    }

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "doctor"}),
                json!({"step": 2, "action": "state"}),
                json!({"step": 3, "action": "status"}),
                json!({"step": 4, "action": "agent", "target": agent_id, "dry": true}),
                json!({"step": 5, "action": "monitor", "target": "progress"}),
                json!({"step": 6, "action": "monitor", "target": "failures"}),
            ],
            "swarm status",
        ));
    }

    let doctor = handle_doctor(request).await?.data;
    let state = handle_state(request).await?.data;
    let status = handle_status(request).await?.data;

    let agent_dry_request = ProtocolRequest {
        cmd: "agent".to_string(),
        rid: request.rid.clone(),
        dry: Some(true),
        args: Map::from_iter(vec![("id".to_string(), Value::from(agent_id))]),
    };
    let agent_dry = handle_agent(&agent_dry_request).await?.data;

    let progress_request = ProtocolRequest {
        cmd: "monitor".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: Map::from_iter(vec![(
            "view".to_string(),
            Value::String("progress".to_string()),
        )]),
    };
    let progress = handle_monitor(&progress_request).await?.data;

    let failures_request = ProtocolRequest {
        cmd: "monitor".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: Map::from_iter(vec![(
            "view".to_string(),
            Value::String("failures".to_string()),
        )]),
    };
    let failures = handle_monitor(&failures_request).await?.data;

    Ok(CommandSuccess {
        data: json!({
            "target": target,
            "agent_id": agent_id,
            "checks": {
                "doctor": doctor,
                "state": state,
                "status": status,
                "agent_dry": agent_dry,
                "progress": progress,
                "failures": failures,
            },
        }),
        next: "swarm run-once --id <agent-id>".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_state(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let resource_limit = request
        .args
        .get("limit")
        .and_then(Value::as_u64)
        .map_or(25usize, |value| value as usize);
    let progress = db
        .get_progress(&repo_id)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    let all_resources = db
        .get_active_agents(&repo_id)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?
        .into_iter()
        .map(
            |(repo, agent_id, bead_id, status): (RepoId, u32, Option<String>, String)| {
                json!({
                    "id": format!("res_agent_{}", agent_id),
                    "name": format!("{}-{}", repo.value(), agent_id),
                    "status": status,
                    "created": now_ms(),
                    "updated": now_ms(),
                    "bead_id": bead_id,
                })
            },
        )
        .collect::<Vec<_>>();
    let truncated = all_resources.len() > resource_limit;
    let resources = all_resources
        .into_iter()
        .take(resource_limit)
        .collect::<Vec<_>>();

    let config = match db.get_config(&repo_id).await {
        Ok(cfg) => json!({
            "max_agents": cfg.max_agents,
            "max_implementation_attempts": cfg.max_implementation_attempts,
            "claim_label": cfg.claim_label,
            "swarm_status": cfg.swarm_status.as_str(),
        }),
        Err(_) => json!({"source": "unavailable"}),
    };

    Ok(CommandSuccess {
        data: json!({
            "initialized": true,
            "repo_id": repo_id.value(),
            "resources": resources,
            "resources_total": progress.working + progress.waiting + progress.errors,
            "resources_truncated": truncated,
            "health": {
                "database": true,
                "api": true,
            },
            "config": config,
            "warnings": [],
        }),
        next: "swarm status".to_string(),
        state: minimal_state_from_progress(&progress),
    })
}

async fn handle_history(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::HistoryInput::parse_input(request).map_err(|error| {
        let error_message = error.to_string();
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                error_message.clone(),
            )
            .with_fix("echo '{\"cmd\":\"history\",\"limit\":100}' | swarm".to_string())
            .with_ctx(json!({"error": error_message})),
        )
    })?;

    let requested_limit = input.limit;
    let limit = bounded_history_limit(requested_limit);
    let db: SwarmDb = db_from_request(request).await?;
    let actions = db
        .get_command_history(limit)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    let total = actions.len() as i64;
    let success = actions.iter().filter(|(_, _, _, _, ok, _, _)| *ok).count() as f64;
    let duration_total = actions
        .iter()
        .map(|(_, _, _, _, _, ms, _)| *ms as f64)
        .sum::<f64>();

    let mut error_frequency = BTreeMap::new();
    actions
        .iter()
        .filter_map(
            |(_, _, _, _, _, _, code): &(i64, i64, String, Value, bool, u64, Option<String>)| {
                code.as_ref()
            },
        )
        .for_each(|code: &String| {
            let next = error_frequency
                .get(code)
                .copied()
                .map_or(0_i64, |value| value)
                .saturating_add(1);
            error_frequency.insert(code.clone(), next);
        });

    let aggregates = json!({
        "success_rate": if total == 0 { 0.0 } else { success / total as f64 },
        "avg_duration_ms": if total == 0 { 0.0 } else { duration_total / total as f64 },
        "common_sequences": [],
        "error_frequency": error_frequency,
    });

    let actions_json = actions
        .into_iter()
        .map(|(seq, t, cmd, args, ok, ms, error_code)| {
            json!({
                "seq": seq,
                "t": t,
                "cmd": cmd,
                "args": args,
                "ok": ok,
                "ms": ms,
                "error_code": error_code,
            })
        })
        .collect::<Vec<_>>();

    Ok(CommandSuccess {
        data: json!({
            "actions": actions_json,
            "requested_limit": requested_limit,
            "effective_limit": limit,
            "total": total,
            "aggregates": aggregates,
        }),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_lock(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let resource = required_string_arg(request, "resource")?;

    if resource.trim().is_empty() {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "resource cannot be empty".to_string(),
            )
            .with_fix("Provide a non-empty resource. Example: {\"cmd\":\"lock\",\"resource\":\"repo-123\",\"agent\":\"agent-1\",\"ttl_ms\":30000}".to_string())
            .with_ctx(json!({"resource": resource})),
        ));
    }

    let agent = required_string_arg(request, "agent")?;
    let ttl_ms = request
        .args
        .get("ttl_ms")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing or invalid ttl_ms".to_string(),
                )
                .with_fix("swarm lock --resource <id> --agent <id> --ttl-ms 30000".to_string())
                .with_ctx(json!({"ttl_ms": "must be > 0"})),
            )
        })?;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "cleanup_expired_locks", "target": resource.clone()}),
                json!({"step": 2, "action": "acquire_lock", "target": resource.clone()}),
            ],
            "swarm lock --resource <id> --agent <id> --ttl-ms 30000",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let acquired = db
        .acquire_resource_lock(&resource, &agent, ttl_ms)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    match acquired {
        Some(until_at) => Ok(CommandSuccess {
            data: json!({"locked": true, "until": until_at.timestamp_millis()}),
            next: format!("swarm unlock --resource {resource} --agent {agent}"),
            state: minimal_state_for_request(request).await,
        }),
        None => Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::BUSY.to_string(),
                "Resource lock already held".to_string(),
            )
            .with_fix("sleep 1; swarm lock --resource <id> --agent <id> --ttl-ms 30000".to_string())
            .with_ctx(json!({"resource": resource, "agent": agent})),
        )),
    }
}

async fn handle_unlock(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let resource = required_string_arg(request, "resource")?;

    if resource.trim().is_empty() {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "resource cannot be empty".to_string(),
            )
            .with_fix("Provide a non-empty resource. Example: {\"cmd\":\"unlock\",\"resource\":\"repo-123\",\"agent\":\"agent-1\"}".to_string())
            .with_ctx(json!({"resource": resource})),
        ));
    }

    let agent = required_string_arg(request, "agent")?;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "unlock", "target": resource.clone()})],
            "swarm agents",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let unlocked = db
        .unlock_resource(&resource, &agent)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    if unlocked {
        Ok(CommandSuccess {
            data: json!({"unlocked": true}),
            next: "swarm agents".to_string(),
            state: minimal_state_for_request(request).await,
        })
    } else {
        Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::CONFLICT.to_string(),
                "Resource lock not owned by agent or missing".to_string(),
            )
            .with_fix("swarm agents".to_string())
            .with_ctx(json!({"resource": resource, "agent": agent})),
        ))
    }
}

async fn handle_agents(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db: SwarmDb = db_from_request(request).await?;
    let agents = db
        .list_active_resource_locks()
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?
        .into_iter()
        .map(|(resource, id, since, _): (String, String, i64, i64)| json!({"id": id, "resource": resource, "since": since}))
        .collect::<Vec<_>>();

    Ok(CommandSuccess {
        data: json!({"agents": agents}),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_broadcast(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let msg = required_string_arg(request, "msg")?;
    if msg.trim().is_empty() {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "msg cannot be empty".to_string(),
            )
            .with_fix("Provide a non-empty msg. Example: {\"cmd\":\"broadcast\",\"msg\":\"hello\",\"from\":\"agent-1\"}".to_string())
            .with_ctx(json!({"msg": msg})),
        ));
    }

    let from = required_string_arg(request, "from")?;
    if from.trim().is_empty() {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "from cannot be empty".to_string(),
            )
            .with_fix("Provide a non-empty from. Example: {\"cmd\":\"broadcast\",\"msg\":\"hello\",\"from\":\"agent-1\"}".to_string())
            .with_ctx(json!({"from": from})),
        ));
    }

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "broadcast", "target": msg.clone()})],
            "swarm agents",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let delivered_to = db
        .write_broadcast(&from, &msg)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"delivered_to": delivered_to}),
        next: "swarm agents".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_batch(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let cmds_alias_present = request.args.contains_key("cmds");
    let ops = request
        .args
        .get("ops")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            let fix_hint = if cmds_alias_present {
                "Use 'ops' (not 'cmds') for batch input. Example: echo '{\"cmd\":\"batch\",\"ops\":[{\"cmd\":\"doctor\"}]}' | swarm"
            } else {
                "Add 'ops' array to batch request. Example: echo '{\"cmd\":\"batch\",\"ops\":[{\"cmd\":\"doctor\"}]}' | swarm"
            };
            Box::new(ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "Missing ops array".to_string(),
            )
            .with_fix(fix_hint.to_string())
            .with_ctx(json!({"ops": "required", "cmds": "not supported"})))
        })?;

    if ops.is_empty() {
        return Err(Box::new(ProtocolEnvelope::error(
            request.rid.clone(),
            code::INVALID.to_string(),
            "Batch ops array cannot be empty".to_string(),
        )
        .with_fix("Provide at least one operation in the ops array. Example: {\"cmd\":\"batch\",\"ops\":[{\"cmd\":\"doctor\"}]}".to_string())
        .with_ctx(json!({"ops": ops.clone()}))));
    }

    if dry_flag(request) {
        let would_do = ops
            .iter()
            .enumerate()
            .map(|(idx, op): (usize, &Value)| {
                json!({
                    "step": (idx + 1) as i64,
                    "action": "execute",
                    "target": op
                        .get("cmd")
                        .and_then(Value::as_str)
                        .map_or("unknown", |value| value),
                })
            })
            .collect::<Vec<_>>();
        return Ok(dry_run_success(request, would_do, "swarm history"));
    }

    let items = ops
        .iter()
        .enumerate()
        .map(|(idx, op): (usize, &Value)| {
            serde_json::from_value::<ProtocolRequest>(op.clone())
                .map_err(|err| {
                    Box::new(
                        ProtocolEnvelope::error(
                            request.rid.clone(),
                            code::INVALID.to_string(),
                            format!("Invalid batch item {idx}: {err}"),
                        )
                        .with_fix(
                            "Ensure each batch item is valid JSON with a 'cmd' field".to_string(),
                        )
                        .with_ctx(json!({"index": idx})),
                    )
                })
                .and_then(|sub_request| {
                    if sub_request.cmd == "batch" {
                        Err(Box::new(
                            ProtocolEnvelope::error(
                                request.rid.clone(),
                                code::INVALID.to_string(),
                                "Nested batch is not supported".to_string(),
                            )
                            .with_fix("Split nested batch into top-level ops".to_string())
                            .with_ctx(json!({"index": idx})),
                        ))
                    } else {
                        Ok(sub_request)
                    }
                })
        })
        .collect::<Vec<_>>();

    let batch_result = process_batch_items(&items, 0, BatchAcc::default()).await;

    Ok(CommandSuccess {
        data: json!({
            "items": batch_result.items,
            "summary": {
                "total": batch_result.pass + batch_result.fail,
                "pass": batch_result.pass,
                "fail": batch_result.fail,
            }
        }),
        next: "swarm history".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

#[allow(clippy::too_many_lines)]
async fn handle_monitor(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::MonitorInput::parse_input(request).map_err(|error| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                error.to_string(),
            )
            .with_fix("echo '{\"cmd\":\"monitor\",\"view\":\"active\"}' | swarm".to_string())
            .with_ctx(json!({"error": error.to_string()})),
        )
    })?;

    let view = input.view.as_deref().map_or("active", |value| value);
    let db: SwarmDb = db_from_request(request).await?;

    let data = match view {
        "active" => {
            let repo_id = repo_id_from_request(request);
            let rows = db
                .get_active_agents(&repo_id)
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?
                .into_iter()
                .map(|(repo, agent_id, bead_id, status): (RepoId, u32, Option<String>, String)| {
                    json!({"repo": repo.value(), "agent_id": agent_id, "bead_id": bead_id, "status": status})
                })
                .collect::<Vec<_>>();
            json!({"view": "active", "repo_id": repo_id.value(), "rows": rows})
        }
        "progress" => {
            let repo_id = repo_id_from_request(request);
            let progress = db
                .get_progress(&repo_id)
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
            let (beads_by_status, beads_by_status_ms) =
                beads_by_status_summary_with_timing(request.rid.clone()).await;
            json!({
                "view": "progress",
                "total": progress.total_agents,
                "working": progress.working,
                "idle": progress.idle,
                "waiting": progress.waiting,
                "done": progress.completed,
                "closed": progress.completed,
                "errors": progress.errors,
                "error": progress.errors,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "beads_by_status": beads_by_status,
                "timing": {
                    "external": {
                        "br_list_ms": beads_by_status_ms,
                    }
                },
            })
        }
        "failures" => {
            let repo_id = repo_id_from_request(request);
            let rows = db
                .get_execution_events(&repo_id, None, 200)
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?
                .into_iter()
                .filter_map(|event| {
                    event.diagnostics.map(|diagnostics| {
                        json!({
                            "seq": event.seq,
                            "bead_id": event.bead_id,
                            "agent_id": event.agent_id,
                            "stage": event.stage,
                            "event_type": event.event_type,
                            "causation_id": event.causation_id,
                            "category": diagnostics.category,
                            "retryable": diagnostics.retryable,
                            "next_command": diagnostics.next_command,
                            "detail": diagnostics.detail,
                            "created_at": event.created_at,
                        })
                    })
                })
                .collect::<Vec<_>>();
            json!({"view": "failures", "rows": rows})
        }
        "events" => {
            let bead_filter = request.args.get("bead_id").and_then(Value::as_str);
            let repo_id = repo_id_from_request(request);
            let rows = db
                .get_execution_events(&repo_id, bead_filter, 200)
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?
                .into_iter()
                .map(|event| {
                    json!({
                        "seq": event.seq,
                        "schema_version": event.schema_version,
                        "event_type": event.event_type,
                        "entity_id": event.entity_id,
                        "bead_id": event.bead_id,
                        "agent_id": event.agent_id,
                        "stage": event.stage,
                        "causation_id": event.causation_id,
                        "diagnostics": event.diagnostics,
                        "payload": event.payload,
                        "created_at": event.created_at,
                    })
                })
                .collect::<Vec<_>>();
            json!({"view": "events", "rows": rows})
        }
        "messages" => {
            let rows = db
                .get_all_unread_messages()
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?
                .into_iter()
                .map(|message: crate::AgentMessage| {
                    json!({
                        "id": message.id,
                        "from_agent_id": message.from_agent_id,
                        "to_agent_id": message.to_agent_id,
                        "bead_id": message.bead_id.map(|b| b.value().to_string()),
                        "message_type": message.message_type.as_str(),
                        "subject": message.subject,
                        "created_at": message.created_at,
                        "read": message.read,
                    })
                })
                .collect::<Vec<_>>();
            json!({"view": "messages", "rows": rows})
        }
        _ => {
            return Err(Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Unknown monitor view".to_string(),
                )
                .with_fix("swarm monitor --view active".to_string())
                .with_ctx(json!({"view": view})),
            ))
        }
    };

    Ok(CommandSuccess {
        data,
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_register(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::RegisterInput::parse_input(request).map_err(|error| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                error.to_string(),
            )
            .with_fix("echo '{\"cmd\":\"register\",\"count\":3}' | swarm".to_string())
            .with_ctx(json!({"error": error.to_string()})),
        )
    })?;

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id_from_context = repo_id_from_request(request);
    let config = db.get_config(&repo_id_from_context).await.ok();

    let count = input
        .count
        .or_else(|| config.as_ref().map(|c| c.max_agents))
        .map_or(10, |value| value);

    if count == 0 {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "count must be greater than 0".to_string(),
            )
            .with_fix(
                "Provide a positive count. Example: {\"cmd\":\"register\",\"count\":3}".to_string(),
            )
            .with_ctx(json!({"count": count})),
        ));
    }

    if count > MAX_REGISTER_COUNT {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("count must be less than or equal to {MAX_REGISTER_COUNT}"),
            )
            .with_fix(format!(
                "Provide count <= {MAX_REGISTER_COUNT}. Example: {{\"cmd\":\"register\",\"count\":10}}"
            ))
            .with_ctx(json!({"count": count, "max_count": MAX_REGISTER_COUNT})),
        ));
    }

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "register_repo", "target": "current_repo"}),
                json!({"step": 2, "action": "register_agents", "target": count}),
            ],
            "swarm status",
        ));
    }

    let repo_id = RepoId::from_current_dir().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "Not in a git repository".to_string(),
            )
            .with_fix("Run command from a git repository root".to_string()),
        )
    })?;
    db.register_repo(&repo_id, repo_id.value(), ".")
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    if let Some(explicit_count) = input.count {
        let _ = db.update_config(explicit_count).await;
    }

    register_agents_recursive(&db, &repo_id, 1, count, request.rid.clone()).await?;

    Ok(CommandSuccess {
        data: json!({"repo": repo_id.value(), "count": count}),
        next: "swarm status".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn register_agents_recursive<'a>(
    db: &'a SwarmDb,
    repo_id: &'a RepoId,
    next: u32,
    count: u32,
    rid: Option<String>,
) -> Pin<Box<dyn Future<Output = std::result::Result<(), Box<ProtocolEnvelope>>> + Send + 'a>> {
    Box::pin(async move {
        if next > count {
            Ok(())
        } else {
            db.register_agent(&AgentId::new(repo_id.clone(), next))
                .await
                .map_err(|e| to_protocol_failure(e, rid.clone()))?;

            register_agents_recursive(db, repo_id, next.saturating_add(1), count, rid).await
        }
    })
}

async fn handle_agent(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::AgentInput::parse_input(request).map_err(|e| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                e.to_string(),
            )
            .with_fix("echo '{\"cmd\":\"agent\",\"id\":1}' | swarm".to_string())
            .with_ctx(json!({"error": e.to_string()})),
        )
    })?;

    if dry_flag(request) || input.dry.unwrap_or(false) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "run_agent", "target": input.id})],
            "swarm status",
        ));
    }

    let config = load_config();
    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = RepoId::from_current_dir().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "Not in git repository".to_string(),
            )
            .with_fix("Run from repo root".to_string()),
        )
    })?;
    run_agent(
        &db,
        &AgentId::new(repo_id, input.id),
        &config.stage_commands,
    )
    .await
    .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"agent_id": input.id, "status": "completed"}),
        next: "swarm monitor --view progress".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_status(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let total_start = Instant::now();
    let connect_start = Instant::now();
    let db: SwarmDb = db_from_request(request).await?;
    let db_connect_ms = elapsed_ms(connect_start);
    let repo_id = repo_id_from_request(request);
    let progress_start = Instant::now();
    let progress = db
        .get_progress(&repo_id)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    let db_progress_ms = elapsed_ms(progress_start);
    let (beads_by_status, beads_by_status_ms) =
        beads_by_status_summary_with_timing(request.rid.clone()).await;
    Ok(CommandSuccess {
        data: json!({
            "working": progress.working,
            "idle": progress.idle,
            "waiting": progress.waiting,
            "done": progress.completed,
            "closed": progress.completed,
            "errors": progress.errors,
            "error": progress.errors,
            "total": progress.total_agents,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "beads_by_status": beads_by_status,
            "timing": {
                "db": {
                    "connect_ms": db_connect_ms,
                    "get_progress_ms": db_progress_ms,
                },
                "external": {
                    "br_list_ms": beads_by_status_ms,
                },
                "total_ms": elapsed_ms(total_start),
            }
        }),
        next: "swarm monitor --view progress".to_string(),
        state: minimal_state_from_progress(&progress),
    })
}

async fn beads_by_status_summary_with_timing(rid: Option<String>) -> (Value, u64) {
    let (payload, elapsed) = run_external_json_command_with_ms(
        "br",
        &["list", "--json"],
        rid,
        "Run `br list --json` manually and verify beads workspace is initialized",
    )
    .await
    .map_or_else(
        |_| (Value::Array(Vec::new()), 0_u64),
        |(value, ms)| (value, ms),
    );

    let counts = payload.as_array().cloned().map_or_else(
        || {
            BTreeMap::from([
                ("open".to_string(), 0_u64),
                ("in_progress".to_string(), 0_u64),
                ("blocked".to_string(), 0_u64),
                ("deferred".to_string(), 0_u64),
                ("closed".to_string(), 0_u64),
            ])
        },
        |rows| {
            let mut by_status = BTreeMap::from([
                ("open".to_string(), 0_u64),
                ("in_progress".to_string(), 0_u64),
                ("blocked".to_string(), 0_u64),
                ("deferred".to_string(), 0_u64),
                ("closed".to_string(), 0_u64),
            ]);

            for row in rows {
                if let Some(status) = row.get("status").and_then(Value::as_str) {
                    let current = by_status.get(status).copied().map_or(0_u64, |value| value);
                    let next = current.saturating_add(1);
                    by_status.insert(status.to_string(), next);
                }
            }

            by_status
        },
    );

    (json!(counts), elapsed)
}

async fn handle_resume(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let contexts = db
        .get_resume_context_projections(&repo_id)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    let contracts = contexts
        .iter()
        .map(ResumeContextContract::from_projection)
        .collect::<Vec<_>>();

    Ok(CommandSuccess {
        data: json!({
            "contexts": contracts,
        }),
        next: "swarm monitor --view failures".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_resume_context(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let bead_filter = parse_resume_context_bead_filter(request)?;

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let contexts = db
        .get_deep_resume_contexts(&repo_id)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    let selected = if let Some(ref bead_id) = bead_filter {
        let filtered = contexts
            .into_iter()
            .filter(|context| context.bead_id == *bead_id)
            .collect::<Vec<_>>();
        if filtered.is_empty() {
            return Err(Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::NOTFOUND.to_string(),
                    format!("Bead {bead_id} not found or not resumable"),
                )
                .with_fix("swarm resume-context --bead-id <bead-id>".to_string())
                .with_ctx(json!({"bead_id": bead_id})),
            ));
        }
        filtered
    } else {
        contexts
    };

    Ok(CommandSuccess {
        data: json!({"contexts": selected}),
        next: "swarm monitor --view failures".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn parse_resume_context_bead_filter(
    request: &ProtocolRequest,
) -> std::result::Result<Option<String>, Box<ProtocolEnvelope>> {
    let Some(raw) = request.args.get("bead_id") else {
        return Ok(None);
    };

    let bead_id = raw.as_str().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "bead_id must be a string".to_string(),
            )
            .with_fix("Use --bead-id <bead-id> with a non-empty string value".to_string())
            .with_ctx(json!({"bead_id": raw})),
        )
    })?;

    if bead_id.trim().is_empty() {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "bead_id cannot be empty".to_string(),
            )
            .with_fix("Use --bead-id <bead-id> with a non-empty value".to_string())
            .with_ctx(json!({"bead_id": bead_id})),
        ));
    }

    Ok(Some(bead_id.to_string()))
}

async fn handle_artifacts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::artifacts::handle_artifacts(request).await
}

async fn handle_release(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let agent_id = request
        .args
        .get("agent_id")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing agent_id".to_string(),
                )
                .with_fix("swarm release --agent-id 1".to_string())
                .with_ctx(json!({"agent_id": "required"})),
            )
        })? as u32;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "release_agent", "target": agent_id})],
            "swarm status",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let released = db
        .release_agent(&AgentId::new(repo_id, agent_id))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"agent_id": agent_id, "released_bead": released.map(|b| b.value().to_string())}),
        next: "swarm status".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_init_db(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::InitDbInput::parse_input(request).map_err(|error| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                error.to_string(),
            )
            .with_fix("echo '{\"cmd\":\"init-db\",\"seed_agents\":12}' | swarm".to_string())
            .with_ctx(json!({"error": error.to_string()})),
        )
    })?;

    let schema = input
        .schema
        .as_deref()
        .map(PathBuf::from)
        .map(|value| value.display().to_string());
    let seed_agents = input.seed_agents.map_or(12, |value| value);

    if dry_flag(request) {
        let dry_database_target = input.url.as_deref().map_or_else(
            || "auto-discover-on-execution".to_string(),
            mask_database_url,
        );
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "connect_db", "target": dry_database_target}),
                json!({"step": 2, "action": "apply_schema", "target": schema.clone().unwrap_or_else(|| EMBEDDED_COORDINATOR_SCHEMA_REF.to_string())}),
                json!({"step": 3, "action": "seed_agents", "target": seed_agents}),
            ],
            "swarm state",
        ));
    }

    let url = resolve_database_url_for_init(request).await?;

    let (schema_sql, schema_ref) = load_schema_sql(request.rid.clone(), schema.as_deref()).await?;
    let db: SwarmDb = SwarmDb::new(&url)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    db.initialize_schema_from_sql(&schema_sql)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    db.update_config(seed_agents)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    db.seed_idle_agents(seed_agents)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({
            "database_url": mask_database_url(&url),
            "schema": schema_ref,
            "seed_agents": seed_agents
        }),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

#[allow(clippy::too_many_lines)]
async fn handle_init_local_db(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let container_name = request
        .args
        .get("container_name")
        .and_then(Value::as_str)
        .map_or("shitty-swarm-manager-db", |value| value)
        .to_string();
    let port = request
        .args
        .get("port")
        .and_then(Value::as_u64)
        .map_or(5437, |value| value) as u16;
    let user = request
        .args
        .get("user")
        .and_then(Value::as_str)
        .map_or("shitty_swarm_manager", |value| value)
        .to_string();
    let database = request
        .args
        .get("database")
        .and_then(Value::as_str)
        .map_or("shitty_swarm_manager_db", |value| value)
        .to_string();
    let schema = request
        .args
        .get("schema")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .map(|value| value.display().to_string());
    let seed_agents = request
        .args
        .get("seed_agents")
        .and_then(Value::as_u64)
        .map_or(12, |value| value) as u32;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "docker_start_or_run", "target": container_name.clone()}),
                json!({"step": 2, "action": "init_db", "target": schema.clone().unwrap_or_else(|| EMBEDDED_COORDINATOR_SCHEMA_REF.to_string())}),
            ],
            "swarm state",
        ));
    }

    let port_mapping = format!("{port}:5432");
    // 1. Try to start existing container
    let _ = Command::new("docker")
        .args(["start", container_name.as_str()])
        .output()
        .await;

    // 2. Try to run new container if start failed or container didn't exist
    let _ = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            container_name.as_str(),
            "-p",
            port_mapping.as_str(),
            "-e",
            format!("POSTGRES_USER={user}").as_str(),
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "-e",
            format!("POSTGRES_DB={database}").as_str(),
            "postgres:16",
        ])
        .output()
        .await;

    // 3. Wait for Postgres to be ready inside the container
    let mut retry_count = 0;
    let max_retries = 10;
    while retry_count < max_retries {
        let ready_check = Command::new("docker")
            .args(["exec", container_name.as_str(), "pg_isready", "-U", &user])
            .output()
            .await;

        if let Ok(check) = ready_check {
            if check.status.success() {
                break;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        retry_count += 1;
    }

    let url = format!("postgresql://{user}@localhost:{port}/{database}");

    // 4. Idempotent bootstrap of repository config
    let bootstrap_request = ProtocolRequest {
        cmd: "bootstrap".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: Map::new(),
    };
    let _ = handle_bootstrap(&bootstrap_request).await?;

    let mut init_args = Map::from_iter(vec![
        ("url".to_string(), Value::String(url.clone())),
        ("seed_agents".to_string(), Value::from(seed_agents)),
    ]);
    if let Some(schema_value) = schema {
        init_args.insert("schema".to_string(), Value::String(schema_value));
    }

    let init_request = ProtocolRequest {
        cmd: "init-db".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: init_args,
    };
    let _ = handle_init_db(&init_request).await?;

    Ok(CommandSuccess {
        data: json!({
            "container": container_name,
            "database_url": mask_database_url(&url),
            "seed_agents": seed_agents
        }),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_spawn_prompts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let (template_text, template_name) =
        if let Some(path) = request.args.get("template").and_then(Value::as_str) {
            let text = fs::read_to_string(path).await.map_err(|err| {
                Box::new(
                    ProtocolEnvelope::error(
                        request.rid.clone(),
                        code::NOTFOUND.to_string(),
                        format!("Template file not found: {err}"),
                    )
                    .with_fix(format!("Ensure {path} exists"))
                    .with_ctx(json!({"template": path})),
                )
            })?;
            (text, path.to_string())
        } else {
            let repo_root = current_repo_root().await?;
            let template_path = crate::prompts::canonical_agent_prompt_path(&repo_root);
            let text = crate::prompts::load_agent_prompt_template(&repo_root)
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
            (text, template_path.to_string_lossy().to_string())
        };

    let out_dir = request
        .args
        .get("out_dir")
        .and_then(Value::as_str)
        .map_or(".agents/generated", |value| value);

    let count = request
        .args
        .get("count")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(10);

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "read_template", "target": template_name}),
                json!({"step": 2, "action": "write_prompts", "target": count, "dir": out_dir}),
            ],
            "swarm monitor --view progress",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let configured_count = db
        .get_config(&repo_id)
        .await
        .ok()
        .map_or(count, |cfg| cfg.max_agents);

    fs::create_dir_all(out_dir)
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    spawn_prompts_recursive(
        out_dir,
        &template_text,
        1,
        configured_count,
        request.rid.clone(),
    )
    .await?;

    Ok(CommandSuccess {
        data: json!({"count": configured_count, "out_dir": out_dir, "template": template_name}),
        next: "swarm monitor --view active".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn spawn_prompts_recursive<'a>(
    out_dir: &'a str,
    template_text: &'a str,
    next: u32,
    count: u32,
    rid: Option<String>,
) -> Pin<Box<dyn Future<Output = std::result::Result<(), Box<ProtocolEnvelope>>> + Send + 'a>> {
    Box::pin(async move {
        if next > count {
            Ok(())
        } else {
            let file = format!("{out_dir}/agent_{next:02}.md");
            fs::write(file, template_text.replace("{N}", &next.to_string()))
                .await
                .map_err(SwarmError::IoError)
                .map_err(|e| to_protocol_failure(e, rid.clone()))?;

            spawn_prompts_recursive(out_dir, template_text, next.saturating_add(1), count, rid)
                .await
        }
    })
}

async fn handle_prompt(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::PromptInput::parse_input(request).map_err(|error| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                error.to_string(),
            )
            .with_fix("echo '{\"cmd\":\"prompt\",\"id\":1}' | swarm".to_string())
            .with_ctx(json!({"error": error.to_string()})),
        )
    })?;

    if let Some(skill_name) = input.skill.as_deref() {
        if let Some(prompt) = crate::skill_prompts::get_skill_prompt(skill_name) {
            return Ok(CommandSuccess {
                data: json!({"skill": skill_name, "prompt": prompt}),
                next: "swarm monitor --view progress".to_string(),
                state: minimal_state_for_request(request).await,
            });
        }
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::NOTFOUND.to_string(),
                format!("Skill prompt not found: {skill_name}"),
            )
            .with_fix(
                "Use a valid skill: rust-contract, implement, qa-enforcer, red-queen".to_string(),
            )
            .with_ctx(json!({"skill": skill_name})),
        ));
    }

    let id = input.id;

    let repo_root = current_repo_root().await?;
    let prompt = crate::prompts::get_agent_prompt(&repo_root, id)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"agent_id": id, "prompt": prompt}),
        next: format!("swarm agent --id {id}"),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_smoke(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let id = request
        .args
        .get("id")
        .and_then(Value::as_u64)
        .map_or(1, |value| value) as u32;
    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "run_smoke", "target": id})],
            "swarm monitor --view progress",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    run_smoke_once(&db, &AgentId::new(repo_id, id))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"agent_id": id, "status": "completed"}),
        next: "swarm monitor --view progress".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_doctor(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let total_start = Instant::now();
    let moon_start = Instant::now();
    let moon = check_command("moon").await;
    let moon_ms = elapsed_ms(moon_start);
    let br_start = Instant::now();
    let br = check_command("br").await;
    let br_ms = elapsed_ms(br_start);
    let jj_start = Instant::now();
    let jj = check_command("jj").await;
    let jj_ms = elapsed_ms(jj_start);
    let zjj_start = Instant::now();
    let zjj = check_command("zjj").await;
    let zjj_ms = elapsed_ms(zjj_start);
    let psql_start = Instant::now();
    let psql = check_command("psql").await;
    let psql_ms = elapsed_ms(psql_start);
    let database_start = Instant::now();
    let database = check_database_connectivity(request).await;
    let database_ms = elapsed_ms(database_start);
    let mut checks = vec![moon, br, jj, zjj, psql];
    checks.push(database);
    let failed = checks
        .iter()
        .filter(|check| !check["ok"].as_bool().is_some_and(|value| value))
        .count() as i64;
    let passed = checks.len() as i64 - failed;

    // Compact machine-readable format (default)
    let check_results: Vec<Value> = checks
        .iter()
        .map(|check| {
            json!({
                "n": check["name"],
                "ok": check["ok"]
            })
        })
        .collect();

    Ok(CommandSuccess {
        data: json!({
            "v": "v1",
            "h": failed == 0,
            "p": passed,
            "f": failed,
            "c": check_results,
            "timing": {
                "checks_ms": {
                    "moon": moon_ms,
                    "br": br_ms,
                    "jj": jj_ms,
                    "zjj": zjj_ms,
                    "psql": psql_ms,
                    "database": database_ms,
                },
                "total_ms": elapsed_ms(total_start),
            }
        }),
        next: if failed == 0 {
            "swarm state".to_string()
        } else {
            "swarm doctor".to_string()
        },
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_load_profile(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let agents = request
        .args
        .get("agents")
        .and_then(Value::as_u64)
        .map_or(90, |value| value) as u32;
    let rounds = request
        .args
        .get("rounds")
        .and_then(Value::as_u64)
        .map_or(5, |value| value) as u32;
    let timeout_ms = request
        .args
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .map_or(1500, |value| value);

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "load_profile", "target": format!("{}x{}", agents, rounds)}),
            ],
            "swarm status",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    db.seed_idle_agents(agents)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    db.enqueue_backlog_batch(&repo_id, "load", agents.saturating_mul(rounds))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    let stats = load_profile_recursive(
        &db,
        &repo_id,
        0,
        rounds,
        agents,
        timeout_ms,
        LoadStats::default(),
    )
    .await?;

    Ok(CommandSuccess {
        data: json!({
            "agents": agents,
            "rounds": rounds,
            "timeouts": stats.timeout,
            "errors": stats.error,
            "successful_claims": stats.success,
            "empty_claims": stats.empty,
        }),
        next: "swarm status".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn load_profile_recursive<'a>(
    db: &'a SwarmDb,
    repo_id: &'a RepoId,
    current_round: u32,
    total_rounds: u32,
    agents_per_round: u32,
    timeout_ms: u64,
    stats: LoadStats,
) -> Pin<Box<dyn Future<Output = std::result::Result<LoadStats, Box<ProtocolEnvelope>>> + Send + 'a>>
{
    Box::pin(async move {
        if current_round >= total_rounds {
            Ok(stats)
        } else {
            let round_stats = load_profile_round_recursive(
                db,
                repo_id,
                1,
                agents_per_round,
                timeout_ms,
                LoadStats::default(),
            )
            .await?;

            let next_stats = LoadStats {
                success: stats.success.saturating_add(round_stats.success),
                empty: stats.empty.saturating_add(round_stats.empty),
                timeout: stats.timeout.saturating_add(round_stats.timeout),
                error: stats.error.saturating_add(round_stats.error),
            };

            load_profile_recursive(
                db,
                repo_id,
                current_round.saturating_add(1),
                total_rounds,
                agents_per_round,
                timeout_ms,
                next_stats,
            )
            .await
        }
    })
}

fn load_profile_round_recursive<'a>(
    db: &'a SwarmDb,
    repo_id: &'a RepoId,
    agent_num: u32,
    total_agents: u32,
    timeout_ms: u64,
    mut stats: LoadStats,
) -> Pin<Box<dyn Future<Output = std::result::Result<LoadStats, Box<ProtocolEnvelope>>> + Send + 'a>>
{
    Box::pin(async move {
        if agent_num > total_agents {
            Ok(stats)
        } else {
            let timeout_dur = tokio::time::Duration::from_millis(timeout_ms);
            let claim = tokio::time::timeout(
                timeout_dur,
                db.claim_next_bead(&AgentId::new(repo_id.clone(), agent_num)),
            )
            .await;

            match claim {
                Ok(Ok(Some(_))) => stats.success = stats.success.saturating_add(1),
                Ok(Ok(None)) => stats.empty = stats.empty.saturating_add(1),
                Ok(Err(_)) => stats.error = stats.error.saturating_add(1),
                Err(_) => stats.timeout = stats.timeout.saturating_add(1),
            }

            load_profile_round_recursive(
                db,
                repo_id,
                agent_num.saturating_add(1),
                total_agents,
                timeout_ms,
                stats,
            )
            .await
        }
    })
}

#[derive(Default, Clone, Copy)]
struct LoadStats {
    success: u64,
    empty: u64,
    timeout: u64,
    error: u64,
}

async fn handle_bootstrap(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let repo_root: PathBuf = current_repo_root().await?;
    let swarm_dir = repo_root.join(".swarm");
    let config_path = swarm_dir.join("config.toml");
    let ignore_path = swarm_dir.join(".swarmignore");

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "create_dir", "target": swarm_dir.display().to_string()}),
                json!({"step": 2, "action": "write_config", "target": config_path.display().to_string()}),
            ],
            "swarm doctor",
        ));
    }

    fs::create_dir_all(&swarm_dir)
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    let mut actions = Vec::new();
    if !config_path.exists() {
        fs::write(
            &config_path,
            "database_url = \"postgresql://shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db\"\nrust_contract_cmd = \"br show {bead_id}\"\nimplement_cmd = \"jj status\"\nqa_enforcer_cmd = \"moon run :quick\"\nred_queen_cmd = \"moon run :test\"\n",
        )
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
        actions.push("created_config");
    }
    if !ignore_path.exists() {
        fs::write(&ignore_path, "*.log\n.cache/\ntemp/\n")
            .await
            .map_err(SwarmError::IoError)
            .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
        actions.push("created_swarmignore");
    }

    Ok(CommandSuccess {
        data: json!({
            "repo_root": repo_root.display().to_string(),
            "swarm_dir": swarm_dir.display().to_string(),
            "actions_taken": actions,
            "idempotent": true,
        }),
        next: "swarm doctor".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

#[allow(clippy::too_many_lines)]
async fn handle_init(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let seed_agents = request
        .args
        .get("seed_agents")
        .and_then(Value::as_u64)
        .map_or(12, |value| value) as u32;
    let db_url = request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);
    let schema = request
        .args
        .get("schema")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "bootstrap", "target": "repository"}),
                json!({"step": 2, "action": "init_db", "target": db_url.as_ref().map_or_else(|| "auto-discover".to_string(), |url| mask_database_url(url))}),
                json!({"step": 3, "action": "register", "target": seed_agents}),
            ],
            "swarm doctor",
        ));
    }

    let mut steps = Vec::new();
    let mut errors = Vec::new();

    // Step 1: Bootstrap
    match handle_bootstrap(request).await {
        Ok(success) => {
            steps
                .push(json!({"step": 1, "action": "bootstrap", "status": "ok", "d": success.data}));
        }
        Err(e) => {
            errors.push(json!({"step": 1, "action": "bootstrap", "err": e.err}));
        }
    }

    // Step 2: Initialize database
    let init_db_request = ProtocolRequest {
        cmd: "init-db".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: {
            let mut args =
                Map::from_iter(vec![("seed_agents".to_string(), Value::from(seed_agents))]);
            if let Some(url) = db_url.clone() {
                args.insert("url".to_string(), Value::String(url));
            }
            if let Some(schema_value) = schema.clone() {
                args.insert("schema".to_string(), Value::String(schema_value));
            }
            args
        },
    };
    match handle_init_db(&init_db_request).await {
        Ok(success) => {
            steps.push(json!({"step": 2, "action": "init_db", "status": "ok", "d": success.data}));
        }
        Err(e) => {
            errors.push(json!({"step": 2, "action": "init_db", "err": e.err}));
        }
    }

    // Step 3: Register agents
    let register_request = ProtocolRequest {
        cmd: "register".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: Map::from_iter(vec![("count".to_string(), Value::from(seed_agents))]),
    };
    match handle_register(&register_request).await {
        Ok(success) => {
            steps.push(json!({"step": 3, "action": "register", "status": "ok", "d": success.data}));
        }
        Err(e) => {
            errors.push(json!({"step": 3, "action": "register", "err": e.err}));
        }
    }

    if errors.is_empty() {
        Ok(CommandSuccess {
            data: json!({
                "initialized": true,
                "steps": steps,
                "database_url": db_url.as_ref().map_or_else(|| "auto-discover".to_string(), |url| mask_database_url(url)),
                "seed_agents": seed_agents,
            }),
            next: "swarm doctor".to_string(),
            state: minimal_state_for_request(request).await,
        })
    } else {
        Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INTERNAL.to_string(),
                format!("Init completed with {} errors", errors.len()),
            )
            .with_fix("Review error details and retry failed steps manually".to_string())
            .with_ctx(json!({"errors": errors, "completed_steps": steps})),
        ))
    }
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

const fn json_value_type_name(value: &Value) -> &'static str {
    parsing::json_value_type_name(value)
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
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod database_candidate_tests {
    use super::{
        compose_database_url_candidates, parse_database_connect_timeout_ms,
        DEFAULT_DB_CONNECT_TIMEOUT_MS, MAX_DB_CONNECT_TIMEOUT_MS, MIN_DB_CONNECT_TIMEOUT_MS,
    };

    #[test]
    fn explicit_database_url_is_preferred_and_deduplicated() {
        let candidates = compose_database_url_candidates(
            Some("postgres://explicit/db"),
            vec![
                "postgres://explicit/db".to_string(),
                "postgres://env/db".to_string(),
            ],
        );

        assert_eq!(
            candidates,
            vec![
                "postgres://explicit/db".to_string(),
                "postgres://env/db".to_string(),
            ]
        );
    }

    #[test]
    fn empty_explicit_database_url_is_ignored() {
        let candidates = compose_database_url_candidates(
            Some("   "),
            vec![
                "postgres://env/db".to_string(),
                "postgres://default/db".to_string(),
            ],
        );

        assert_eq!(
            candidates,
            vec![
                "postgres://env/db".to_string(),
                "postgres://default/db".to_string(),
            ]
        );
    }

    #[test]
    fn discovered_candidates_are_preserved_when_no_explicit_url() {
        let candidates = compose_database_url_candidates(
            None,
            vec![
                "postgres://env/db".to_string(),
                "postgres://default/db".to_string(),
            ],
        );

        assert_eq!(
            candidates,
            vec![
                "postgres://env/db".to_string(),
                "postgres://default/db".to_string(),
            ]
        );
    }

    #[test]
    fn parse_connect_timeout_defaults_when_missing_or_invalid() {
        assert_eq!(
            parse_database_connect_timeout_ms(None),
            DEFAULT_DB_CONNECT_TIMEOUT_MS
        );
        assert_eq!(
            parse_database_connect_timeout_ms(Some("")),
            DEFAULT_DB_CONNECT_TIMEOUT_MS
        );
        assert_eq!(
            parse_database_connect_timeout_ms(Some("not-a-number")),
            DEFAULT_DB_CONNECT_TIMEOUT_MS
        );
    }

    #[test]
    fn parse_connect_timeout_enforces_bounds() {
        assert_eq!(
            parse_database_connect_timeout_ms(Some("1")),
            MIN_DB_CONNECT_TIMEOUT_MS
        );
        assert_eq!(
            parse_database_connect_timeout_ms(Some("999999")),
            MAX_DB_CONNECT_TIMEOUT_MS
        );
        assert_eq!(parse_database_connect_timeout_ms(Some("2500")), 2500);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod history_limit_tests {
    use super::{bounded_history_limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT};

    #[test]
    fn history_limit_defaults_when_not_provided() {
        assert_eq!(bounded_history_limit(None), DEFAULT_HISTORY_LIMIT);
    }

    #[test]
    fn history_limit_caps_excessive_values() {
        assert_eq!(bounded_history_limit(Some(50_000)), MAX_HISTORY_LIMIT);
    }

    #[test]
    fn history_limit_preserves_values_within_bounds() {
        assert_eq!(bounded_history_limit(Some(5_000)), 5_000);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod register_input_tests {
    use super::{ParseInput, ProtocolRequest, MAX_REGISTER_COUNT};
    use serde_json::{json, Map, Value};

    fn request_with_count(count: Value) -> ProtocolRequest {
        let mut args = Map::new();
        args.insert("count".to_string(), count);
        ProtocolRequest {
            cmd: "register".to_string(),
            rid: None,
            dry: None,
            args,
        }
    }

    #[test]
    fn register_input_rejects_zero_count() {
        let request = request_with_count(json!(0));
        let err =
            crate::RegisterInput::parse_input(&request).expect_err("count=0 should be rejected");
        assert!(err.to_string().contains("must be greater than 0"));
    }

    #[test]
    fn register_input_rejects_negative_count() {
        let request = request_with_count(json!(-2));
        let err = crate::RegisterInput::parse_input(&request)
            .expect_err("negative count should be rejected");
        assert!(err.to_string().contains("must be greater than 0"));
    }

    #[test]
    fn register_input_accepts_positive_count() {
        let request = request_with_count(json!(2));
        let parsed =
            crate::RegisterInput::parse_input(&request).expect("positive count should be accepted");
        assert_eq!(parsed.count, Some(2));
    }

    #[test]
    fn register_input_rejects_count_above_maximum() {
        let request = request_with_count(json!(MAX_REGISTER_COUNT + 1));
        let err = crate::RegisterInput::parse_input(&request)
            .expect_err("count above max should be rejected");
        assert!(err.to_string().contains("less than or equal to"));
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod init_db_input_tests {
    use super::{ParseInput, ProtocolRequest};
    use serde_json::{json, Map};

    fn request_with_seed_agents(value: serde_json::Value) -> ProtocolRequest {
        let mut args = Map::new();
        args.insert("seed_agents".to_string(), value);
        ProtocolRequest {
            cmd: "init-db".to_string(),
            rid: None,
            dry: None,
            args,
        }
    }

    #[test]
    fn init_db_input_rejects_negative_seed_agents() {
        let request = request_with_seed_agents(json!(-1));
        let err = crate::InitDbInput::parse_input(&request)
            .expect_err("negative seed_agents should be rejected");
        assert!(err.to_string().contains("must be non-negative"));
    }

    #[test]
    fn init_db_input_accepts_zero_seed_agents() {
        let request = request_with_seed_agents(json!(0));
        let parsed =
            crate::InitDbInput::parse_input(&request).expect("zero seed_agents should be accepted");
        assert_eq!(parsed.seed_agents, Some(0));
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod parse_input_tests {
    use super::{ParseInput, ProtocolRequest};
    use serde_json::{json, Map, Value};

    fn make_request(cmd: &str, args: Map<String, Value>) -> ProtocolRequest {
        ProtocolRequest {
            cmd: cmd.to_string(),
            rid: None,
            dry: None,
            args,
        }
    }

    mod agent_input_tests {
        use super::*;

        fn request_with_id(id: Value) -> ProtocolRequest {
            let mut args = Map::new();
            args.insert("id".to_string(), id);
            make_request("agent", args)
        }

        #[test]
        fn agent_input_rejects_missing_id() {
            let request = make_request("agent", Map::new());
            let err = crate::AgentInput::parse_input(&request)
                .expect_err("missing id should be rejected");
            assert!(err.to_string().contains("Missing required field: id"));
        }

        #[test]
        fn agent_input_rejects_zero_id() {
            let request = request_with_id(json!(0));
            let err =
                crate::AgentInput::parse_input(&request).expect_err("id=0 should be rejected");
            assert!(err.to_string().contains("must be greater than 0"));
        }

        #[test]
        fn agent_input_rejects_negative_id() {
            let request = request_with_id(json!(-1));
            let err = crate::AgentInput::parse_input(&request)
                .expect_err("negative id should be rejected");
            assert!(err.to_string().contains("must be greater than 0"));
        }

        #[test]
        fn agent_input_rejects_string_id() {
            let request = request_with_id(json!("not-a-number"));
            let err =
                crate::AgentInput::parse_input(&request).expect_err("string id should be rejected");
            assert!(err.to_string().contains("Invalid type"));
        }

        #[test]
        fn agent_input_rejects_id_exceeding_u32_max() {
            let request = request_with_id(json!(u32::MAX as u64 + 1));
            let err = crate::AgentInput::parse_input(&request)
                .expect_err("id exceeding u32 max should be rejected");
            assert!(err.to_string().contains("exceeds max u32"));
        }

        #[test]
        fn agent_input_accepts_valid_id() {
            let request = request_with_id(json!(5));
            let parsed =
                crate::AgentInput::parse_input(&request).expect("valid id should be accepted");
            assert_eq!(parsed.id, 5);
        }

        #[test]
        fn agent_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("id".to_string(), json!(1));
            args.insert("dry".to_string(), json!(true));
            let request = make_request("agent", args);
            let parsed = crate::AgentInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod lock_input_tests {
        use super::*;

        fn request_with_lock(resource: Value, agent: Value, ttl_ms: Value) -> ProtocolRequest {
            let mut args = Map::new();
            args.insert("resource".to_string(), resource);
            args.insert("agent".to_string(), agent);
            args.insert("ttl_ms".to_string(), ttl_ms);
            make_request("lock", args)
        }

        #[test]
        fn lock_input_rejects_missing_resource() {
            let mut args = Map::new();
            args.insert("agent".to_string(), json!("agent-1"));
            args.insert("ttl_ms".to_string(), json!(30000));
            let request = make_request("lock", args);
            let err = crate::LockInput::parse_input(&request)
                .expect_err("missing resource should be rejected");
            assert!(err.to_string().contains("Missing required field: resource"));
        }

        #[test]
        fn lock_input_rejects_missing_agent() {
            let mut args = Map::new();
            args.insert("resource".to_string(), json!("repo-123"));
            args.insert("ttl_ms".to_string(), json!(30000));
            let request = make_request("lock", args);
            let err = crate::LockInput::parse_input(&request)
                .expect_err("missing agent should be rejected");
            assert!(err.to_string().contains("Missing required field: agent"));
        }

        #[test]
        fn lock_input_rejects_missing_ttl_ms() {
            let mut args = Map::new();
            args.insert("resource".to_string(), json!("repo-123"));
            args.insert("agent".to_string(), json!("agent-1"));
            let request = make_request("lock", args);
            let err = crate::LockInput::parse_input(&request)
                .expect_err("missing ttl_ms should be rejected");
            assert!(err.to_string().contains("Missing required field: ttl_ms"));
        }

        #[test]
        fn lock_input_accepts_all_required_fields() {
            let request = request_with_lock(json!("repo-123"), json!("agent-1"), json!(30000));
            let parsed = crate::LockInput::parse_input(&request)
                .expect("all required fields should be accepted");
            assert_eq!(parsed.resource, "repo-123");
            assert_eq!(parsed.agent, "agent-1");
            assert_eq!(parsed.ttl_ms, 30000);
        }

        #[test]
        fn lock_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("resource".to_string(), json!("repo-123"));
            args.insert("agent".to_string(), json!("agent-1"));
            args.insert("ttl_ms".to_string(), json!(30000));
            args.insert("dry".to_string(), json!(true));
            let request = make_request("lock", args);
            let parsed = crate::LockInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod broadcast_input_tests {
        use super::*;

        fn request_with_broadcast(msg: Value, from: Value) -> ProtocolRequest {
            let mut args = Map::new();
            args.insert("msg".to_string(), msg);
            args.insert("from".to_string(), from);
            make_request("broadcast", args)
        }

        #[test]
        fn broadcast_input_rejects_missing_msg() {
            let mut args = Map::new();
            args.insert("from".to_string(), json!("agent-1"));
            let request = make_request("broadcast", args);
            let err = crate::BroadcastInput::parse_input(&request)
                .expect_err("missing msg should be rejected");
            assert!(err.to_string().contains("Missing required field: msg"));
        }

        #[test]
        fn broadcast_input_rejects_missing_from() {
            let mut args = Map::new();
            args.insert("msg".to_string(), json!("hello world"));
            let request = make_request("broadcast", args);
            let err = crate::BroadcastInput::parse_input(&request)
                .expect_err("missing from should be rejected");
            assert!(err.to_string().contains("Missing required field: from"));
        }

        #[test]
        fn broadcast_input_accepts_all_required_fields() {
            let request = request_with_broadcast(json!("hello"), json!("agent-1"));
            let parsed = crate::BroadcastInput::parse_input(&request)
                .expect("all required fields should be accepted");
            assert_eq!(parsed.msg, "hello");
            assert_eq!(parsed.from, "agent-1");
        }

        #[test]
        fn broadcast_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("msg".to_string(), json!("hello"));
            args.insert("from".to_string(), json!("agent-1"));
            args.insert("dry".to_string(), json!(true));
            let request = make_request("broadcast", args);
            let parsed = crate::BroadcastInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod prompt_input_tests {
        use super::*;

        fn request_with_prompt(id: Option<Value>, skill: Option<Value>) -> ProtocolRequest {
            let mut args = Map::new();
            if let Some(id_val) = id {
                args.insert("id".to_string(), id_val);
            }
            if let Some(skill_val) = skill {
                args.insert("skill".to_string(), skill_val);
            }
            make_request("prompt", args)
        }

        #[test]
        fn prompt_input_defaults_id_to_one() {
            let request = request_with_prompt(None, None);
            let parsed = crate::PromptInput::parse_input(&request)
                .expect("request without id should be accepted");
            assert_eq!(parsed.id, 1);
        }

        #[test]
        fn prompt_input_accepts_custom_id() {
            let request = request_with_prompt(Some(json!(5)), None);
            let parsed = crate::PromptInput::parse_input(&request)
                .expect("request with custom id should be accepted");
            assert_eq!(parsed.id, 5);
        }

        #[test]
        fn prompt_input_rejects_zero_id() {
            let request = request_with_prompt(Some(json!(0)), None);
            let err =
                crate::PromptInput::parse_input(&request).expect_err("id=0 should be rejected");
            assert!(err.to_string().contains("must be greater than 0"));
        }

        #[test]
        fn prompt_input_rejects_negative_id() {
            let request = request_with_prompt(Some(json!(-1)), None);
            let err = crate::PromptInput::parse_input(&request)
                .expect_err("negative id should be rejected");
            assert!(err.to_string().contains("must be greater than 0"));
        }

        #[test]
        fn prompt_input_accepts_skill() {
            let request = request_with_prompt(None, Some(json!("rust")));
            let parsed = crate::PromptInput::parse_input(&request)
                .expect("request with skill should be accepted");
            assert_eq!(parsed.skill, Some("rust".to_string()));
        }

        #[test]
        fn prompt_input_accepts_id_and_skill() {
            let request = request_with_prompt(Some(json!(3)), Some(json!("python")));
            let parsed = crate::PromptInput::parse_input(&request)
                .expect("request with id and skill should be accepted");
            assert_eq!(parsed.id, 3);
            assert_eq!(parsed.skill, Some("python".to_string()));
        }
    }

    mod smoke_input_tests {
        use super::*;

        fn request_with_smoke(id: Option<Value>) -> ProtocolRequest {
            let mut args = Map::new();
            if let Some(id_val) = id {
                args.insert("id".to_string(), id_val);
            }
            make_request("smoke", args)
        }

        #[test]
        fn smoke_input_defaults_id_to_one() {
            let request = request_with_smoke(None);
            let parsed = crate::SmokeInput::parse_input(&request)
                .expect("request without id should be accepted");
            assert_eq!(parsed.id, 1);
        }

        #[test]
        fn smoke_input_accepts_custom_id() {
            let request = request_with_smoke(Some(json!(7)));
            let parsed = crate::SmokeInput::parse_input(&request)
                .expect("request with custom id should be accepted");
            assert_eq!(parsed.id, 7);
        }

        #[test]
        fn smoke_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("id".to_string(), json!(1));
            args.insert("dry".to_string(), json!(true));
            let request = make_request("smoke", args);
            let parsed = crate::SmokeInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod monitor_input_tests {
        use super::*;

        fn request_with_monitor(view: Option<Value>, watch_ms: Option<Value>) -> ProtocolRequest {
            let mut args = Map::new();
            if let Some(view_val) = view {
                args.insert("view".to_string(), view_val);
            }
            if let Some(watch_val) = watch_ms {
                args.insert("watch_ms".to_string(), watch_val);
            }
            make_request("monitor", args)
        }

        #[test]
        fn monitor_input_accepts_view() {
            let request = request_with_monitor(Some(json!("progress")), None);
            let parsed = crate::MonitorInput::parse_input(&request)
                .expect("request with view should be accepted");
            assert_eq!(parsed.view, Some("progress".to_string()));
        }

        #[test]
        fn monitor_input_accepts_watch_ms() {
            let request = request_with_monitor(None, Some(json!(5000)));
            let parsed = crate::MonitorInput::parse_input(&request)
                .expect("request with watch_ms should be accepted");
            assert_eq!(parsed.watch_ms, Some(5000));
        }

        #[test]
        fn monitor_input_accepts_view_and_watch_ms_with_values() {
            let request = request_with_monitor(Some(json!("failures")), Some(json!(10000)));
            let parsed = crate::MonitorInput::parse_input(&request)
                .expect("request with view and watch_ms should be accepted");
            assert_eq!(parsed.view, Some("failures".to_string()));
            assert_eq!(parsed.watch_ms, Some(10000));
        }

        #[test]
        fn monitor_input_rejects_negative_watch_ms() {
            let request = request_with_monitor(None, Some(json!(-1)));
            let err = crate::MonitorInput::parse_input(&request)
                .expect_err("negative watch_ms should be rejected");
            assert!(err.to_string().contains("must be non-negative"));
        }
    }

    mod init_local_db_input_tests {
        use super::*;

        fn request_with_init_local_db(
            container_name: Option<Value>,
            port: Option<Value>,
            user: Option<Value>,
            database: Option<Value>,
            schema: Option<Value>,
            seed_agents: Option<Value>,
        ) -> ProtocolRequest {
            let mut args = Map::new();
            if let Some(v) = container_name {
                args.insert("container_name".to_string(), v);
            }
            if let Some(v) = port {
                args.insert("port".to_string(), v);
            }
            if let Some(v) = user {
                args.insert("user".to_string(), v);
            }
            if let Some(v) = database {
                args.insert("database".to_string(), v);
            }
            if let Some(v) = schema {
                args.insert("schema".to_string(), v);
            }
            if let Some(v) = seed_agents {
                args.insert("seed_agents".to_string(), v);
            }
            make_request("init-local-db", args)
        }

        #[test]
        fn init_local_db_input_accepts_empty_args() {
            let request = make_request("init-local-db", Map::new());
            let parsed = crate::InitLocalDbInput::parse_input(&request)
                .expect("empty args should be accepted");
            assert_eq!(parsed.container_name, None);
            assert_eq!(parsed.port, None);
            assert_eq!(parsed.user, None);
            assert_eq!(parsed.database, None);
            assert_eq!(parsed.schema, None);
            assert_eq!(parsed.seed_agents, None);
        }

        #[test]
        fn init_local_db_input_accepts_all_fields() {
            let request = request_with_init_local_db(
                Some(json!("swarm-db")),
                Some(json!(5437)),
                Some(json!("user")),
                Some(json!("db")),
                Some(json!("public")),
                Some(json!(5)),
            );
            let parsed = crate::InitLocalDbInput::parse_input(&request)
                .expect("all fields should be accepted");
            assert_eq!(parsed.container_name, Some("swarm-db".to_string()));
            assert_eq!(parsed.port, Some(5437));
            assert_eq!(parsed.user, Some("user".to_string()));
            assert_eq!(parsed.database, Some("db".to_string()));
            assert_eq!(parsed.schema, Some("public".to_string()));
            assert_eq!(parsed.seed_agents, Some(5));
        }

        #[test]
        fn init_local_db_input_rejects_port_exceeding_u16_max() {
            let mut args = Map::new();
            args.insert("port".to_string(), json!(u16::MAX as u64 + 1));
            let request = make_request("init-local-db", args);
            let err = crate::InitLocalDbInput::parse_input(&request)
                .expect_err("port exceeding u16 max should be rejected");
            assert!(err.to_string().contains("exceeds max"));
        }

        #[test]
        fn init_local_db_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("dry".to_string(), json!(true));
            let request = make_request("init-local-db", args);
            let parsed = crate::InitLocalDbInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod init_input_tests {
        use super::*;

        fn request_with_init(
            database_url: Option<Value>,
            schema: Option<Value>,
            seed_agents: Option<Value>,
        ) -> ProtocolRequest {
            let mut args = Map::new();
            if let Some(v) = database_url {
                args.insert("database_url".to_string(), v);
            }
            if let Some(v) = schema {
                args.insert("schema".to_string(), v);
            }
            if let Some(v) = seed_agents {
                args.insert("seed_agents".to_string(), v);
            }
            make_request("init", args)
        }

        #[test]
        fn init_input_accepts_empty_args() {
            let request = make_request("init", Map::new());
            let parsed =
                crate::InitInput::parse_input(&request).expect("empty args should be accepted");
            assert_eq!(parsed.database_url, None);
            assert_eq!(parsed.schema, None);
            assert_eq!(parsed.seed_agents, None);
        }

        #[test]
        fn init_input_accepts_database_url() {
            let request = request_with_init(Some(json!("postgres://localhost/test")), None, None);
            let parsed = crate::InitInput::parse_input(&request)
                .expect("request with database_url should be accepted");
            assert_eq!(
                parsed.database_url,
                Some("postgres://localhost/test".to_string())
            );
        }

        #[test]
        fn init_input_accepts_schema() {
            let request = request_with_init(None, Some(json!("custom-schema")), None);
            let parsed = crate::InitInput::parse_input(&request)
                .expect("request with schema should be accepted");
            assert_eq!(parsed.schema, Some("custom-schema".to_string()));
        }

        #[test]
        fn init_input_accepts_seed_agents() {
            let request = request_with_init(None, None, Some(json!(10)));
            let parsed = crate::InitInput::parse_input(&request)
                .expect("request with seed_agents should be accepted");
            assert_eq!(parsed.seed_agents, Some(10));
        }

        #[test]
        fn init_input_accepts_all_fields() {
            let request = request_with_init(
                Some(json!("postgres://localhost/test")),
                Some(json!("public")),
                Some(json!(5)),
            );
            let parsed =
                crate::InitInput::parse_input(&request).expect("all fields should be accepted");
            assert_eq!(
                parsed.database_url,
                Some("postgres://localhost/test".to_string())
            );
            assert_eq!(parsed.schema, Some("public".to_string()));
            assert_eq!(parsed.seed_agents, Some(5));
        }

        #[test]
        fn init_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("dry".to_string(), json!(true));
            let request = make_request("init", args);
            let parsed = crate::InitInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod release_input_tests {
        use super::*;

        fn request_with_release(agent_id: Value) -> ProtocolRequest {
            let mut args = Map::new();
            args.insert("agent_id".to_string(), agent_id);
            make_request("release", args)
        }

        #[test]
        fn release_input_rejects_missing_agent_id() {
            let request = make_request("release", Map::new());
            let err = crate::ReleaseInput::parse_input(&request)
                .expect_err("missing agent_id should be rejected");
            assert!(err.to_string().contains("Missing required field: agent_id"));
        }

        #[test]
        fn release_input_accepts_valid_agent_id() {
            let request = request_with_release(json!(5));
            let parsed = crate::ReleaseInput::parse_input(&request)
                .expect("valid agent_id should be accepted");
            assert_eq!(parsed.agent_id, 5);
        }

        #[test]
        fn release_input_rejects_string_agent_id() {
            let request = request_with_release(json!("not-a-number"));
            let err = crate::ReleaseInput::parse_input(&request)
                .expect_err("string agent_id should be rejected");
            assert!(err.to_string().contains("Missing required field"));
        }

        #[test]
        fn release_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("agent_id".to_string(), json!(1));
            args.insert("dry".to_string(), json!(true));
            let request = make_request("release", args);
            let parsed = crate::ReleaseInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod spawn_prompts_input_tests {
        use super::*;

        fn request_with_spawn_prompts(
            template: Option<Value>,
            out_dir: Option<Value>,
            count: Option<Value>,
        ) -> ProtocolRequest {
            let mut args = Map::new();
            if let Some(v) = template {
                args.insert("template".to_string(), v);
            }
            if let Some(v) = out_dir {
                args.insert("out_dir".to_string(), v);
            }
            if let Some(v) = count {
                args.insert("count".to_string(), v);
            }
            make_request("spawn-prompts", args)
        }

        #[test]
        fn spawn_prompts_input_accepts_empty_args() {
            let request = make_request("spawn-prompts", Map::new());
            let parsed = crate::SpawnPromptsInput::parse_input(&request)
                .expect("empty args should be accepted");
            assert_eq!(parsed.template, None);
            assert_eq!(parsed.out_dir, None);
            assert_eq!(parsed.count, None);
        }

        #[test]
        fn spawn_prompts_input_accepts_template() {
            let request = request_with_spawn_prompts(Some(json!("default")), None, None);
            let parsed = crate::SpawnPromptsInput::parse_input(&request)
                .expect("request with template should be accepted");
            assert_eq!(parsed.template, Some("default".to_string()));
        }

        #[test]
        fn spawn_prompts_input_accepts_out_dir() {
            let request = request_with_spawn_prompts(None, Some(json!("/tmp/output")), None);
            let parsed = crate::SpawnPromptsInput::parse_input(&request)
                .expect("request with out_dir should be accepted");
            assert_eq!(parsed.out_dir, Some("/tmp/output".to_string()));
        }

        #[test]
        fn spawn_prompts_input_accepts_count() {
            let request = request_with_spawn_prompts(None, None, Some(json!(50)));
            let parsed = crate::SpawnPromptsInput::parse_input(&request)
                .expect("request with count should be accepted");
            assert_eq!(parsed.count, Some(50));
        }

        #[test]
        fn spawn_prompts_input_accepts_all_fields() {
            let request = request_with_spawn_prompts(
                Some(json!("custom")),
                Some(json!("/out")),
                Some(json!(100)),
            );
            let parsed = crate::SpawnPromptsInput::parse_input(&request)
                .expect("all fields should be accepted");
            assert_eq!(parsed.template, Some("custom".to_string()));
            assert_eq!(parsed.out_dir, Some("/out".to_string()));
            assert_eq!(parsed.count, Some(100));
        }

        #[test]
        fn spawn_prompts_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("dry".to_string(), json!(true));
            let request = make_request("spawn-prompts", args);
            let parsed = crate::SpawnPromptsInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod batch_input_tests {
        use super::*;

        fn request_with_batch(ops: Value) -> ProtocolRequest {
            let mut args = Map::new();
            args.insert("ops".to_string(), ops);
            make_request("batch", args)
        }

        #[test]
        fn batch_input_rejects_missing_ops() {
            let request = make_request("batch", Map::new());
            let err = crate::BatchInput::parse_input(&request)
                .expect_err("missing ops should be rejected");
            assert!(err.to_string().contains("Missing required field: ops"));
        }

        #[test]
        fn batch_input_accepts_empty_ops_array() {
            let request = request_with_batch(json!([]));
            let parsed = crate::BatchInput::parse_input(&request)
                .expect("empty ops array should be accepted");
            assert!(parsed.ops.is_empty());
        }

        #[test]
        fn batch_input_accepts_ops_with_commands() {
            let request = request_with_batch(json!([{"cmd": "doctor"}, {"cmd": "status"}]));
            let parsed = crate::BatchInput::parse_input(&request)
                .expect("ops with commands should be accepted");
            assert_eq!(parsed.ops.len(), 2);
        }

        #[test]
        fn batch_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("ops".to_string(), json!([]));
            args.insert("dry".to_string(), json!(true));
            let request = make_request("batch", args);
            let parsed = crate::BatchInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod history_input_tests {
        use super::*;

        fn request_with_history(limit: Option<Value>) -> ProtocolRequest {
            let mut args = Map::new();
            if let Some(v) = limit {
                args.insert("limit".to_string(), v);
            }
            make_request("history", args)
        }

        #[test]
        fn history_input_accepts_no_limit() {
            let request = request_with_history(None);
            let parsed =
                crate::HistoryInput::parse_input(&request).expect("no limit should be accepted");
            assert_eq!(parsed.limit, None);
        }

        #[test]
        fn history_input_accepts_limit() {
            let request = request_with_history(Some(json!(100)));
            let parsed =
                crate::HistoryInput::parse_input(&request).expect("limit should be accepted");
            assert_eq!(parsed.limit, Some(100));
        }

        #[test]
        fn history_input_accepts_zero_limit() {
            let request = request_with_history(Some(json!(0)));
            let parsed =
                crate::HistoryInput::parse_input(&request).expect("zero limit should be accepted");
            assert_eq!(parsed.limit, Some(0));
        }

        #[test]
        fn history_input_rejects_negative_limit() {
            let request = request_with_history(Some(json!(-1)));
            let err = crate::HistoryInput::parse_input(&request)
                .expect_err("negative limit should be rejected");
            assert!(err.to_string().contains("must be non-negative"));
        }

        #[test]
        fn history_input_rejects_string_limit() {
            let request = request_with_history(Some(json!("invalid")));
            let err = crate::HistoryInput::parse_input(&request)
                .expect_err("string limit should be rejected");
            assert!(err.to_string().contains("must be non-negative"));
        }
    }

    mod unlock_input_tests {
        use super::*;

        fn request_with_unlock(resource: Value, agent: Value) -> ProtocolRequest {
            let mut args = Map::new();
            args.insert("resource".to_string(), resource);
            args.insert("agent".to_string(), agent);
            make_request("unlock", args)
        }

        #[test]
        fn unlock_input_rejects_missing_resource() {
            let mut args = Map::new();
            args.insert("agent".to_string(), json!("agent-1"));
            let request = make_request("unlock", args);
            let err = crate::UnlockInput::parse_input(&request)
                .expect_err("missing resource should be rejected");
            assert!(err.to_string().contains("Missing required field: resource"));
        }

        #[test]
        fn unlock_input_rejects_missing_agent() {
            let mut args = Map::new();
            args.insert("resource".to_string(), json!("repo-123"));
            let request = make_request("unlock", args);
            let err = crate::UnlockInput::parse_input(&request)
                .expect_err("missing agent should be rejected");
            assert!(err.to_string().contains("Missing required field: agent"));
        }

        #[test]
        fn unlock_input_accepts_all_required_fields() {
            let request = request_with_unlock(json!("repo-123"), json!("agent-1"));
            let parsed = crate::UnlockInput::parse_input(&request)
                .expect("all required fields should be accepted");
            assert_eq!(parsed.resource, "repo-123");
            assert_eq!(parsed.agent, "agent-1");
        }

        #[test]
        fn unlock_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("resource".to_string(), json!("repo-123"));
            args.insert("agent".to_string(), json!("agent-1"));
            args.insert("dry".to_string(), json!(true));
            let request = make_request("unlock", args);
            let parsed = crate::UnlockInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod load_profile_input_tests {
        use super::*;

        fn request_with_load_profile(
            agents: Option<Value>,
            rounds: Option<Value>,
            timeout_ms: Option<Value>,
        ) -> ProtocolRequest {
            let mut args = Map::new();
            if let Some(v) = agents {
                args.insert("agents".to_string(), v);
            }
            if let Some(v) = rounds {
                args.insert("rounds".to_string(), v);
            }
            if let Some(v) = timeout_ms {
                args.insert("timeout_ms".to_string(), v);
            }
            make_request("load-profile", args)
        }

        #[test]
        fn load_profile_input_accepts_empty_args() {
            let request = make_request("load-profile", Map::new());
            let parsed = crate::LoadProfileInput::parse_input(&request)
                .expect("empty args should be accepted");
            assert_eq!(parsed.agents, None);
            assert_eq!(parsed.rounds, None);
            assert_eq!(parsed.timeout_ms, None);
        }

        #[test]
        fn load_profile_input_accepts_agents() {
            let request = request_with_load_profile(Some(json!(10)), None, None);
            let parsed = crate::LoadProfileInput::parse_input(&request)
                .expect("request with agents should be accepted");
            assert_eq!(parsed.agents, Some(10));
        }

        #[test]
        fn load_profile_input_accepts_rounds() {
            let request = request_with_load_profile(None, Some(json!(5)), None);
            let parsed = crate::LoadProfileInput::parse_input(&request)
                .expect("request with rounds should be accepted");
            assert_eq!(parsed.rounds, Some(5));
        }

        #[test]
        fn load_profile_input_accepts_timeout_ms() {
            let request = request_with_load_profile(None, None, Some(json!(60000)));
            let parsed = crate::LoadProfileInput::parse_input(&request)
                .expect("request with timeout_ms should be accepted");
            assert_eq!(parsed.timeout_ms, Some(60000));
        }

        #[test]
        fn load_profile_input_accepts_all_fields() {
            let request =
                request_with_load_profile(Some(json!(10)), Some(json!(5)), Some(json!(60000)));
            let parsed = crate::LoadProfileInput::parse_input(&request)
                .expect("all fields should be accepted");
            assert_eq!(parsed.agents, Some(10));
            assert_eq!(parsed.rounds, Some(5));
            assert_eq!(parsed.timeout_ms, Some(60000));
        }

        #[test]
        fn load_profile_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("dry".to_string(), json!(true));
            let request = make_request("load-profile", args);
            let parsed = crate::LoadProfileInput::parse_input(&request)
                .expect("request with dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod bootstrap_input_tests {
        use super::*;

        #[test]
        fn bootstrap_input_accepts_no_args() {
            let request = make_request("bootstrap", Map::new());
            let parsed =
                crate::BootstrapInput::parse_input(&request).expect("no args should be accepted");
            assert_eq!(parsed.dry, None);
        }

        #[test]
        fn bootstrap_input_accepts_dry_flag() {
            let mut args = Map::new();
            args.insert("dry".to_string(), json!(true));
            let request = make_request("bootstrap", args);
            let parsed =
                crate::BootstrapInput::parse_input(&request).expect("dry flag should be accepted");
            assert_eq!(parsed.dry, Some(true));
        }
    }

    mod doctor_input_tests {
        use super::*;

        #[test]
        fn doctor_input_accepts_no_args() {
            let request = make_request("doctor", Map::new());
            let parsed =
                crate::DoctorInput::parse_input(&request).expect("no args should be accepted");
            assert_eq!(parsed.json, None);
        }

        #[test]
        fn doctor_input_accepts_json_flag() {
            let mut args = Map::new();
            args.insert("json".to_string(), json!(true));
            let request = make_request("doctor", args);
            let parsed =
                crate::DoctorInput::parse_input(&request).expect("json flag should be accepted");
            assert_eq!(parsed.json, Some(true));
        }
    }

    mod help_input_tests {
        use super::*;

        #[test]
        fn help_input_accepts_no_args() {
            let request = make_request("help", Map::new());
            let parsed =
                crate::HelpInput::parse_input(&request).expect("no args should be accepted");
            assert_eq!(parsed.short, None);
            assert_eq!(parsed.s, None);
        }

        #[test]
        fn help_input_accepts_short_flag() {
            let mut args = Map::new();
            args.insert("short".to_string(), json!(true));
            let request = make_request("help", args);
            let parsed =
                crate::HelpInput::parse_input(&request).expect("short flag should be accepted");
            assert_eq!(parsed.short, Some(true));
        }

        #[test]
        fn help_input_accepts_s_flag() {
            let mut args = Map::new();
            args.insert("s".to_string(), json!(true));
            let request = make_request("help", args);
            let parsed =
                crate::HelpInput::parse_input(&request).expect("s flag should be accepted");
            assert_eq!(parsed.s, Some(true));
        }
    }

    mod status_input_tests {
        use super::*;

        #[test]
        fn status_input_accepts_no_args() {
            let request = make_request("status", Map::new());
            let _parsed =
                crate::StatusInput::parse_input(&request).expect("no args should be accepted");
        }
    }

    mod state_input_tests {
        use super::*;

        #[test]
        fn state_input_accepts_no_args() {
            let request = make_request("state", Map::new());
            let _parsed =
                crate::StateInput::parse_input(&request).expect("no args should be accepted");
        }
    }

    mod agents_input_tests {
        use super::*;

        #[test]
        fn agents_input_accepts_no_args() {
            let request = make_request("agents", Map::new());
            let _parsed =
                crate::AgentsInput::parse_input(&request).expect("no args should be accepted");
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod stream_capture_tests {
    use super::capture_stream_limited;
    use tokio::io::{AsyncWriteExt, DuplexStream};

    async fn write_all(mut writer: DuplexStream, bytes: Vec<u8>) -> std::io::Result<()> {
        writer.write_all(&bytes).await?;
        writer.shutdown().await
    }

    #[tokio::test]
    async fn capture_stream_limited_preserves_full_output_under_limit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (writer, reader) = tokio::io::duplex(64);
        let payload = b"hello-stream".to_vec();
        let writer_task = tokio::spawn(write_all(writer, payload.clone()));

        let captured = capture_stream_limited(reader, 1024).await?;
        writer_task.await.map_err(std::io::Error::other)??;

        assert_eq!(captured.bytes, payload);
        assert!(!captured.truncated);
        Ok(())
    }

    #[tokio::test]
    async fn capture_stream_limited_truncates_when_output_exceeds_limit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (writer, reader) = tokio::io::duplex(32);
        let payload = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let writer_task = tokio::spawn(write_all(writer, payload));

        let captured = capture_stream_limited(reader, 10).await?;
        writer_task.await.map_err(std::io::Error::other)??;

        assert_eq!(captured.bytes, b"abcdefghij".to_vec());
        assert!(captured.truncated);
        Ok(())
    }
}

fn mask_database_url(url: &str) -> String {
    db_resolution::mask_database_url(url)
}

async fn current_repo_root() -> std::result::Result<PathBuf, Box<ProtocolEnvelope>> {
    Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, None))
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

fn now_ms() -> i64 {
    helpers::now_ms()
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

fn process_batch_items<'a>(
    items: &'a [std::result::Result<ProtocolRequest, Box<ProtocolEnvelope>>],
    idx: usize,
    acc: BatchAcc,
) -> Pin<Box<dyn Future<Output = BatchAcc> + Send + 'a>> {
    Box::pin(async move {
        match items.get(idx) {
            None => acc,
            Some(result) => match result {
                Ok(sub_request) => {
                    let sub_request_cloned: ProtocolRequest = sub_request.clone();
                    match execute_request_no_batch(sub_request_cloned).await {
                        Ok(success) => {
                            let item = json!({
                                "seq": acc.items.len() + 1,
                                "ev": "item",
                                "ok": true,
                                "d": success.data,
                            });
                            let next_acc = BatchAcc {
                                pass: acc.pass.saturating_add(1),
                                fail: acc.fail,
                                items: acc.items.into_iter().chain(std::iter::once(item)).collect(),
                            };
                            process_batch_items(items, idx + 1, next_acc).await
                        }
                        Err(failure) => {
                            let item = json!({
                                "seq": acc.items.len() + 1,
                                "ev": "item",
                                "ok": false,
                                "err": failure.err,
                            });
                            let next_acc = BatchAcc {
                                pass: acc.pass,
                                fail: acc.fail.saturating_add(1),
                                items: acc.items.into_iter().chain(std::iter::once(item)).collect(),
                            };
                            process_batch_items(items, idx + 1, next_acc).await
                        }
                    }
                }
                Err(failure) => {
                    let item = json!({
                        "seq": acc.items.len() + 1,
                        "ev": "item",
                        "ok": false,
                        "err": failure.err,
                    });
                    let next_acc = BatchAcc {
                        pass: acc.pass,
                        fail: acc.fail.saturating_add(1),
                        items: acc.items.into_iter().chain(std::iter::once(item)).collect(),
                    };
                    process_batch_items(items, idx + 1, next_acc).await
                }
            },
        }
    })
}
