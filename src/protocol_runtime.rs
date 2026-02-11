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
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Stdio;
use std::time::{Duration, Instant};
use swarm::protocol_envelope::ProtocolEnvelope;
use swarm::{
    code, AgentId, ArtifactType, BeadId, RepoId, ResumeContextContract, StageArtifact, SwarmDb,
    SwarmError, CANONICAL_COORDINATOR_SCHEMA_PATH,
};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

const EMBEDDED_COORDINATOR_SCHEMA_SQL: &str =
    include_str!("../crates/swarm-coordinator/schema.sql");
const EMBEDDED_COORDINATOR_SCHEMA_REF: &str = "embedded:crates/swarm-coordinator/schema.sql";
const DEFAULT_DB_CONNECT_TIMEOUT_MS: u64 = 3_000;
const MIN_DB_CONNECT_TIMEOUT_MS: u64 = 100;
const MAX_DB_CONNECT_TIMEOUT_MS: u64 = 30_000;
const MAX_EXTERNAL_OUTPUT_CAPTURE_BYTES: usize = 1_048_576;
const GLOBAL_ALLOWED_REQUEST_ARGS: &[&str] = &["repo_id", "database_url", "connect_timeout_ms"];

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct ProtocolRequest {
    pub cmd: String,
    pub rid: Option<String>,
    pub dry: Option<bool>,
    #[serde(flatten)]
    pub args: Map<String, Value>,
}

// ============================================================================
// PARSE INPUT TRAIT
// ============================================================================

pub trait ParseInput {
    type Input;
    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError>;
}

#[derive(Debug, thiserror::Error)]
#[expect(dead_code)]
pub enum ParseError {
    #[error("Missing required field: {field}")]
    MissingField { field: String },

    #[error("Invalid type for field {field}: expected {expected}, got {got}")]
    InvalidType {
        field: String,
        expected: String,
        got: String,
    },

    #[error("Invalid value for field {field}: {value}")]
    InvalidValue { field: String, value: String },

    #[error("Parse error: {0}")]
    Custom(String),
}

// Implement ParseInput for all contract types
impl ParseInput for swarm::DoctorInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            json: request.args.get("json").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::HelpInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            short: request.args.get("short").and_then(|v: &Value| v.as_bool()),
            s: request.args.get("s").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::StatusInput {
    type Input = Self;

    fn parse_input(_request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {})
    }
}

impl ParseInput for swarm::AgentInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let id_raw = request
            .args
            .get("id")
            .ok_or_else(|| ParseError::MissingField {
                field: "id".to_string(),
            })?;

        let id_as_u64 = id_raw.as_u64().ok_or_else(|| ParseError::InvalidType {
            field: "id".to_string(),
            expected: "u32".to_string(),
            got: json_value_type_name(id_raw).to_string(),
        })?;

        let id = u32::try_from(id_as_u64).map_err(|_| ParseError::InvalidValue {
            field: "id".to_string(),
            value: format!("{id_as_u64} exceeds max u32"),
        })?;

        Ok(Self::Input {
            id,
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::InitInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
            database_url: request
                .args
                .get("database_url")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            schema: request
                .args
                .get("schema")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            seed_agents: request
                .args
                .get("seed_agents")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
        })
    }
}

impl ParseInput for swarm::RegisterInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            count: request
                .args
                .get("count")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::ReleaseInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let agent_id = request
            .args
            .get("agent_id")
            .and_then(|v: &Value| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| ParseError::MissingField {
                field: "agent_id".to_string(),
            })?;

        Ok(Self::Input {
            agent_id,
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::MonitorInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            view: request
                .args
                .get("view")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            watch_ms: request
                .args
                .get("watch_ms")
                .and_then(|v: &Value| v.as_u64()),
        })
    }
}

impl ParseInput for swarm::InitDbInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            url: request
                .args
                .get("url")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            schema: request
                .args
                .get("schema")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            seed_agents: request
                .args
                .get("seed_agents")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::InitLocalDbInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            container_name: request
                .args
                .get("container_name")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            port: request
                .args
                .get("port")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u16::try_from(v).ok()),
            user: request
                .args
                .get("user")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            database: request
                .args
                .get("database")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            schema: request
                .args
                .get("schema")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            seed_agents: request
                .args
                .get("seed_agents")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::BootstrapInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::SpawnPromptsInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            template: request
                .args
                .get("template")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            out_dir: request
                .args
                .get("out_dir")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
            count: request
                .args
                .get("count")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::PromptInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            id: request
                .args
                .get("id")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(1),
            skill: request
                .args
                .get("skill")
                .and_then(|v: &Value| v.as_str())
                .map(std::string::ToString::to_string),
        })
    }
}

impl ParseInput for swarm::SmokeInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            id: request
                .args
                .get("id")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(1),
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::BatchInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let ops = request
            .args
            .get("ops")
            .and_then(|v: &Value| v.as_array())
            .ok_or_else(|| ParseError::MissingField {
                field: "ops".to_string(),
            })?
            .clone();

        Ok(Self::Input {
            ops,
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::StateInput {
    type Input = Self;

    fn parse_input(_request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {})
    }
}

impl ParseInput for swarm::HistoryInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            limit: request.args.get("limit").and_then(|v: &Value| v.as_i64()),
        })
    }
}

impl ParseInput for swarm::LockInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let resource = request
            .args
            .get("resource")
            .and_then(|v: &Value| v.as_str())
            .ok_or_else(|| ParseError::MissingField {
                field: "resource".to_string(),
            })?
            .to_string();

        let agent = request
            .args
            .get("agent")
            .and_then(|v: &Value| v.as_str())
            .ok_or_else(|| ParseError::MissingField {
                field: "agent".to_string(),
            })?
            .to_string();

        let ttl_ms = request
            .args
            .get("ttl_ms")
            .and_then(|v: &Value| v.as_i64())
            .ok_or_else(|| ParseError::MissingField {
                field: "ttl_ms".to_string(),
            })?;

        Ok(Self::Input {
            resource,
            agent,
            ttl_ms,
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::UnlockInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let resource = request
            .args
            .get("resource")
            .and_then(|v: &Value| v.as_str())
            .ok_or_else(|| ParseError::MissingField {
                field: "resource".to_string(),
            })?
            .to_string();

        let agent = request
            .args
            .get("agent")
            .and_then(|v: &Value| v.as_str())
            .ok_or_else(|| ParseError::MissingField {
                field: "agent".to_string(),
            })?
            .to_string();

        Ok(Self::Input {
            resource,
            agent,
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::AgentsInput {
    type Input = Self;

    fn parse_input(_request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {})
    }
}

impl ParseInput for swarm::BroadcastInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let msg = request
            .args
            .get("msg")
            .and_then(|v: &Value| v.as_str())
            .ok_or_else(|| ParseError::MissingField {
                field: "msg".to_string(),
            })?
            .to_string();

        let from = request
            .args
            .get("from")
            .and_then(|v: &Value| v.as_str())
            .ok_or_else(|| ParseError::MissingField {
                field: "from".to_string(),
            })?
            .to_string();

        Ok(Self::Input {
            msg,
            from,
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

impl ParseInput for swarm::LoadProfileInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self::Input {
            agents: request
                .args
                .get("agents")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            rounds: request
                .args
                .get("rounds")
                .and_then(|v: &Value| v.as_u64())
                .and_then(|v| u32::try_from(v).ok()),
            timeout_ms: request
                .args
                .get("timeout_ms")
                .and_then(|v: &Value| v.as_u64()),
            dry: request.args.get("dry").and_then(|v: &Value| v.as_bool()),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct BatchAcc {
    pass: i64,
    fail: i64,
    items: Vec<Value>,
}

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
    match request.cmd.as_str() {
        "batch" => handle_batch(&request).await,
        _ => execute_request_no_batch(request).await,
    }
}

async fn execute_request_no_batch(
    request: ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    validate_request_args(&request)?;

    match request.cmd.as_str() {
        "?" | "help" => handle_help(&request).await,
        "state" => handle_state(&request).await,
        "history" => handle_history(&request).await,
        "lock" => handle_lock(&request).await,
        "unlock" => handle_unlock(&request).await,
        "agents" => handle_agents(&request).await,
        "broadcast" => handle_broadcast(&request).await,
        "monitor" => handle_monitor(&request).await,
        "register" => handle_register(&request).await,
        "agent" => handle_agent(&request).await,
        "status" => handle_status(&request).await,
        "next" => handle_next(&request).await,
        "claim-next" => handle_claim_next(&request).await,
        "assign" => handle_assign(&request).await,
        "run-once" => handle_run_once(&request).await,
        "qa" => handle_qa(&request).await,
        "resume" => handle_resume(&request).await,
        "resume-context" => handle_resume_context(&request).await,
        "artifacts" => handle_artifacts(&request).await,
        "release" => handle_release(&request).await,
        "init-db" => handle_init_db(&request).await,
        "init-local-db" => handle_init_local_db(&request).await,
        "spawn-prompts" => handle_spawn_prompts(&request).await,
        "smoke" => handle_smoke(&request).await,
        "prompt" => handle_prompt(&request).await,
        "doctor" => handle_doctor(&request).await,
        "load-profile" => handle_load_profile(&request).await,
        "bootstrap" => handle_bootstrap(&request).await,
        "init" => handle_init(&request).await,
        other => Err(Box::new(ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("Unknown command: {other}"),
            ).with_fix("Use a valid command: init, doctor, status, next, claim-next, assign, run-once, qa, resume, artifacts, resume-context, agent, smoke, prompt, register, release, monitor, init-db, init-local-db, spawn-prompts, batch, bootstrap, state, or ?/help for help".to_string())
            .with_ctx(json!({"cmd": other})))),
    }
}

fn allowed_command_args(cmd: &str) -> Option<&'static [&'static str]> {
    match cmd {
        "?" | "help" => Some(&["short", "s"]),
        "state" | "history" => Some(&["limit"]),
        "doctor" | "status" | "resume" | "agents" => Some(&[]),
        "lock" => Some(&["resource", "agent", "ttl_ms", "dry"]),
        "unlock" => Some(&["resource", "agent", "dry"]),
        "broadcast" => Some(&["msg", "from", "dry"]),
        "monitor" => Some(&["view", "watch_ms"]),
        "register" => Some(&["count", "dry"]),
        "agent" | "run-once" | "smoke" => Some(&["id", "dry"]),
        "next" | "claim-next" | "bootstrap" => Some(&["dry"]),
        "assign" => Some(&["bead_id", "agent_id", "dry"]),
        "qa" => Some(&["target", "id", "dry"]),
        "resume-context" => Some(&["bead_id"]),
        "artifacts" => Some(&["bead_id", "artifact_type"]),
        "release" => Some(&["agent_id", "dry"]),
        "init-db" => Some(&["url", "schema", "seed_agents", "dry"]),
        "init-local-db" => Some(&[
            "container_name",
            "port",
            "user",
            "database",
            "schema",
            "seed_agents",
            "dry",
        ]),
        "spawn-prompts" => Some(&["template", "out_dir", "count", "dry"]),
        "prompt" => Some(&["id", "skill"]),
        "load-profile" => Some(&["agents", "rounds", "timeout_ms", "dry"]),
        "init" => Some(&["dry", "database_url", "schema", "seed_agents"]),
        "batch" => Some(&["ops", "cmds", "dry"]),
        _ => None,
    }
}

fn validate_request_args(
    request: &ProtocolRequest,
) -> std::result::Result<(), Box<ProtocolEnvelope>> {
    let Some(allowed_command_specific) = allowed_command_args(request.cmd.as_str()) else {
        return Ok(());
    };
    let unknown = request
        .args
        .keys()
        .filter(|key| {
            !allowed_command_specific.contains(&key.as_str())
                && !GLOBAL_ALLOWED_REQUEST_ARGS.contains(&key.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();

    if unknown.is_empty() {
        return Ok(());
    }

    let mut allowed = allowed_command_specific
        .iter()
        .map(|key| (*key).to_string())
        .collect::<Vec<_>>();
    allowed.extend(
        GLOBAL_ALLOWED_REQUEST_ARGS
            .iter()
            .map(|key| (*key).to_string()),
    );
    allowed.sort();
    allowed.dedup();

    Err(Box::new(
        ProtocolEnvelope::error(
            request.rid.clone(),
            code::INVALID.to_string(),
            format!(
                "Unknown field(s) for {}: {}",
                request.cmd,
                unknown.join(", ")
            ),
        )
        .with_fix("Remove unknown fields or use documented command arguments".to_string())
        .with_ctx(json!({"cmd": request.cmd, "unknown": unknown, "allowed": allowed})),
    ))
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
    let total_start = Instant::now();
    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "bv_robot_next", "target": "bv --robot-next"}),
                json!({"step": 2, "action": "br_update", "target": "br update <bead-id> --status in_progress --json"}),
            ],
            "swarm status",
        ));
    }

    let (recommendation_payload, bv_robot_next_ms) = run_external_json_command_with_ms(
        "bv",
        &["--robot-next"],
        request.rid.clone(),
        "Run `bv --robot-next` manually and verify beads index is available",
    )
    .await?;
    let recommendation = project_next_recommendation(&recommendation_payload);
    let bead_id = bead_id_from_recommendation(&recommendation).ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "bv --robot-next returned no bead id".to_string(),
            )
            .with_fix("Run `bv --robot-next` and verify it returns an object with id".to_string())
            .with_ctx(json!({"next": recommendation})),
        )
    })?;

    let (claim, br_update_ms) = run_external_json_command_with_ms(
        "br",
        &[
            "update",
            bead_id.as_str(),
            "--status",
            "in_progress",
            "--json",
        ],
        request.rid.clone(),
        "Run `br update <bead-id> --status in_progress --json` manually",
    )
    .await?;

    Ok(CommandSuccess {
        data: json!({
            "selection": recommendation,
            "claim": claim,
            "timing": {
                "external": {
                    "bv_robot_next_ms": bv_robot_next_ms,
                    "br_update_ms": br_update_ms,
                },
                "total_ms": elapsed_ms(total_start),
            }
        }),
        next: format!("br show {bead_id}"),
        state: minimal_state_for_request(request).await,
    })
}

fn first_issue_from_br_payload(payload: &Value) -> Option<&Value> {
    if payload.is_object() {
        return Some(payload);
    }

    payload.as_array().and_then(|items| items.first())
}

fn issue_status_from_br_payload(payload: &Value) -> Option<String> {
    first_issue_from_br_payload(payload)
        .and_then(|issue| issue.get("status"))
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
}

fn issue_id_from_br_payload(payload: &Value) -> Option<String> {
    first_issue_from_br_payload(payload)
        .and_then(|issue| issue.get("id"))
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
}

async fn valid_agent_ids(db: &SwarmDb, repo_id: &RepoId) -> Vec<u32> {
    db.get_available_agents(repo_id)
        .await
        .map(|agents| {
            agents
                .into_iter()
                .map(|agent| agent.agent_id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

async fn handle_assign(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);

    let bead_id = request
        .args
        .get("bead_id")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing required field: bead_id".to_string(),
                )
                .with_fix(
                    "echo '{\"cmd\":\"assign\",\"bead_id\":\"<bead-id>\",\"agent_id\":1}' | swarm"
                        .to_string(),
                )
                .with_ctx(json!({"bead_id": "required"})),
            )
        })?;

    let valid_ids = valid_agent_ids(&db, &repo_id).await;
    let agent_id = request
        .args
        .get("agent_id")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing required field: agent_id".to_string(),
                )
                .with_fix(
                    "echo '{\"cmd\":\"assign\",\"bead_id\":\"<bead-id>\",\"agent_id\":1}' | swarm"
                        .to_string(),
                )
                .with_ctx(json!({"agent_id": "required", "valid_ids": valid_ids})),
            )
        })?;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "br_show", "target": format!("br show {bead_id} --json")}),
                json!({"step": 2, "action": "claim_bead", "target": format!("agent:{agent_id}, bead:{bead_id}")}),
                json!({"step": 3, "action": "br_update", "target": format!("br update {bead_id} --status in_progress --assignee swarm-agent-{agent_id} --json")}),
                json!({"step": 4, "action": "br_verify", "target": format!("br show {bead_id} --json")}),
            ],
            "swarm monitor --view active",
        ));
    }

    let agent_key = AgentId::new(repo_id.clone(), agent_id);
    let state = db
        .get_agent_state(&agent_key)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    let Some(agent_state) = state else {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::NOTFOUND.to_string(),
                format!("Agent {agent_id} is not registered"),
            )
            .with_fix("swarm register --count <n>".to_string())
            .with_ctx(json!({"agent_id": agent_id, "valid_ids": valid_ids})),
        ));
    };

    if agent_state.status.as_str() != "idle" || agent_state.bead_id.is_some() {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::CONFLICT.to_string(),
                format!("Agent {agent_id} is not idle"),
            )
            .with_fix(
                "Choose an idle agent from `swarm monitor --view active` or `swarm state`"
                    .to_string(),
            )
            .with_ctx(json!({
                "agent_id": agent_id,
                "agent_status": agent_state.status.as_str(),
                "current_bead": agent_state.bead_id.map(|b| b.value().to_string()),
            })),
        ));
    }

    let bead_before = run_external_json_command(
        "br",
        &["show", bead_id.as_str(), "--json"],
        request.rid.clone(),
        "Run `br show <bead-id> --json` and verify bead exists",
    )
    .await?;

    let current_status = issue_status_from_br_payload(&bead_before).ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "br show returned payload without status".to_string(),
            )
            .with_fix("Run `br show <bead-id> --json` and inspect response shape".to_string())
            .with_ctx(json!({"payload": bead_before})),
        )
    })?;

    if current_status != "open" {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::CONFLICT.to_string(),
                format!("Bead {bead_id} is not assignable: status={current_status}"),
            )
            .with_fix("Use an open bead id from `br ready --json`".to_string())
            .with_ctx(json!({"bead_id": bead_id, "status": current_status})),
        ));
    }

    let claimed = db
        .claim_bead(&agent_key, &BeadId::new(bead_id.clone()))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    if !claimed {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::CONFLICT.to_string(),
                format!("Failed to claim bead {bead_id} for agent {agent_id}"),
            )
            .with_fix("Verify bead is not already claimed and retry".to_string())
            .with_ctx(json!({"bead_id": bead_id, "agent_id": agent_id})),
        ));
    }

    let assignee = format!("swarm-agent-{agent_id}");
    let update_result = run_external_json_command(
        "br",
        &[
            "update",
            bead_id.as_str(),
            "--status",
            "in_progress",
            "--assignee",
            assignee.as_str(),
            "--json",
        ],
        request.rid.clone(),
        "Run `br update <bead-id> --status in_progress --assignee swarm-agent-<id> --json` manually",
    )
    .await;

    let br_update = match update_result {
        Ok(value) => value,
        Err(err) => {
            let _ = db.release_agent(&agent_key).await;
            return Err(Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::CONFLICT.to_string(),
                    format!(
                        "assign failed during br update and was rolled back for bead {bead_id}"
                    ),
                )
                .with_fix(
                    "Retry once br command succeeds. Local claim was reverted to avoid drift"
                        .to_string(),
                )
                .with_ctx(json!({"bead_id": bead_id, "agent_id": agent_id, "br_error": err.err})),
            ));
        }
    };

    let bead_after = run_external_json_command(
        "br",
        &["show", bead_id.as_str(), "--json"],
        request.rid.clone(),
        "Run `br show <bead-id> --json` and verify bead status",
    )
    .await?;

    let verified_status = issue_status_from_br_payload(&bead_after);
    let verified_id = issue_id_from_br_payload(&bead_after);

    Ok(CommandSuccess {
        data: json!({
            "bead_id": bead_id,
            "agent_id": agent_id,
            "assignee": assignee,
            "swarm_claim": {
                "claimed": true,
                "agent_status": "working",
            },
            "br_sync": {
                "update": br_update,
                "verify": bead_after,
                "verified_status": verified_status,
                "verified_id": verified_id,
            },
            "synced": true,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }),
        next: "swarm monitor --view active".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_run_once(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let total_start = Instant::now();
    let agent_id = request
        .args
        .get("id")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .map_or(1_u32, |value| value);

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "doctor"}),
                json!({"step": 2, "action": "status"}),
                json!({"step": 3, "action": "claim_next"}),
                json!({"step": 4, "action": "agent", "target": agent_id}),
                json!({"step": 5, "action": "monitor", "target": "progress"}),
            ],
            "swarm status",
        ));
    }

    let doctor_start = Instant::now();
    let doctor = handle_doctor(request).await?.data;
    let doctor_ms = elapsed_ms(doctor_start);
    let status_before_start = Instant::now();
    let status_before = handle_status(request).await?.data;
    let status_before_ms = elapsed_ms(status_before_start);
    let claim_start = Instant::now();
    let claim = handle_claim_next(request).await?.data;
    let claim_ms = elapsed_ms(claim_start);

    let agent_request = ProtocolRequest {
        cmd: "agent".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: Map::from_iter(vec![("id".to_string(), Value::from(agent_id))]),
    };
    let agent_start = Instant::now();
    let agent = handle_agent(&agent_request).await?.data;
    let agent_ms = elapsed_ms(agent_start);

    let progress_request = ProtocolRequest {
        cmd: "monitor".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: Map::from_iter(vec![(
            "view".to_string(),
            Value::String("progress".to_string()),
        )]),
    };
    let progress_start = Instant::now();
    let progress = handle_monitor(&progress_request).await?.data;
    let progress_ms = elapsed_ms(progress_start);

    Ok(CommandSuccess {
        data: json!({
            "agent_id": agent_id,
            "steps": {
                "doctor": doctor,
                "status_before": status_before,
                "claim_next": claim,
                "agent": agent,
                "progress": progress,
            },
            "timing": {
                "steps_ms": {
                    "doctor": doctor_ms,
                    "status_before": status_before_ms,
                    "claim_next": claim_ms,
                    "agent": agent_ms,
                    "progress": progress_ms,
                },
                "total_ms": elapsed_ms(total_start),
            }
        }),
        next: "swarm monitor --view failures".to_string(),
        state: minimal_state_for_request(request).await,
    })
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
    let limit = request
        .args
        .get("limit")
        .and_then(Value::as_i64)
        .map_or(100, |value| value);
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
    let from = required_string_arg(request, "from")?;

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
    let view = request
        .args
        .get("view")
        .and_then(Value::as_str)
        .map_or("active", |value| value);
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
                .map(|message: swarm::AgentMessage| {
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
    let db: SwarmDb = db_from_request(request).await?;
    let repo_id_from_context = repo_id_from_request(request);
    let config = db.get_config(&repo_id_from_context).await.ok();

    let count = request
        .args
        .get("count")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .or_else(|| config.as_ref().map(|c| c.max_agents))
        .unwrap_or(10);

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

    if let Some(explicit_count) = request.args.get("count").and_then(Value::as_u64) {
        let _ = db.update_config(explicit_count as u32).await;
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
    let input = swarm::AgentInput::parse_input(request).map_err(|e| {
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

    let config = load_config(None, false)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
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
    let bead_filter = request
        .args
        .get("bead_id")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);

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

async fn handle_artifacts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let bead_id = parse_artifact_bead_id(request)?;
    let artifact_type = parse_artifact_type(request)?;
    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let artifacts = db
        .get_bead_artifacts(&repo_id, &bead_id, artifact_type)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    let artifact_payload = artifacts.iter().map(artifact_to_json).collect::<Vec<_>>();

    Ok(CommandSuccess {
        data: json!({
            "bead_id": bead_id.value(),
            "artifact_count": artifact_payload.len(),
            "artifacts": artifact_payload,
        }),
        next: "swarm monitor --view progress".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn parse_artifact_bead_id(
    request: &ProtocolRequest,
) -> std::result::Result<BeadId, Box<ProtocolEnvelope>> {
    request
        .args
        .get("bead_id")
        .and_then(Value::as_str)
        .map(BeadId::new)
        .ok_or_else(||
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing required field: bead_id".to_string(),
                )
                .with_fix("Include `bead_id` in the request. Example: {\"cmd\":\"artifacts\",\"bead_id\":\"<bead>\"}".to_string())
                .with_ctx(json!({"bead_id": "required"})),
            ),
        )
}

fn parse_artifact_type(
    request: &ProtocolRequest,
) -> std::result::Result<Option<ArtifactType>, Box<ProtocolEnvelope>> {
    let Some(raw_artifact_type) = request.args.get("artifact_type") else {
        return Ok(None);
    };

    let Some(raw_artifact_type) = raw_artifact_type.as_str() else {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "artifact_type must be a string".to_string(),
            )
            .with_fix(format!(
                "Use artifact_type from: {}",
                ArtifactType::names().join(", ")
            ))
            .with_ctx(json!({"artifact_type": request.args.get("artifact_type")})),
        ));
    };

    let candidate = raw_artifact_type.trim();
    if candidate.is_empty() {
        return Ok(None);
    }

    ArtifactType::try_from(candidate).map(Some).map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(request.rid.clone(), code::INVALID.to_string(), err)
                .with_fix(format!(
                    "Use artifact_type from: {}",
                    ArtifactType::names().join(", ")
                ))
                .with_ctx(json!({"artifact_type": candidate})),
        )
    })
}

fn artifact_to_json(artifact: &StageArtifact) -> Value {
    json!({
        "id": artifact.id,
        "stage_history_id": artifact.stage_history_id,
        "artifact_type": artifact.artifact_type.as_str(),
        "content": artifact.content.clone(),
        "metadata": artifact.metadata.clone(),
        "created_at": artifact.created_at.to_rfc3339(),
        "content_hash": artifact.content_hash.clone(),
    })
}

#[cfg(test)]
mod artifact_tests {
    use super::*;
    use serde_json::{map::Map, Value};

    fn request_with_args(entries: &[(&str, &str)]) -> ProtocolRequest {
        let args = entries
            .iter()
            .map(|(key, value)| (key.to_string(), Value::String(value.to_string())))
            .collect::<Map<_, _>>();
        ProtocolRequest {
            cmd: "artifacts".to_string(),
            rid: None,
            dry: None,
            args,
        }
    }

    #[test]
    fn parse_artifact_bead_id_returns_value() {
        let request = request_with_args(&[("bead_id", "bead-42")]);
        let bead_id = parse_artifact_bead_id(&request).expect("should parse bead_id");
        assert_eq!(bead_id.value(), "bead-42");
    }

    #[test]
    fn parse_artifact_bead_id_errors_when_missing() {
        let request = request_with_args(&[]);
        let err = parse_artifact_bead_id(&request).expect_err("bead_id missing");
        let envelope: &ProtocolEnvelope = err.as_ref();
        assert_eq!(envelope.err.as_ref().unwrap().code, "INVALID");
        assert!(envelope.fix.as_ref().unwrap().contains("bead_id"));
    }

    #[test]
    fn parse_artifact_type_returns_none_by_default() {
        let request = request_with_args(&[("bead_id", "bead-42")]);
        let artifact_type = parse_artifact_type(&request).expect("should parse optional type");
        assert!(artifact_type.is_none());
    }

    #[test]
    fn parse_artifact_type_accepts_known_value() {
        let request =
            request_with_args(&[("bead_id", "bead-42"), ("artifact_type", "test_output")]);
        let artifact_type = parse_artifact_type(&request).expect("valid type");
        assert_eq!(artifact_type, Some(ArtifactType::TestOutput));
    }

    #[test]
    fn parse_artifact_type_rejects_unknown_value() {
        let request =
            request_with_args(&[("bead_id", "bead-42"), ("artifact_type", "unknown-type")]);
        let err = parse_artifact_type(&request).expect_err("unexpected type");
        let envelope: &ProtocolEnvelope = err.as_ref();
        assert_eq!(envelope.err.as_ref().unwrap().code, "INVALID");
        assert!(envelope.fix.as_ref().unwrap().contains("artifact_type"));
    }

    #[test]
    fn parse_artifact_type_rejects_non_string_value() {
        let mut request = request_with_args(&[("bead_id", "bead-42")]);
        request
            .args
            .insert("artifact_type".to_string(), Value::Bool(true));
        let err = parse_artifact_type(&request).expect_err("non-string artifact_type");
        let envelope: &ProtocolEnvelope = err.as_ref();
        assert_eq!(envelope.err.as_ref().unwrap().code, "INVALID");
        assert_eq!(
            envelope.err.as_ref().map(|e| e.msg.as_str()),
            Some("artifact_type must be a string")
        );
    }
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
    let url = resolve_database_url_for_init(request).await?;
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
                json!({"step": 1, "action": "connect_db", "target": mask_database_url(&url)}),
                json!({"step": 2, "action": "apply_schema", "target": schema.clone().unwrap_or_else(|| EMBEDDED_COORDINATOR_SCHEMA_REF.to_string())}),
                json!({"step": 3, "action": "seed_agents", "target": seed_agents}),
            ],
            "swarm state",
        ));
    }

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
    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let config = db.get_config(&repo_id).await.ok();

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
            let template_path = swarm::prompts::canonical_agent_prompt_path(&repo_root);
            let text = swarm::prompts::load_agent_prompt_template(&repo_root)
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
        .or_else(|| config.as_ref().map(|c| c.max_agents))
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

    fs::create_dir_all(out_dir)
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    spawn_prompts_recursive(out_dir, &template_text, 1, count, request.rid.clone()).await?;

    Ok(CommandSuccess {
        data: json!({"count": count, "out_dir": out_dir, "template": template_name}),
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
    if let Some(skill_name) = request.args.get("skill").and_then(Value::as_str) {
        if let Some(prompt) = swarm::skill_prompts::get_skill_prompt(skill_name) {
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

    let id = request
        .args
        .get("id")
        .and_then(Value::as_u64)
        .map_or(1, |value| value) as u32;

    let repo_root = current_repo_root().await?;
    let prompt = swarm::prompts::get_agent_prompt(&repo_root, id)
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
    db.enqueue_backlog_batch("load", agents.saturating_mul(rounds))
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
    request
        .args
        .get(key)
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
        .ok_or_else(|| {
            Box::new(ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("Missing required field: {key}"),
            )
            .with_fix(format!("Add '{key}' field to request. Example: echo '{{\"cmd\":\"agent\",\"{key}\":<value>}}' | swarm"))
            .with_ctx(json!({key: "required"})))
        })
}

async fn db_from_request(
    request: &ProtocolRequest,
) -> std::result::Result<SwarmDb, Box<ProtocolEnvelope>> {
    let explicit_database_url = request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let candidates =
        compose_database_url_candidates(explicit_database_url, database_url_candidates_for_cli());
    let timeout_ms = request_connect_timeout_ms(request)?;
    connect_using_candidates(candidates, timeout_ms, request.rid.clone()).await
}

async fn resolve_database_url_for_init(
    request: &ProtocolRequest,
) -> std::result::Result<String, Box<ProtocolEnvelope>> {
    if let Some(url) = request
        .args
        .get("url")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
    {
        return Ok(url);
    }

    if let Some(url) = request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
    {
        return Ok(url);
    }

    let candidates = database_url_candidates_for_cli();
    let timeout_ms = request_connect_timeout_ms(request)?;
    let (connected, failures) = try_connect_candidates(&candidates, timeout_ms).await;
    if let Some((_db, connected_url)) = connected {
        return Ok(connected_url);
    }

    let masked: Vec<String> = candidates
        .iter()
        .map(|candidate| mask_database_url(candidate))
        .collect();

    Err(Box::new(
        ProtocolEnvelope::error(
            request.rid.clone(),
            code::INTERNAL.to_string(),
            "Unable to resolve a reachable database URL for init-db".to_string(),
        )
        .with_fix("Pass --url <database_url> or run 'swarm init-local-db'".to_string())
        .with_ctx(json!({"tried": masked, "errors": failures})),
    ))
}

async fn connect_using_candidates(
    candidates: Vec<String>,
    timeout_ms: u64,
    rid: Option<String>,
) -> std::result::Result<SwarmDb, Box<ProtocolEnvelope>> {
    let (connected, failures) = try_connect_candidates(&candidates, timeout_ms).await;
    if let Some((db, _connected_url)) = connected {
        return Ok(db);
    }

    let masked: Vec<String> = candidates
        .iter()
        .map(|candidate| mask_database_url(candidate))
        .collect();

    Err(Box::new(
        ProtocolEnvelope::error(
            rid,
            code::INTERNAL.to_string(),
            "Unable to connect to any configured database URL".to_string(),
        )
        .with_fix(
            "Set DATABASE_URL, verify postgres is reachable, or run 'swarm init-local-db'"
                .to_string(),
        )
        .with_ctx(json!({"tried": masked, "errors": failures})),
    ))
}

async fn try_connect_candidates(
    candidates: &[String],
    timeout_ms: u64,
) -> (Option<(SwarmDb, String)>, Vec<String>) {
    let mut failures = Vec::new();

    for candidate in candidates {
        match SwarmDb::new_with_timeout(candidate, Some(timeout_ms)).await {
            Ok(db) => return (Some((db, candidate.clone())), failures),
            Err(err) => failures.push(format!("{}: {}", mask_database_url(candidate), err)),
        }
    }

    (None, failures)
}

fn database_connect_timeout_ms() -> u64 {
    parse_database_connect_timeout_ms(std::env::var("SWARM_DB_CONNECT_TIMEOUT_MS").ok().as_deref())
}

fn parse_database_connect_timeout_ms(raw: Option<&str>) -> u64 {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(DEFAULT_DB_CONNECT_TIMEOUT_MS, |value| {
            value.clamp(MIN_DB_CONNECT_TIMEOUT_MS, MAX_DB_CONNECT_TIMEOUT_MS)
        })
}

fn parse_connect_timeout_value(raw: &Value) -> std::result::Result<u64, ParseError> {
    let Some(value) = raw.as_u64() else {
        return Err(ParseError::InvalidType {
            field: "connect_timeout_ms".to_string(),
            expected: "u64".to_string(),
            got: json_value_type_name(raw).to_string(),
        });
    };

    Ok(value.clamp(MIN_DB_CONNECT_TIMEOUT_MS, MAX_DB_CONNECT_TIMEOUT_MS))
}

fn request_connect_timeout_ms(
    request: &ProtocolRequest,
) -> std::result::Result<u64, Box<ProtocolEnvelope>> {
    request
        .args
        .get("connect_timeout_ms")
        .map(parse_connect_timeout_value)
        .transpose()
        .map(|maybe| maybe.unwrap_or_else(database_connect_timeout_ms))
        .map_err(|error| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    error.to_string(),
                )
                .with_fix("Use connect_timeout_ms as an integer between 100 and 30000".to_string())
                .with_ctx(json!({"error": error.to_string()})),
            )
        })
}

const fn json_value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

async fn minimal_state_for_request(request: &ProtocolRequest) -> Value {
    let repo_id = repo_id_from_request(request);
    match db_from_request(request).await {
        Ok(db) => match db.get_progress(&repo_id).await {
            Ok(progress) => minimal_state_from_progress(&progress),
            Err(_) => json!({"total": 0, "active": 0}),
        },
        Err(_) => json!({"total": 0, "active": 0}),
    }
}

fn minimal_state_from_progress(progress: &swarm::ProgressSummary) -> Value {
    json!({
        "total": progress.total_agents,
        "active": progress.working + progress.waiting + progress.errors,
    })
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
    match url::Url::parse(url) {
        Ok(mut parsed) => {
            if parsed.password().is_some() {
                let _ = parsed.set_password(Some("********"));
            }
            parsed.to_string()
        }
        Err(_) => "<invalid-database-url>".to_string(),
    }
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
    Box::new(
        ProtocolEnvelope::error(rid, error.code().to_string(), error.to_string())
            .with_fix("Check error details and retry with corrected parameters".to_string())
            .with_ctx(json!({"error": error.to_string()})),
    )
}

fn parse_rid(raw: &str) -> Option<String> {
    serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| value.get("rid").and_then(Value::as_str).map(str::to_string))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn dry_flag(request: &ProtocolRequest) -> bool {
    request.dry.is_some_and(|value| value)
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
    request
        .args
        .get("repo_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(RepoId::new)
        .or_else(RepoId::from_current_dir)
        .unwrap_or_else(|| RepoId::new("local"))
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
