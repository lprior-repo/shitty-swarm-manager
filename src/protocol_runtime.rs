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
use crate::config::{database_url_candidates_for_cli, default_database_url_for_cli, load_config};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Instant;
use swarm::protocol_envelope::ProtocolEnvelope;
use swarm::{
    code,
    AgentId,
    CANONICAL_COORDINATOR_SCHEMA_PATH,
    RepoId,
    ResumeContextContract,
    SwarmDb,
    SwarmError,
};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

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
        let id = request
            .args
            .get("id")
            .and_then(|v: &Value| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| ParseError::MissingField {
                field: "id".to_string(),
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
    let lines = stdin.lines();
    run_protocol_loop_recursive(lines).await
}

fn run_protocol_loop_recursive(
    mut lines: tokio::io::Lines<BufReader<tokio::io::Stdin>>,
) -> Pin<Box<dyn Future<Output = std::result::Result<(), SwarmError>> + Send>> {
    Box::pin(async move {
        match lines.next_line().await.map_err(SwarmError::IoError)? {
            Some(line) if !line.trim().is_empty() => {
                process_protocol_line(&line).await?;
                run_protocol_loop_recursive(lines).await
            }
            Some(_) => run_protocol_loop_recursive(lines).await,
            None => Ok(()),
        }
    })
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

    let _ = audit_request(
        &audit_cmd,
        maybe_rid.as_deref(),
        audit_args,
        envelope.ok,
        started.elapsed().as_millis() as u64,
        envelope.err.as_ref().map(|e| e.code.as_str()),
    )
    .await;
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
        "resume" => handle_resume(&request).await,
        "resume-context" => handle_resume_context(&request).await,
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
            ).with_fix("Use a valid command: init, doctor, status, resume, resume-context, agent, smoke, prompt, register, release, monitor, init-db, init-local-db, spawn-prompts, batch, bootstrap, state, or ?/help for help".to_string())
            .with_ctx(json!({"cmd": other})))),
    }
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
        ("resume", "Show resumable context projections"),
        ("resume-context", "Show deep resume context payload"),
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
        }),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(_request).await,
    })
}

async fn handle_state(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db: SwarmDb = db_from_request(request).await?;
    let progress = db
        .get_progress(&RepoId::new("local"))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    let resources = db
        .get_all_active_agents()
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

    let config = match db.get_config(&RepoId::new("local")).await {
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
            "resources": resources,
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
    let ops = request
        .args
        .get("ops")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Box::new(ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "Missing ops array".to_string(),
            )
            .with_fix("Add 'ops' array to batch request. Example: echo '{\"cmd\":\"batch\",\"ops\":[\"cmd\":\"doctor\"}]}' | swarm".to_string())
            .with_ctx(json!({"ops": "required"})))
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
            let rows = db
                .get_all_active_agents()
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?
                .into_iter()
                .map(|(repo, agent_id, bead_id, status): (RepoId, u32, Option<String>, String)| {
                    json!({"repo": repo.value(), "agent_id": agent_id, "bead_id": bead_id, "status": status})
                })
                .collect::<Vec<_>>();
            json!({"view": "active", "rows": rows})
        }
        "progress" => {
            let progress = db
                .get_progress(&RepoId::new("local"))
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
            json!({
                "view": "progress",
                "total": progress.total_agents,
                "working": progress.working,
                "idle": progress.idle,
                "waiting": progress.waiting,
                "done": progress.completed,
                "errors": progress.errors,
            })
        }
        "failures" => {
            let rows = db
                .get_execution_events(None, 200)
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
            let rows = db
                .get_execution_events(bead_filter, 200)
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
    let config = db.get_config(&RepoId::new("local")).await.ok();

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

    if input.dry.unwrap_or(false) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "run_agent", "target": input.id})],
            "swarm status",
        ));
    }

    let config = load_config(None, false)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    let db: SwarmDb = SwarmDb::new(&config.database_url)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
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
    let db: SwarmDb = db_from_request(request).await?;
    let progress = db
        .get_progress(&RepoId::new("local"))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    Ok(CommandSuccess {
        data: json!({
            "working": progress.working,
            "idle": progress.idle,
            "waiting": progress.waiting,
            "done": progress.completed,
            "errors": progress.errors,
            "total": progress.total_agents,
        }),
        next: "swarm monitor --view progress".to_string(),
        state: minimal_state_from_progress(&progress),
    })
}

async fn handle_resume(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db: SwarmDb = db_from_request(request).await?;
    let contexts = db
        .get_resume_context_projections()
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
    let db: SwarmDb = db_from_request(request).await?;
    let contexts = db
        .get_deep_resume_contexts()
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({
            "contexts": contexts,
        }),
        next: "swarm monitor --view failures".to_string(),
        state: minimal_state_for_request(request).await,
    })
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
    let released = db
        .release_agent(&AgentId::new(RepoId::new("local"), agent_id))
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
    let url = match request
        .args
        .get("url")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
    {
        Some(value) => value,
        None => default_database_url_for_cli(),
    };
    let schema = match request
        .args
        .get("schema")
        .and_then(Value::as_str)
        .map(PathBuf::from)
    {
        Some(value) => value,
        None => PathBuf::from("crates/swarm-coordinator/schema.sql"),
    };
    let seed_agents = request
        .args
        .get("seed_agents")
        .and_then(Value::as_u64)
        .map_or(10, |value| value) as u32;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "connect_db", "target": url.clone()}),
                json!({"step": 2, "action": "apply_schema", "target": schema.display().to_string()}),
                json!({"step": 3, "action": "seed_agents", "target": seed_agents}),
            ],
            "swarm state",
        ));
    }

    let schema_sql = fs::read_to_string(&schema).await.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("Failed to read schema: {err}"),
            )
            .with_fix("swarm init-db --schema crates/swarm-coordinator/schema.sql".to_string())
            .with_ctx(json!({"schema": schema.display().to_string()})),
        )
    })?;
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
        data: json!({"database_url": url, "schema": schema.display().to_string(), "seed_agents": seed_agents}),
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
    let schema = match request
        .args
        .get("schema")
        .and_then(Value::as_str)
        .map(PathBuf::from)
    {
        Some(value) => value,
        None => PathBuf::from("crates/swarm-coordinator/schema.sql"),
    };
    let seed_agents = request
        .args
        .get("seed_agents")
        .and_then(Value::as_u64)
        .map_or(10, |value| value) as u32;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "docker_start_or_run", "target": container_name.clone()}),
                json!({"step": 2, "action": "init_db", "target": schema.display().to_string()}),
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

    let init_request = ProtocolRequest {
        cmd: "init-db".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: Map::from_iter(vec![
            ("url".to_string(), Value::String(url.clone())),
            (
                "schema".to_string(),
                Value::String(schema.display().to_string()),
            ),
            ("seed_agents".to_string(), Value::from(seed_agents)),
        ]),
    };
    let _ = handle_init_db(&init_request).await?;

    Ok(CommandSuccess {
        data: json!({"container": container_name, "database_url": url, "seed_agents": seed_agents}),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_spawn_prompts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db: SwarmDb = db_from_request(request).await?;
    let config = db.get_config(&RepoId::new("local")).await.ok();

    let (template_text, template_name) = match request.args.get("template").and_then(Value::as_str)
    {
        Some(path) => {
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
        }
        None => (
            swarm::prompts::AGENT_PROMPT_TEMPLATE.to_string(),
            "embedded_template".to_string(),
        ),
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

    let prompt = swarm::prompts::get_agent_prompt(id);

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
    run_smoke_once(&db, &AgentId::new(RepoId::new("local"), id))
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
    let mut checks = vec![
        check_command("moon").await,
        check_command("br").await,
        check_command("jj").await,
        check_command("zjj").await,
        check_command("psql").await,
    ];
    checks.push(check_database_connectivity().await);
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
            "c": check_results
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
    db.seed_idle_agents(agents)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    db.enqueue_backlog_batch("load", agents.saturating_mul(rounds))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    let stats =
        load_profile_recursive(&db, 0, rounds, agents, timeout_ms, LoadStats::default()).await?;

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
                db.claim_next_bead(&AgentId::new(RepoId::new("local"), agent_num)),
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
        .map_or(10, |value| value) as u32;
    let db_url = match request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
    {
        Some(value) => value,
        None => default_database_url_for_cli(),
    };
    let schema = match request
        .args
        .get("schema")
        .and_then(Value::as_str)
        .map(PathBuf::from)
    {
        Some(value) => value,
        None => PathBuf::from("crates/swarm-coordinator/schema.sql"),
    };

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "bootstrap", "target": "repository"}),
                json!({"step": 2, "action": "init_db", "target": db_url.clone()}),
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
        args: Map::from_iter(vec![
            ("url".to_string(), Value::String(db_url.clone())),
            (
                "schema".to_string(),
                Value::String(schema.display().to_string()),
            ),
            ("seed_agents".to_string(), Value::from(seed_agents)),
        ]),
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
                "database_url": db_url,
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
    let Some(database_url) = request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
    else {
        let candidates = database_url_candidates_for_cli();
        return connect_using_candidates(candidates, request.rid.clone()).await;
    };

    SwarmDb::new(&database_url)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))
}

async fn connect_using_candidates(
    candidates: Vec<String>,
    rid: Option<String>,
) -> std::result::Result<SwarmDb, Box<ProtocolEnvelope>> {
    let (connected, failures) = try_connect_candidates(&candidates).await;
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

async fn try_connect_candidates(candidates: &[String]) -> (Option<(SwarmDb, String)>, Vec<String>) {
    let mut failures = Vec::new();

    for candidate in candidates {
        match SwarmDb::new(candidate).await {
            Ok(db) => return (Some((db, candidate.clone())), failures),
            Err(err) => failures.push(format!("{}: {}", mask_database_url(candidate), err)),
        }
    }

    (None, failures)
}

async fn minimal_state_for_request(request: &ProtocolRequest) -> Value {
    match db_from_request(request).await {
        Ok(db) => match db.get_progress(&RepoId::new("local")).await {
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

async fn check_database_connectivity() -> Value {
    let candidates = database_url_candidates_for_cli();
    let (connected, failures) = try_connect_candidates(&candidates).await;

    match connected {
        Some((_db, connected_url)) => {
            json!({"name": "database", "ok": true, "url": mask_database_url(&connected_url)})
        }
        None => json!({
            "name": "database",
            "ok": false,
            "fix": "Set DATABASE_URL, verify postgres is reachable, or run 'swarm init-local-db'",
            "errors": failures,
        }),
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
) -> std::result::Result<(), SwarmError> {
    let db: SwarmDb = SwarmDb::new(&default_database_url_for_cli()).await?;
    db.record_command_audit(cmd, rid, args, ok, ms, error_code)
        .await
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
