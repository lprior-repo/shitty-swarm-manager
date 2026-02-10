use crate::agent_runtime::{run_agent, run_smoke_once};
use crate::config::{default_database_url_for_cli, load_config};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Instant;
use swarm::protocol_envelope::ProtocolEnvelope;
use swarm::{code, AgentId, RepoId, SwarmDb, SwarmError, ERROR_CODES};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

#[derive(Debug, Clone, Deserialize)]
pub struct ProtocolRequest {
    pub cmd: String,
    pub rid: Option<String>,
    pub dry: Option<bool>,
    #[serde(flatten)]
    pub args: Map<String, Value>,
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

async fn process_protocol_line(line: &str) -> std::result::Result<(), SwarmError> {
    let mut stdout = tokio::io::stdout();
    let started = Instant::now();
    let maybe_rid = parse_rid(line);
    let parsed = serde_json::from_str::<ProtocolRequest>(line).map_err(|err| {
        ProtocolEnvelope::error(
            maybe_rid.clone(),
            code::INVALID.to_string(),
            format!("Invalid request JSON: {}", err),
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
                env.with_ms(started.elapsed().as_millis() as i64),
                command_name,
                command_args,
            )
        }
        Err(env) => (
            env.with_ms(started.elapsed().as_millis() as i64),
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
        "?" => handle_help(&request).await,
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
        "release" => handle_release(&request).await,
        "init-db" => handle_init_db(&request).await,
        "init-local-db" => handle_init_local_db(&request).await,
        "spawn-prompts" => handle_spawn_prompts(&request).await,
        "smoke" => handle_smoke(&request).await,
        "doctor" => handle_doctor(&request).await,
        "load-profile" => handle_load_profile(&request).await,
        "bootstrap" => handle_bootstrap(&request).await,
            other => Err(Box::new(ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("Unknown command: {}", other),
            ).with_fix("Use a valid command: doctor, status, agent, smoke, register, release, monitor, init-db, init-local-db, spawn-prompts, batch, bootstrap, or ? for help".to_string())
            .with_ctx(json!({"cmd": other})))),
    }
}

struct CommandSuccess {
    data: Value,
    next: String,
    state: Value,
}
async fn handle_help(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let commands = BTreeMap::from([
        ("?", "Return API metadata"),
        ("state", "Return full coordinator state"),
        ("history", "Return persisted command history"),
        ("batch", "Execute multiple commands with partial success"),
        ("lock", "Acquire resource lock"),
        ("unlock", "Release resource lock"),
        ("agents", "List active lock holders"),
        ("broadcast", "Broadcast coordination message"),
        ("monitor", "Read monitor view"),
        ("register", "Register repository agents"),
        ("agent", "Run single agent pipeline"),
        ("status", "Read progress summary"),
        ("release", "Release current agent claim"),
        ("init-db", "Initialize database schema"),
        ("init-local-db", "Start local Postgres and init schema"),
        ("spawn-prompts", "Generate per-agent prompts"),
        ("smoke", "Run single-agent smoke"),
        ("doctor", "Run environment diagnostics"),
        ("load-profile", "Run synthetic load profile"),
        ("bootstrap", "Idempotent repository bootstrap"),
    ]);

    Ok(CommandSuccess {
        data: json!({
            "name": "swarm",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Deterministic multi-agent orchestration protocol CLI",
            "commands": commands,
            "errors": standard_errors(),
        }),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
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
            next: format!("swarm unlock --resource {} --agent {}", resource, agent),
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
                            format!("Invalid batch item {}: {}", idx, err),
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
                .get_feedback_required()
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?
                .into_iter()
                .map(
                    |(bead_id, agent_id, stage, attempt, feedback, completed_at): (
                        String,
                        u32,
                        String,
                        u32,
                        Option<String>,
                        Option<String>,
                    )| {
                        json!({
                            "bead_id": bead_id,
                            "agent_id": agent_id,
                            "stage": stage,
                            "attempt": attempt,
                            "feedback": feedback,
                            "completed_at": completed_at,
                        })
                    },
                )
                .collect::<Vec<_>>();
            json!({"view": "failures", "rows": rows})
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
    let count = request
        .args
        .get("count")
        .and_then(Value::as_u64)
        .map_or(12, |value| value) as u32;
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

    let db: SwarmDb = db_from_request(request).await?;
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

    let register_results = futures_util::future::join_all((1..=count).map(|idx| {
        let db = db.clone();
        let repo_id = repo_id.clone();
        let rid = request.rid.clone();
        async move {
            db.register_agent(&AgentId::new(repo_id, idx))
                .await
                .map_err(|e| to_protocol_failure(e, rid))
        }
    }))
    .await;

    // Verify all registrations succeeded functionally
    register_results
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(CommandSuccess {
        data: json!({"repo": repo_id.value(), "count": count}),
        next: "swarm status".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

async fn handle_agent(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let id = request
        .args
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing id".to_string(),
                )
                .with_fix("echo '{\"cmd\":\"agent\",\"id\":1}' | swarm".to_string())
                .with_ctx(json!({"id": "required"})),
            )
        })? as u32;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "run_agent", "target": id})],
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
    run_agent(&db, &AgentId::new(repo_id, id), &config.stage_commands)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"agent_id": id, "status": "completed"}),
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
        .map(|value| value.to_string())
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
        .map_or(12, |value| value) as u32;

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
                format!("Failed to read schema: {}", err),
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
    db.seed_idle_agents(seed_agents)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"database_url": url, "schema": schema.display().to_string(), "seed_agents": seed_agents}),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

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
    let password = request
        .args
        .get("password")
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
        .map_or(12, |value| value) as u32;

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

    let port_mapping = format!("{}:5432", port);
    let _ = Command::new("docker")
        .args(["start", container_name.as_str()])
        .output()
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    let _ = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            container_name.as_str(),
            "-p",
            port_mapping.as_str(),
            "-e",
            format!("POSTGRES_USER={}", user).as_str(),
            "-e",
            format!("POSTGRES_PASSWORD={}", password).as_str(),
            "-e",
            format!("POSTGRES_DB={}", database).as_str(),
            "postgres:16",
        ])
        .output()
        .await
        .map_err(SwarmError::IoError);

    let url = format!(
        "postgresql://{}:{}@localhost:{}/{}",
        user, password, port, database
    );
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
    let template = request
        .args
        .get("template")
        .and_then(Value::as_str)
        .map_or(".agents/agent_prompt.md", |value| value);
    let out_dir = request
        .args
        .get("out_dir")
        .and_then(Value::as_str)
        .map_or(".agents/generated", |value| value);
    let count = request
        .args
        .get("count")
        .and_then(Value::as_u64)
        .map_or(12, |value| value) as u32;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "read_template", "target": template}),
                json!({"step": 2, "action": "write_prompts", "target": count}),
            ],
            "swarm monitor --view progress",
        ));
    }

    let template_text = fs::read_to_string(template).await.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::NOTFOUND.to_string(),
                format!("Template not found: {}", err),
            )
            .with_fix("swarm spawn-prompts --template .agents/agent_prompt.md".to_string())
            .with_ctx(json!({"template": template})),
        )
    })?;
    fs::create_dir_all(out_dir)
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    for idx in 1..=count {
        let file = format!("{}/agent_{:02}.md", out_dir, idx);
        fs::write(file, template_text.replace("{N}", &idx.to_string()))
            .await
            .map_err(SwarmError::IoError)
            .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    }

    Ok(CommandSuccess {
        data: json!({"count": count, "out_dir": out_dir}),
        next: "swarm monitor --view active".to_string(),
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
    let checks = vec![
        check_command("moon").await,
        check_command("br").await,
        check_command("jj").await,
        check_command("zjj").await,
        check_command("psql").await,
    ];
    let failed = checks
        .iter()
        .filter(|check| !check["ok"].as_bool().is_some_and(|value| value))
        .count() as i64;
    let passed = checks.len() as i64 - failed;

    Ok(CommandSuccess {
        data: json!({
            "version": "v1",
            "healthy": failed == 0,
            "dry_run": dry_flag(request),
            "checks": checks,
            "summary": {"passed": passed, "failed": failed, "warn": 0},
            "next_actions": if failed == 0 { vec!["System ready".to_string()] } else { vec!["Install missing commands from checks".to_string()] },
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

    #[derive(Default)]
    struct LoadStats {
        success: u64,
        empty: u64,
        timeout: u64,
        error: u64,
    }

    let iterations = (0..rounds).flat_map(|_| 1..=agents);
    let stats = futures_util::stream::iter(iterations)
        .fold(LoadStats::default(), |mut acc, agent_num| {
            let db = db.clone();
            let timeout_dur = tokio::time::Duration::from_millis(timeout_ms);
            async move {
                let claim = tokio::time::timeout(
                    timeout_dur,
                    db.claim_next_bead(&AgentId::new(RepoId::new("local"), agent_num)),
                )
                .await;

                match claim {
                    Ok(Ok(Some(_))) => acc.success = acc.success.saturating_add(1),
                    Ok(Ok(None)) => acc.empty = acc.empty.saturating_add(1),
                    Ok(Err(_)) => acc.error = acc.error.saturating_add(1),
                    Err(_) => acc.timeout = acc.timeout.saturating_add(1),
                };
                acc
            }
        })
        .await;

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
            "database_url = \"postgresql://shitty_swarm_manager:shitty_swarm_manager@localhost:5432/shitty_swarm_manager_db\"\nrust_contract_cmd = \"br show {bead_id}\"\nimplement_cmd = \"jj status\"\nqa_enforcer_cmd = \"moon run :quick\"\nred_queen_cmd = \"moon run :test\"\n",
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

fn required_string_arg(
    request: &ProtocolRequest,
    key: &str,
) -> std::result::Result<String, Box<ProtocolEnvelope>> {
    request
        .args
        .get(key)
        .and_then(Value::as_str)
        .map(|value| value.to_string())
        .ok_or_else(|| {
            Box::new(ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("Missing required field: {}", key),
            )
            .with_fix(format!("Add '{}' field to request. Example: echo '{{\"cmd\":\"agent\",\"{}\":<value>}}' | swarm", key, key))
            .with_ctx(json!({key: "required"})))
        })
}

async fn db_from_request(
    request: &ProtocolRequest,
) -> std::result::Result<SwarmDb, Box<ProtocolEnvelope>> {
    let database_url = match request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(|value| value.to_string())
    {
        Some(value) => value,
        None => default_database_url_for_cli(),
    };
    SwarmDb::new(&database_url)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))
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
        .arg(format!("command -v {}", command))
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

fn standard_errors() -> Value {
    let mut errors = serde_json::Map::new();
    for (code, desc, fix) in ERROR_CODES {
        errors.insert(code.to_string(), json!({"desc": desc, "fix": fix}));
    }
    Value::Object(errors)
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
