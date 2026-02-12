#![allow(clippy::too_many_lines)]

use super::super::{
    dry_flag, dry_run_success, execute_request_no_batch, minimal_state_for_request, CommandSuccess,
    ProtocolRequest,
};
use crate::code;
use crate::protocol_envelope::ProtocolEnvelope;
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

pub(in crate::protocol_runtime) async fn handle_batch(
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

pub(in crate::protocol_runtime) async fn handle_help(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
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
        state: minimal_state_for_request(request).await,
    })
}

#[derive(Clone, Debug, Default)]
struct BatchAcc {
    pass: i64,
    fail: i64,
    items: Vec<Value>,
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
