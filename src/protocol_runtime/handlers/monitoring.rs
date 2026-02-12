use super::super::{
    db_from_request, minimal_state_for_request, minimal_state_from_progress, repo_id_from_request,
    run_external_json_command_with_ms, to_protocol_failure, CommandSuccess, ParseInput,
    ProtocolRequest,
};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, RepoId, SwarmDb};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Instant;

pub(in crate::protocol_runtime) async fn handle_monitor(
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
            ));
        }
    };

    Ok(CommandSuccess {
        data,
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

pub(in crate::protocol_runtime) async fn handle_status(
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

fn elapsed_ms(start: Instant) -> u64 {
    let ms = start.elapsed().as_millis();
    u64::try_from(ms).map_or(u64::MAX, |value| value)
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
