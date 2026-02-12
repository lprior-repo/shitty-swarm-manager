#![allow(clippy::too_many_lines)]

use super::super::{
    bounded_history_limit, db_from_request, minimal_state_for_request, minimal_state_from_progress,
    repo_id_from_request, CommandSuccess, ParseInput, ProtocolRequest,
};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, HistoryInput, SwarmError};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub(in crate::protocol_runtime) async fn handle_state(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db = db_from_request(request).await?;
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
            |(repo, agent_id, bead_id, status): (crate::RepoId, u32, Option<String>, String)| {
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

pub(in crate::protocol_runtime) async fn handle_history(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = HistoryInput::parse_input(request).map_err(|error| {
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
    let db = db_from_request(request).await?;
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

pub(in crate::protocol_runtime) async fn handle_agents(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db = db_from_request(request).await?;
    let agents = db
        .list_active_resource_locks()
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?
        .into_iter()
        .map(|(resource, id, since, _): (String, String, i64, i64)| {
            json!({"id": id, "resource": resource, "since": since})
        })
        .collect::<Vec<_>>();

    Ok(CommandSuccess {
        data: json!({"agents": agents}),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn to_protocol_failure(error: SwarmError, rid: Option<String>) -> Box<ProtocolEnvelope> {
    super::super::helpers::to_protocol_failure(error, rid)
}

fn now_ms() -> i64 {
    super::super::helpers::now_ms()
}
