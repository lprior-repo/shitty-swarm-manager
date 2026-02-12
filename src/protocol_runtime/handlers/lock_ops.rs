#![allow(clippy::too_many_lines)]

use super::super::{
    db_from_request, dry_flag, dry_run_success, minimal_state_for_request, required_string_arg,
    CommandSuccess, ProtocolRequest,
};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, SwarmError};
use serde_json::json;

pub(in crate::protocol_runtime) async fn handle_lock(
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
        .and_then(serde_json::Value::as_i64)
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

    let db = db_from_request(request).await?;
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

pub(in crate::protocol_runtime) async fn handle_unlock(
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

    let db = db_from_request(request).await?;
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

fn to_protocol_failure(error: SwarmError, rid: Option<String>) -> Box<ProtocolEnvelope> {
    super::super::helpers::to_protocol_failure(error, rid)
}
