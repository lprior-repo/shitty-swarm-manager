#![allow(clippy::too_many_lines)]

use super::super::{
    db_from_request, dry_flag, dry_run_success, minimal_state_for_request, required_string_arg,
    CommandSuccess, ProtocolRequest,
};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, SwarmError};
use serde_json::json;

pub(in crate::protocol_runtime) async fn handle_broadcast(
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

    let db = db_from_request(request).await?;
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

fn to_protocol_failure(error: SwarmError, rid: Option<String>) -> Box<ProtocolEnvelope> {
    super::super::helpers::to_protocol_failure(error, rid)
}
