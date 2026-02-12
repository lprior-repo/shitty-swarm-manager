#![allow(clippy::too_many_lines)]

use super::super::{
    dry_flag, dry_run_success, elapsed_ms, handle_agent, handle_doctor, handle_monitor,
    handle_status, minimal_state_for_request, run_external_json_command_with_ms, CommandSuccess,
    ProtocolRequest,
};
use super::state_ops::handle_state;
use crate::code;
use crate::protocol_envelope::ProtocolEnvelope;
use serde_json::{json, Map, Value};
use std::time::Instant;

pub(in crate::protocol_runtime) async fn handle_next(
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

#[allow(clippy::too_many_lines)]
pub(in crate::protocol_runtime) async fn handle_qa(
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
