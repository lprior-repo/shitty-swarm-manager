use super::super::super::{
    dry_flag, dry_run_success, elapsed_ms, minimal_state_for_request, CommandSuccess,
    ProtocolRequest,
};
use super::adapter::ProtocolCommandAdapter;
use crate::orchestrator_service::RunOnceAppService;
use crate::protocol_envelope::ProtocolEnvelope;
use serde_json::{json, Value};
use std::time::Instant;

pub(in crate::protocol_runtime) async fn handle_run_once(
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

    let adapter = ProtocolCommandAdapter::new(request);
    let service = RunOnceAppService::new(adapter);
    let result = service
        .execute(agent_id)
        .await
        .map_err(|error| super::super::super::to_protocol_failure(error, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({
            "agent_id": result.agent_id,
            "steps": {
                "doctor": result.doctor,
                "status_before": result.status_before,
                "claim_next": result.claim_next,
                "agent": result.agent,
                "progress": result.progress,
            },
            "timing": {
                "steps_ms": {
                    "doctor": result.doctor_ms,
                    "status_before": result.status_before_ms,
                    "claim_next": result.claim_next_ms,
                    "agent": result.agent_ms,
                    "progress": result.progress_ms,
                },
                "total_ms": elapsed_ms(total_start),
            }
        }),
        next: "swarm monitor --view failures".to_string(),
        state: minimal_state_for_request(request).await,
    })
}
