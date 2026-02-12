use super::super::super::{
    bead_id_from_recommendation, dry_flag, dry_run_success, elapsed_ms, minimal_state_for_request,
    CommandSuccess, ProtocolRequest,
};
use super::adapter::ProtocolCommandAdapter;
use crate::code;
use crate::orchestrator_service::ClaimNextAppService;
use crate::protocol_envelope::ProtocolEnvelope;
use serde_json::json;
use std::time::Instant;

pub(in crate::protocol_runtime) async fn handle_claim_next(
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

    let adapter = ProtocolCommandAdapter::new(request);
    let service = ClaimNextAppService::new(adapter);
    let result = service
        .execute(bead_id_from_recommendation)
        .await
        .map_err(|error| {
            if error
                .to_string()
                .contains("missing bead id in recommendation")
            {
                return Box::new(
                    ProtocolEnvelope::error(
                        request.rid.clone(),
                        code::INVALID.to_string(),
                        "bv --robot-next returned no bead id".to_string(),
                    )
                    .with_fix(
                        "Run `bv --robot-next` and verify it returns an object with id".to_string(),
                    ),
                );
            }
            super::super::super::to_protocol_failure(error, request.rid.clone())
        })?;

    Ok(CommandSuccess {
        data: json!({
            "selection": result.recommendation,
            "claim": result.claim,
            "timing": {
                "external": {
                    "bv_robot_next_ms": result.bv_robot_next_ms,
                    "br_update_ms": result.br_update_ms,
                },
                "total_ms": elapsed_ms(total_start),
            }
        }),
        next: format!("br show {}", result.bead_id),
        state: minimal_state_for_request(request).await,
    })
}
