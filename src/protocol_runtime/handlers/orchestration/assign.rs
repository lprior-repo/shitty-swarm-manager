use super::super::super::{
    dry_flag, dry_run_success, minimal_state_for_request, repo_id_from_request, CommandSuccess,
    ProtocolRequest,
};
use super::adapter::ProtocolCommandAdapter;
use super::helpers::{issue_id_from_br_payload, issue_status_from_br_payload};
use crate::orchestrator_service::{AssignAppService, AssignCommand};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, RuntimeRepoId, SwarmError};
use serde_json::{json, Value};

pub(in crate::protocol_runtime) async fn handle_assign(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let bead_id = request
        .args
        .get("bead_id")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing required field: bead_id".to_string(),
                )
                .with_fix(
                    "echo '{\"cmd\":\"assign\",\"bead_id\":\"<bead-id>\",\"agent_id\":1}' | swarm"
                        .to_string(),
                )
                .with_ctx(json!({"bead_id": "required"})),
            )
        })?;

    let agent_id = request
        .args
        .get("agent_id")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing required field: agent_id".to_string(),
                )
                .with_fix(
                    "echo '{\"cmd\":\"assign\",\"bead_id\":\"<bead-id>\",\"agent_id\":1}' | swarm"
                        .to_string(),
                )
                .with_ctx(json!({"agent_id": "required"})),
            )
        })?;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "br_show", "target": format!("br show {bead_id} --json")}),
                json!({"step": 2, "action": "claim_bead", "target": format!("agent:{agent_id}, bead:{bead_id}")}),
                json!({"step": 3, "action": "br_update", "target": format!("br update {bead_id} --status in_progress --assignee swarm-agent-{agent_id} --json")}),
                json!({"step": 4, "action": "br_verify", "target": format!("br show {bead_id} --json")}),
            ],
            "swarm monitor --view active",
        ));
    }

    let repo_id = repo_id_from_request(request);
    let command = AssignCommand {
        repo_id: RuntimeRepoId::new(repo_id.value()),
        bead_id: bead_id.clone(),
        agent_id,
    };
    let adapter = ProtocolCommandAdapter::new(request);
    let service = AssignAppService::new(adapter);
    let result = service
        .execute(
            command,
            issue_status_from_br_payload,
            issue_id_from_br_payload,
        )
        .await
        .map_err(|error| map_assign_error(request, &bead_id, agent_id, error))?;

    Ok(CommandSuccess {
        data: json!({
            "bead_id": result.bead_id,
            "agent_id": result.agent_id,
            "assignee": result.assignee,
            "swarm_claim": {
                "claimed": true,
                "agent_status": "working",
            },
            "br_sync": {
                "update": result.br_update,
                "verify": result.bead_verify,
                "verified_status": result.verified_status,
                "verified_id": result.verified_id,
            },
            "synced": true,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        }),
        next: "swarm monitor --view active".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn map_assign_error(
    request: &ProtocolRequest,
    bead_id: &str,
    agent_id: u32,
    error: SwarmError,
) -> Box<ProtocolEnvelope> {
    match error {
        SwarmError::BeadError(message) => Box::new(
            ProtocolEnvelope::error(request.rid.clone(), code::NOTFOUND.to_string(), message)
                .with_fix("swarm register --count <n>".to_string())
                .with_ctx(json!({"agent_id": agent_id})),
        ),
        SwarmError::AgentError(message) => Box::new(
            ProtocolEnvelope::error(request.rid.clone(), code::CONFLICT.to_string(), message)
                .with_fix(
                    "Choose an idle agent from `swarm monitor --view active` or `swarm state`"
                        .to_string(),
                )
                .with_ctx(json!({"agent_id": agent_id})),
        ),
        SwarmError::StageError(message) => Box::new(
            ProtocolEnvelope::error(request.rid.clone(), code::CONFLICT.to_string(), message)
                .with_fix("Use an open and unclaimed bead from `br ready --json`".to_string())
                .with_ctx(json!({"bead_id": bead_id, "agent_id": agent_id})),
        ),
        SwarmError::ConfigError(message) => Box::new(
            ProtocolEnvelope::error(request.rid.clone(), code::INVALID.to_string(), message)
                .with_fix("Run `br show <bead-id> --json` and inspect response shape".to_string()),
        ),
        other => Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::CONFLICT.to_string(),
                format!("assign failed during br update and was rolled back for bead {bead_id}"),
            )
            .with_fix(
                "Retry once br command succeeds. Local claim was reverted to avoid drift"
                    .to_string(),
            )
            .with_ctx(
                json!({"bead_id": bead_id, "agent_id": agent_id, "error": other.to_string()}),
            ),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::map_assign_error;
    use crate::protocol_runtime::ProtocolRequest;
    use crate::{code, SwarmError};
    use serde_json::{Map, Value};

    fn sample_request() -> ProtocolRequest {
        ProtocolRequest {
            cmd: "assign".to_string(),
            rid: Some("rid-assign-error".to_string()),
            dry: Some(false),
            args: Map::new(),
        }
    }

    #[test]
    fn given_bead_error_when_mapping_then_notfound_envelope_is_returned() {
        let envelope = map_assign_error(
            &sample_request(),
            "bd-123",
            7,
            SwarmError::BeadError("missing bead".to_string()),
        );

        assert_eq!(
            envelope.err.as_ref().map(|err| err.code.as_str()),
            Some(code::NOTFOUND)
        );
        assert_eq!(
            envelope.err.as_ref().map(|err| err.msg.as_str()),
            Some("missing bead")
        );
        assert_eq!(
            envelope
                .err
                .as_ref()
                .and_then(|err| err.ctx.as_ref())
                .and_then(|ctx| ctx.get("agent_id"))
                .and_then(Value::as_u64),
            Some(7)
        );
    }

    #[test]
    fn given_agent_error_when_mapping_then_conflict_envelope_is_returned() {
        let envelope = map_assign_error(
            &sample_request(),
            "bd-123",
            3,
            SwarmError::AgentError("agent is busy".to_string()),
        );

        assert_eq!(
            envelope.err.as_ref().map(|err| err.code.as_str()),
            Some(code::CONFLICT)
        );
        assert_eq!(
            envelope.err.as_ref().map(|err| err.msg.as_str()),
            Some("agent is busy")
        );
        assert!(envelope
            .fix
            .as_deref()
            .map_or(false, |fix| fix.contains("idle agent")));
    }

    #[test]
    fn given_config_error_when_mapping_then_invalid_envelope_is_returned() {
        let envelope = map_assign_error(
            &sample_request(),
            "bd-777",
            5,
            SwarmError::ConfigError("status missing".to_string()),
        );

        assert_eq!(
            envelope.err.as_ref().map(|err| err.code.as_str()),
            Some(code::INVALID)
        );
        assert_eq!(
            envelope.err.as_ref().map(|err| err.msg.as_str()),
            Some("status missing")
        );
        assert!(envelope
            .fix
            .as_deref()
            .map_or(false, |fix| fix.contains("br show <bead-id> --json")));
    }

    #[test]
    fn given_internal_error_when_mapping_then_rollback_conflict_envelope_is_returned() {
        let envelope = map_assign_error(
            &sample_request(),
            "bd-900",
            11,
            SwarmError::Internal("br timeout".to_string()),
        );

        assert_eq!(
            envelope.err.as_ref().map(|err| err.code.as_str()),
            Some(code::CONFLICT)
        );
        assert!(envelope
            .err
            .as_ref()
            .map_or(false, |err| err.msg.contains("rolled back for bead bd-900")));
        assert!(envelope
            .err
            .as_ref()
            .and_then(|err| err.ctx.as_ref())
            .map_or(false, |ctx| {
                ctx.get("error")
                    .and_then(Value::as_str)
                    .map_or(false, |value| value.contains("br timeout"))
            }));
    }
}
