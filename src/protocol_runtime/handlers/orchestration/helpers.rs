use crate::protocol_envelope::ProtocolEnvelope;
use crate::SwarmError;
use serde_json::Value;

pub(super) fn first_issue_from_br_payload(payload: &Value) -> Option<&Value> {
    if payload.is_object() {
        return Some(payload);
    }

    payload.as_array().and_then(|items| items.first())
}

pub(super) fn issue_status_from_br_payload(payload: &Value) -> Option<String> {
    first_issue_from_br_payload(payload)
        .and_then(|issue| issue.get("status"))
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
}

pub(super) fn issue_id_from_br_payload(payload: &Value) -> Option<String> {
    first_issue_from_br_payload(payload)
        .and_then(|issue| issue.get("id"))
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
}

pub(super) fn protocol_failure_to_swarm_error(failure: ProtocolEnvelope) -> SwarmError {
    let message = failure.err.as_ref().map_or_else(
        || "Protocol command failed".to_string(),
        |err| err.msg.clone(),
    );
    SwarmError::Internal(message)
}
