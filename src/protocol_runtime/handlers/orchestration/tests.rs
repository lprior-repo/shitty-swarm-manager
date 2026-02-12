#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
use super::super::super::ProtocolRequest;
use super::assign::handle_assign;
use super::claim_next::handle_claim_next;
use super::helpers::{
    first_issue_from_br_payload, issue_id_from_br_payload, issue_status_from_br_payload,
    protocol_failure_to_swarm_error,
};
use super::run_once::handle_run_once;
use crate::protocol_envelope::ProtocolEnvelope;
use serde_json::{Map, Value};

#[test]
fn given_br_payload_array_when_extracting_issue_fields_then_first_issue_values_are_used() {
    let payload = serde_json::json!([
        {"id": "bd-101", "status": "open"},
        {"id": "bd-102", "status": "closed"}
    ]);

    let first = first_issue_from_br_payload(&payload).expect("first issue");
    assert_eq!(first.get("id").and_then(Value::as_str), Some("bd-101"));
    assert_eq!(
        issue_status_from_br_payload(&payload),
        Some("open".to_string())
    );
    assert_eq!(
        issue_id_from_br_payload(&payload),
        Some("bd-101".to_string())
    );
}

#[test]
fn given_non_issue_payload_when_extracting_issue_fields_then_none_is_returned() {
    let payload = serde_json::json!(42);
    assert!(first_issue_from_br_payload(&payload).is_none());
    assert!(issue_status_from_br_payload(&payload).is_none());
    assert!(issue_id_from_br_payload(&payload).is_none());
}

#[test]
fn given_object_issue_payload_when_extracting_issue_fields_then_values_are_returned() {
    let payload = serde_json::json!({"id": "bd-9", "status": "in_progress"});

    assert_eq!(
        issue_status_from_br_payload(&payload),
        Some("in_progress".to_string())
    );
    assert_eq!(issue_id_from_br_payload(&payload), Some("bd-9".to_string()));
}

#[test]
fn given_protocol_failure_without_error_when_mapping_to_swarm_error_then_default_message_is_used() {
    let failure = ProtocolEnvelope {
        ok: false,
        rid: Some("rid-1".to_string()),
        t: 0,
        ms: None,
        d: None,
        err: None,
        fix: None,
        next: None,
        state: None,
    };

    let mapped = protocol_failure_to_swarm_error(failure);
    assert!(mapped.to_string().contains("Protocol command failed"));
}

#[test]
fn given_protocol_failure_with_error_when_mapping_to_swarm_error_then_error_message_is_propagated()
{
    let failure = ProtocolEnvelope::error(
        Some("rid-2".to_string()),
        "invalid".to_string(),
        "missing id".to_string(),
    );

    let mapped = protocol_failure_to_swarm_error(failure);
    assert!(mapped.to_string().contains("missing id"));
}

#[tokio::test]
async fn given_dry_claim_next_when_executed_then_it_returns_reversible_plan() {
    let request = ProtocolRequest {
        cmd: "claim-next".to_string(),
        rid: Some("rid-dry-claim".to_string()),
        dry: Some(true),
        args: Map::new(),
    };

    let success = handle_claim_next(&request)
        .await
        .expect("dry run claim-next");
    assert_eq!(success.data.get("dry").and_then(Value::as_bool), Some(true));
    assert_eq!(
        success
            .data
            .get("would_do")
            .and_then(Value::as_array)
            .map(std::vec::Vec::len),
        Some(2)
    );
}

#[tokio::test]
async fn given_dry_assign_when_executed_then_it_returns_assignment_plan() {
    let request = ProtocolRequest {
        cmd: "assign".to_string(),
        rid: Some("rid-dry-assign".to_string()),
        dry: Some(true),
        args: Map::from_iter(vec![
            ("bead_id".to_string(), Value::String("bd-777".to_string())),
            ("agent_id".to_string(), Value::from(2_u64)),
        ]),
    };

    let success = handle_assign(&request).await.expect("dry run assign");
    assert_eq!(success.data.get("dry").and_then(Value::as_bool), Some(true));
    assert_eq!(
        success
            .data
            .get("would_do")
            .and_then(Value::as_array)
            .map(std::vec::Vec::len),
        Some(4)
    );
}

#[tokio::test]
async fn given_assign_request_without_bead_id_when_executed_then_invalid_error_is_returned() {
    let request = ProtocolRequest {
        cmd: "assign".to_string(),
        rid: Some("rid-assign-missing-bead".to_string()),
        dry: Some(false),
        args: Map::from_iter(vec![("agent_id".to_string(), Value::from(2_u64))]),
    };

    let result = handle_assign(&request).await;
    assert!(result.is_err(), "missing bead id should error");
    let error = match result {
        Ok(_) => unreachable!("expected missing bead id error"),
        Err(error) => error,
    };
    assert_eq!(
        error.err.as_ref().map(|err| err.code.as_str()),
        Some("INVALID")
    );
}

#[tokio::test]
async fn given_assign_request_without_agent_id_when_executed_then_invalid_error_is_returned() {
    let request = ProtocolRequest {
        cmd: "assign".to_string(),
        rid: Some("rid-assign-missing-agent".to_string()),
        dry: Some(false),
        args: Map::from_iter(vec![(
            "bead_id".to_string(),
            Value::String("bd-42".to_string()),
        )]),
    };

    let result = handle_assign(&request).await;
    assert!(result.is_err(), "missing agent id should error");
    let error = match result {
        Ok(_) => unreachable!("expected missing agent id error"),
        Err(error) => error,
    };
    assert_eq!(
        error.err.as_ref().map(|err| err.code.as_str()),
        Some("INVALID")
    );
}

#[tokio::test]
async fn given_dry_run_once_when_executed_then_it_returns_five_step_plan() {
    let request = ProtocolRequest {
        cmd: "run-once".to_string(),
        rid: Some("rid-dry-once".to_string()),
        dry: Some(true),
        args: Map::new(),
    };

    let success = handle_run_once(&request).await.expect("dry run run-once");
    assert_eq!(success.data.get("dry").and_then(Value::as_bool), Some(true));
    assert_eq!(
        success
            .data
            .get("would_do")
            .and_then(Value::as_array)
            .map(std::vec::Vec::len),
        Some(5)
    );
}

#[tokio::test]
async fn given_dry_run_once_with_explicit_id_when_executed_then_agent_step_targets_requested_id() {
    let request = ProtocolRequest {
        cmd: "run-once".to_string(),
        rid: Some("rid-dry-once-id".to_string()),
        dry: Some(true),
        args: Map::from_iter(vec![("id".to_string(), Value::from(9_u64))]),
    };

    let success = handle_run_once(&request)
        .await
        .expect("dry run run-once with id");
    let target = success
        .data
        .get("would_do")
        .and_then(Value::as_array)
        .and_then(|steps| steps.get(3))
        .and_then(|step| step.get("target"))
        .and_then(Value::as_u64);
    assert_eq!(target, Some(9));
}
