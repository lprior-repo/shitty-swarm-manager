#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod support;

use serde_json::Value;
use support::contract_harness::{assert_protocol_envelope, ProtocolScenarioHarness};

#[test]
fn artifacts_command_requires_bead_id() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"artifacts","rid":"artifacts-missing-bead"}"#)?;
    let output = scenario.output;

    assert_protocol_envelope(&output)?;
    if output["ok"] != Value::Bool(false) {
        return Err(format!(
            "expected artifacts request without bead_id to fail, got: {output}"
        ));
    }
    if output["err"]["code"] != Value::String("INVALID".to_string()) {
        return Err(format!(
            "expected INVALID code for missing bead_id, got: {output}"
        ));
    }
    if !output["fix"]
        .as_str()
        .is_some_and(|text| text.contains("bead_id"))
    {
        return Err(format!(
            "expected fix guidance mentioning bead_id, got: {output}"
        ));
    }

    Ok(())
}

#[test]
fn artifacts_command_rejects_unknown_artifact_type() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"artifacts","rid":"artifacts-bad-type","bead_id":"bead-123","artifact_type":"NotARealType"}"#,
    )?;
    let output = scenario.output;

    assert_protocol_envelope(&output)?;
    if output["ok"] != Value::Bool(false) {
        return Err(format!(
            "expected artifacts request with invalid artifact_type to fail, got: {output}"
        ));
    }
    if output["err"]["code"] != Value::String("INVALID".to_string()) {
        return Err(format!(
            "expected INVALID code for unknown artifact_type, got: {output}"
        ));
    }
    if output["err"]["ctx"]["artifact_type"] != Value::String("NotARealType".to_string()) {
        return Err(format!(
            "expected err.ctx.artifact_type to echo rejected value, got: {output}"
        ));
    }

    Ok(())
}

#[test]
fn artifacts_command_rejects_empty_bead_id() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario =
        harness.run_protocol(r#"{"cmd":"artifacts","rid":"artifacts-empty-bead","bead_id":""}"#)?;
    let output = scenario.output;

    assert_protocol_envelope(&output)?;
    if output["ok"] != Value::Bool(false) {
        return Err(format!(
            "expected artifacts request with empty bead_id to fail, got: {output}"
        ));
    }
    if output["err"]["code"] != Value::String("INVALID".to_string()) {
        return Err(format!(
            "expected INVALID code for empty bead_id, got: {output}"
        ));
    }
    if !output["fix"]
        .as_str()
        .is_some_and(|text| text.contains("bead_id"))
    {
        return Err(format!(
            "expected fix guidance mentioning bead_id, got: {output}"
        ));
    }

    Ok(())
}

#[test]
fn artifacts_command_rejects_whitespace_only_bead_id() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness
        .run_protocol(r#"{"cmd":"artifacts","rid":"artifacts-whitespace-bead","bead_id":"   "}"#)?;
    let output = scenario.output;

    assert_protocol_envelope(&output)?;
    if output["ok"] != Value::Bool(false) {
        return Err(format!(
            "expected artifacts request with whitespace-only bead_id to fail, got: {output}"
        ));
    }
    if output["err"]["code"] != Value::String("INVALID".to_string()) {
        return Err(format!(
            "expected INVALID code for whitespace-only bead_id, got: {output}"
        ));
    }

    Ok(())
}

#[test]
fn artifacts_command_rejects_bead_id_exceeding_255_characters() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let long_id = "x".repeat(256);
    let request = format!(
        r#"{{"cmd":"artifacts","rid":"artifacts-long-bead","bead_id":"{}"}}"#,
        long_id
    );
    let scenario = harness.run_protocol(&request)?;
    let output = scenario.output;

    assert_protocol_envelope(&output)?;
    if output["ok"] != Value::Bool(false) {
        return Err(format!(
            "expected artifacts request with bead_id > 255 chars to fail, got: {output}"
        ));
    }
    if output["err"]["code"] != Value::String("INVALID".to_string()) {
        return Err(format!(
            "expected INVALID code for bead_id > 255 chars, got: {output}"
        ));
    }
    if !output["fix"]
        .as_str()
        .is_some_and(|text| text.contains("255"))
    {
        return Err(format!(
            "expected fix guidance mentioning 255 character limit, got: {output}"
        ));
    }

    Ok(())
}
