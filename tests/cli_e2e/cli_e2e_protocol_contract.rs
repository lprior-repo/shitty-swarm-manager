use assert_cmd::Command;
use predicates::str::contains;

use crate::support::contract_harness::{
    assert_contract_test_is_decoupled, assert_protocol_envelope, ProtocolScenarioHarness,
};

#[test]
fn help_command_returns_protocol_envelope() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"?","rid":"r-1"}"#)?;
    let json = scenario.output;

    assert_eq!(json["ok"], true);
    assert_eq!(json["rid"], "r-1");
    assert!(json["t"].is_i64());
    assert!(json["ms"].is_i64());
    assert!(json["d"]["commands"].is_object());
    assert!(json["state"]["total"].is_number());

    Ok(())
}

#[test]
fn help_command_documents_batch_ops_contract() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"?","rid":"batch-help"}"#)?;
    let json = scenario.output;

    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["batch_input"]["required"], "ops");
    assert_eq!(json["d"]["batch_input"]["not"], "cmds");
    assert!(json["d"]["batch_input"]["example"].is_string());

    Ok(())
}

#[test]
fn batch_with_cmds_field_returns_actionable_ops_hint() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario =
        harness.run_protocol(r#"{"cmd":"batch","cmds":[{"cmd":"doctor"}],"dry":false}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    let fix = json["fix"]
        .as_str()
        .ok_or_else(|| format!("expected fix hint string, got: {json}"))?;
    if !(fix.contains("ops") && fix.contains("cmds")) {
        return Err(format!(
            "expected fix hint to explain ops-vs-cmds contract, got: {json}"
        ));
    }

    Ok(())
}

#[test]
fn batch_rejects_empty_ops_array() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"batch","ops":[],"dry":false}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    let msg = json["err"]["msg"]
        .as_str()
        .ok_or_else(|| format!("expected error message, got: {json}"))?;
    if !msg.contains("empty") && !msg.contains("at least one") {
        return Err(format!(
            "expected error message to mention empty/require at least one op, got: {msg}"
        ));
    }

    Ok(())
}

#[test]
fn invalid_command_returns_structured_error() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"nope"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");
    assert!(json["fix"].is_string());

    Ok(())
}

#[test]
fn init_db_default_schema_path_is_canonical() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario =
        harness.run_protocol(r#"{"cmd":"init-db","rid":"schema-default","dry":true}"#)?;
    assert_protocol_envelope(&scenario.output)?;

    let steps = scenario.output["d"]["would_do"]
        .as_array()
        .ok_or_else(|| "missing dry-run steps".to_string())?;

    let schema_step = steps
        .iter()
        .find(|step| step["action"] == "apply_schema")
        .ok_or_else(|| "apply_schema step missing".to_string())?;

    let target = schema_step["target"]
        .as_str()
        .ok_or_else(|| "apply_schema target is not a string".to_string())?;

    if !target.starts_with("embedded:") {
        return Err(format!(
            "unexpected schema target {target}, expected embedded schema reference"
        ));
    }

    Ok(())
}

#[test]
fn dry_run_lock_uses_standard_dry_shape() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"lock","resource":"res_abc","agent":"agent-1","ttl_ms":30000,"dry":true}"#,
    )?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["dry"], true);
    assert!(json["d"]["would_do"].is_array());
    assert!(json["d"]["estimated_ms"].is_number());

    Ok(())
}

#[test]
fn lock_rejects_empty_resource() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"lock","resource":"","agent":"agent-1","ttl_ms":30000,"dry":true}"#,
    )?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn lock_rejects_whitespace_only_resource() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"lock","resource":"   ","agent":"agent-1","ttl_ms":30000,"dry":true}"#,
    )?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn unlock_rejects_empty_resource() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario =
        harness.run_protocol(r#"{"cmd":"unlock","resource":"","agent":"agent-1","dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn unlock_rejects_whitespace_only_resource() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness
        .run_protocol(r#"{"cmd":"unlock","resource":"   ","agent":"agent-1","dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn broadcast_rejects_empty_msg_parameter() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario =
        harness.run_protocol(r#"{"cmd":"broadcast","msg":"","from":"agent-1","dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn broadcast_cli_rejects_empty_msg_parameter() {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    Command::new(binary_path)
        .args(["broadcast", "--msg", "", "--from", "agent-1", "--dry"])
        .assert()
        .failure()
        .stdout(contains("msg cannot be empty"));
}

#[test]
fn broadcast_rejects_empty_from_parameter() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario =
        harness.run_protocol(r#"{"cmd":"broadcast","msg":"hello","from":"","dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn broadcast_cli_rejects_empty_from_parameter() {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    Command::new(binary_path)
        .args(["broadcast", "--msg", "hello", "--from", "", "--dry"])
        .assert()
        .failure()
        .stdout(contains("from cannot be empty"));
}

#[test]
fn status_command_includes_bead_terminology_timestamp_and_breakdown() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"status"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert!(json["d"]["closed"].is_number());
    assert!(json["d"]["done"].is_number());
    assert!(json["d"]["timestamp"].is_string());
    assert!(json["d"]["beads_by_status"].is_object());

    Ok(())
}

#[test]
fn status_cli_help_flag_returns_help_envelope_instead_of_executing_status() -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["status", "--help"])
        .assert()
        .success();

    let raw = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))?;

    assert_eq!(json["ok"], true);
    if !json["d"]["cmds"].is_array() {
        return Err(format!(
            "expected help payload with cmds array, got: {json}"
        ));
    }

    Ok(())
}

#[test]
fn status_cli_unknown_flag_fails_fast() {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    Command::new(binary_path)
        .args(["status", "--definitely-invalid-flag"])
        .assert()
        .failure()
        .stderr(contains("Unknown command: --definitely-invalid-flag"));
}

#[test]
fn cli_contract_test_assertions_stay_decoupled_from_internals() -> Result<(), String> {
    assert_contract_test_is_decoupled("tests/cli_e2e/cli_e2e_protocol_contract.rs")
}
