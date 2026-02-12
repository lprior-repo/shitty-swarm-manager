use assert_cmd::Command;

use crate::cli_e2e_common::e2e_enabled;
use crate::support::contract_harness::{assert_protocol_envelope, ProtocolScenarioHarness};

#[test]
fn next_command_dry_runs_robot_next_with_minimal_plan() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"next","dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["dry"], true);
    assert_eq!(json["d"]["would_do"][0]["action"], "bv_robot_next");
    Ok(())
}

#[test]
fn next_cli_dry_flag_short_circuits_bv_execution() -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["next", "--dry"])
        .assert()
        .success();

    let raw = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))?;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["dry"], true);
    Ok(())
}

#[test]
fn claim_next_command_dry_runs_selection_and_claim_plan() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"claim-next","dry":true}"#)?;
    let json = scenario.output;
    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["would_do"][0]["action"], "bv_robot_next");
    assert_eq!(json["d"]["would_do"][1]["action"], "br_update");
    Ok(())
}

#[test]
fn assign_command_dry_emits_br_synced_plan() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario =
        harness.run_protocol(r#"{"cmd":"assign","bead_id":"bd-test","agent_id":2,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["would_do"][0]["action"], "br_show");
    assert_eq!(json["d"]["would_do"][1]["action"], "claim_bead");
    assert_eq!(json["d"]["would_do"][2]["action"], "br_update");
    assert_eq!(json["d"]["would_do"][3]["action"], "br_verify");
    Ok(())
}

#[test]
fn run_once_command_dry_emits_compact_orchestration_plan() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"run-once","id":2,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["would_do"][0]["action"], "doctor");
    assert_eq!(json["d"]["would_do"][3]["action"], "agent");
    Ok(())
}

#[test]
fn qa_command_dry_smoke_reports_deterministic_checks() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"qa","target":"smoke","id":1,"dry":true}"#)?;
    let json = scenario.output;
    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["would_do"][0]["action"], "doctor");
    assert_eq!(json["d"]["would_do"][5]["action"], "monitor");
    Ok(())
}

#[test]
fn qa_command_rejects_unknown_target() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"qa","target":"unknown"}"#)?;
    let json = scenario.output;
    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");
    Ok(())
}

#[test]
fn claim_next_cli_dry_flag_short_circuits_external_calls() -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["claim-next", "--dry"])
        .assert()
        .success();
    let raw = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))?;
    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    Ok(())
}

#[test]
fn run_once_cli_dry_flag_short_circuits_orchestration() -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["run-once", "--id", "3", "--dry"])
        .assert()
        .success();
    let raw = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))?;
    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    Ok(())
}

#[test]
fn qa_cli_dry_flag_short_circuits_checks() -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["qa", "--target", "smoke", "--id", "1", "--dry"])
        .assert()
        .success();
    let raw = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))?;
    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    Ok(())
}

#[test]
fn run_once_live_executes_when_e2e_enabled() -> Result<(), String> {
    if !e2e_enabled() {
        return Ok(());
    }

    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["run-once", "--id", "1"])
        .assert()
        .success();
    let raw = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))?;
    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    Ok(())
}

#[test]
fn qa_smoke_live_executes_when_e2e_enabled() -> Result<(), String> {
    if !e2e_enabled() {
        return Ok(());
    }

    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["qa", "--target", "smoke", "--id", "1"])
        .assert()
        .success();
    let raw = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))?;
    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["target"], "smoke");
    Ok(())
}

#[test]
fn agent_cli_dry_flag_short_circuits_agent_loop() -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["agent", "--id", "1", "--dry"])
        .assert()
        .success();

    let raw = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))?;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["dry"], true);
    assert_eq!(json["d"]["would_do"][0]["action"], "run_agent");

    Ok(())
}
