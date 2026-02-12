use assert_cmd::Command;
use predicates::str::contains;

use crate::cli_e2e_common::local_database_url;
use crate::support::contract_harness::{assert_protocol_envelope, ProtocolScenarioHarness};

#[test]
fn run_once_rejects_unknown_fields_in_protocol_payload() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"run-once","agent_id":9999,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");
    Ok(())
}

#[test]
fn lock_command_rejects_null_byte_in_resource_field() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"lock","resource":"repo\u0000tmp","agent":"agent-1","ttl_ms":1000,"dry":true}"#,
    )?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");
    Ok(())
}

#[test]
fn batch_command_rejects_null_byte_in_nested_operation_payload() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"batch","ops":[{"cmd":"lock","resource":"repo\u0000tmp","agent":"agent-1","ttl_ms":1000,"dry":true}]}"#,
    )?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");
    Ok(())
}

#[test]
fn agent_command_rejects_non_numeric_id_with_type_error() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"agent","id":"abc","dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");
    Ok(())
}

#[test]
fn agent_command_rejects_zero_id_with_validation_error() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"agent","id":0,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");
    Ok(())
}

#[test]
fn agent_cli_rejects_non_numeric_id_with_type_error() {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    Command::new(binary_path)
        .args(["agent", "--id", "not-a-number", "--dry"])
        .assert()
        .failure()
        .stderr(contains("Invalid type for id"));
}

#[test]
fn agent_cli_rejects_zero_id_with_validation_error() {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    Command::new(binary_path)
        .args(["agent", "--id", "0", "--dry"])
        .assert()
        .failure()
        .stdout(contains(
            "Invalid value for field id: must be greater than 0",
        ));
}

#[test]
fn status_command_fallbacks_from_unreachable_explicit_url_with_bounded_latency(
) -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .env("DATABASE_URL", local_database_url())
        .env("SWARM_DB_CONNECT_TIMEOUT_MS", "250")
        .write_stdin(
            "{\"cmd\":\"status\",\"database_url\":\"postgres://invalid:invalid@127.0.0.1:1/does_not_exist\"}\n",
        )
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
fn doctor_command_fallbacks_from_unreachable_explicit_url_with_bounded_latency(
) -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .env("DATABASE_URL", local_database_url())
        .env("SWARM_DB_CONNECT_TIMEOUT_MS", "250")
        .write_stdin(
            "{\"cmd\":\"doctor\",\"database_url\":\"postgres://invalid:invalid@127.0.0.1:1/does_not_exist\"}\n",
        )
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
fn status_command_honors_request_connect_timeout_override() -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .env("DATABASE_URL", local_database_url())
        .write_stdin(
            "{\"cmd\":\"status\",\"database_url\":\"postgres://invalid:invalid@127.0.0.1:1/does_not_exist\",\"connect_timeout_ms\":100}\n",
        )
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
fn empty_stdin_returns_structured_invalid_envelope() -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path).assert().success();

    let raw = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))?;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn doctor_with_explicit_unreachable_database_url_falls_back_to_candidates() -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .env("DATABASE_URL", local_database_url())
        .write_stdin(
            "{\"cmd\":\"doctor\",\"database_url\":\"postgresql://nope@127.0.0.1:1/no_db\"}\n",
        )
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
