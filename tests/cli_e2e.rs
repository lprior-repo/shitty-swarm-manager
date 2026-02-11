mod support;
use assert_cmd::Command;
use predicates::str::contains;
use std::fs;
use std::path::Path;

use support::contract_harness::{
    assert_contract_test_is_decoupled, assert_protocol_envelope, ProtocolScenarioHarness,
};

fn e2e_enabled() -> bool {
    std::env::var("SWARM_E2E")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn local_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://shitty_swarm_manager:shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db"
            .to_string()
    })
}

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
            "unexpected schema target {target}, expected embedded schema reference",
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
fn next_command_dry_runs_robot_next_with_minimal_plan() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"next","dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["dry"], true);

    let steps = json["d"]["would_do"]
        .as_array()
        .ok_or_else(|| "missing dry next steps".to_string())?;
    let step = steps
        .first()
        .ok_or_else(|| "expected one dry-run step for next command".to_string())?;

    assert_eq!(step["action"], "bv_robot_next");
    assert_eq!(step["target"], "bv --robot-next");

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
    assert_eq!(json["d"]["would_do"][0]["action"], "bv_robot_next");

    Ok(())
}

#[test]
fn claim_next_command_dry_runs_selection_and_claim_plan() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"claim-next","dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["dry"], true);

    let steps = json["d"]["would_do"]
        .as_array()
        .ok_or_else(|| "missing claim-next dry steps".to_string())?;
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0]["action"], "bv_robot_next");
    assert_eq!(steps[1]["action"], "br_update");

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
    assert_eq!(json["d"]["dry"], true);

    let steps = json["d"]["would_do"]
        .as_array()
        .ok_or_else(|| "missing assign dry steps".to_string())?;
    assert_eq!(steps.len(), 4);
    assert_eq!(steps[0]["action"], "br_show");
    assert_eq!(steps[1]["action"], "claim_bead");
    assert_eq!(steps[2]["action"], "br_update");
    assert_eq!(steps[3]["action"], "br_verify");

    Ok(())
}

#[test]
fn run_once_command_dry_emits_compact_orchestration_plan() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"run-once","id":2,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["dry"], true);

    let steps = json["d"]["would_do"]
        .as_array()
        .ok_or_else(|| "missing run-once dry steps".to_string())?;
    assert_eq!(steps.len(), 5);
    assert_eq!(steps[0]["action"], "doctor");
    assert_eq!(steps[3]["action"], "agent");

    Ok(())
}

#[test]
fn qa_command_dry_smoke_reports_deterministic_checks() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"qa","target":"smoke","id":1,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["dry"], true);

    let steps = json["d"]["would_do"]
        .as_array()
        .ok_or_else(|| "missing qa dry steps".to_string())?;
    assert_eq!(steps.len(), 6);
    assert_eq!(steps[0]["action"], "doctor");
    assert_eq!(steps[5]["action"], "monitor");

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
    assert_eq!(json["d"]["dry"], true);

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
    assert_eq!(json["d"]["dry"], true);

    Ok(())
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
    assert!(json["d"]["beads_by_status"]["open"].is_number());
    assert!(json["d"]["beads_by_status"]["in_progress"].is_number());
    assert!(json["d"]["beads_by_status"]["closed"].is_number());

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
fn run_once_rejects_unknown_fields_in_protocol_payload() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"run-once","agent_id":9999,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    let msg = json["err"]["msg"]
        .as_str()
        .ok_or_else(|| format!("expected error message for unknown field, got: {json}"))?;
    if !msg.contains("Unknown field(s) for run-once") || !msg.contains("agent_id") {
        return Err(format!(
            "expected unknown-field validation message, got: {json}"
        ));
    }

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

    let msg = json["err"]["msg"]
        .as_str()
        .ok_or_else(|| format!("expected null-byte validation message, got: {json}"))?;
    if !msg.contains("Null byte is not allowed") || !msg.contains("resource") {
        return Err(format!(
            "expected explicit null-byte validation on resource field, got: {json}"
        ));
    }

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

    let msg = json["err"]["msg"]
        .as_str()
        .ok_or_else(|| format!("expected nested null-byte validation message, got: {json}"))?;
    if !msg.contains("Null byte is not allowed") || !msg.contains("ops[0].resource") {
        return Err(format!(
            "expected nested null-byte validation path ops[0].resource, got: {json}"
        ));
    }

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

    let msg = json["err"]["msg"]
        .as_str()
        .ok_or_else(|| format!("expected type validation message, got: {json}"))?;
    if !msg.contains("Invalid type for field id") {
        return Err(format!(
            "expected explicit id type validation message, got: {json}"
        ));
    }

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

    let msg = json["err"]["msg"]
        .as_str()
        .ok_or_else(|| format!("expected id validation message, got: {json}"))?;
    if !msg.contains("Invalid value for field id") || !msg.contains("greater than 0") {
        return Err(format!(
            "expected explicit positive-id validation message, got: {json}"
        ));
    }

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

    let connect_ms = json["d"]["timing"]["db"]["connect_ms"]
        .as_i64()
        .ok_or_else(|| format!("missing status timing.db.connect_ms in response: {json}"))?;
    if connect_ms > 2_000 {
        return Err(format!(
            "expected bounded fallback connect latency <= 2000ms, got {connect_ms}ms ({json})"
        ));
    }

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

    let db_check_ms = json["d"]["timing"]["checks_ms"]["database"]
        .as_i64()
        .ok_or_else(|| format!("missing doctor timing.checks_ms.database in response: {json}"))?;
    if db_check_ms > 2_000 {
        return Err(format!(
            "expected bounded doctor database check latency <= 2000ms, got {db_check_ms}ms ({json})"
        ));
    }

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

    let connect_ms = json["d"]["timing"]["db"]["connect_ms"]
        .as_i64()
        .ok_or_else(|| format!("missing status timing.db.connect_ms in response: {json}"))?;
    if connect_ms > 2_000 {
        return Err(format!(
            "expected request connect_timeout_ms override to keep latency <= 2000ms, got {connect_ms}ms ({json})"
        ));
    }

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
    assert_eq!(json["d"]["dry"], true);

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
    assert!(json["d"]["steps"]["doctor"].is_object());
    assert!(json["d"]["steps"]["claim_next"].is_object());
    assert!(json["d"]["steps"]["progress"].is_object());

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
    assert!(json["d"]["checks"]["doctor"].is_object());
    assert!(json["d"]["checks"]["status"].is_object());

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

    if json["d"].get("status").is_some() || json["d"].get("agent_id").is_some() {
        return Err(
            "dry agent path should not return full loop completion payload fields".to_string(),
        );
    }

    Ok(())
}

#[test]
fn spawn_prompts_default_template_is_canonical() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness
        .run_protocol(r#"{"cmd":"spawn-prompts","count":1,"dry":true,"rid":"spawn-template"}"#)?;
    assert_protocol_envelope(&scenario.output)?;
    let steps = scenario.output["d"]["would_do"]
        .as_array()
        .ok_or_else(|| "missing dry-run steps".to_string())?;
    let template_step = steps
        .iter()
        .find(|step| step["action"] == "read_template")
        .ok_or_else(|| "read_template step missing".to_string())?;
    let target = template_step["target"]
        .as_str()
        .ok_or_else(|| "read_template target missing".to_string())?;

    let expected = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".agents")
        .join("agent_prompt.md")
        .canonicalize()
        .map_err(|err| format!("failed to canonicalize canonical prompt path: {err}"))?;
    let observed = Path::new(target)
        .canonicalize()
        .map_err(|err| format!("failed to canonicalize template target path: {err}"))?;
    if observed != expected {
        return Err(format!(
            "spawn-prompts default template should be canonical (observed {observed:?}, expected {expected:?})"
        ));
    }

    Ok(())
}

#[test]
fn prompt_command_matches_canonical_template() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"prompt","id":5,"rid":"prompt-parity"}"#)?;
    assert_protocol_envelope(&scenario.output)?;
    let prompt = scenario.output["d"]["prompt"]
        .as_str()
        .ok_or_else(|| "missing prompt payload".to_string())?;

    let template = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(".agents")
            .join("agent_prompt.md"),
    )
    .map_err(|err| format!("failed to load canonical template: {err}"))?;
    let expected = template.replace("#{N}", "5").replace("{N}", "5");
    if prompt != expected {
        return Err(
            "prompt command must match canonical template after placeholder replacement"
                .to_string(),
        );
    }

    Ok(())
}

#[test]
fn batch_partial_success_reports_summary() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"batch","ops":[{"cmd":"?"},{"cmd":"definitely-invalid"}],"dry":false}"#,
    )?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;

    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["summary"]["total"], 2);
    assert_eq!(json["d"]["summary"]["pass"], 1);
    assert_eq!(json["d"]["summary"]["fail"], 1);

    Ok(())
}

#[test]
fn cli_contract_test_assertions_stay_decoupled_from_internals() -> Result<(), String> {
    assert_contract_test_is_decoupled("tests/cli_e2e.rs")
}

#[test]
fn resume_context_command_exposes_context_payload() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"resume-context"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    if json["ok"] != serde_json::Value::Bool(true) {
        return Err(format!("expected success payload, got: {json}"));
    }
    if !json["d"]["contexts"].is_array() {
        return Err(format!("unexpected resume-context payload, got: {json}"));
    }

    Ok(())
}

#[test]
fn resume_context_with_unknown_bead_returns_notfound() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"resume-context","bead_id":"swm-unknown"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    if json["ok"] != serde_json::Value::Bool(false) {
        return Err(format!("expected failure payload, got: {json}"));
    }
    if json["err"]["code"] != serde_json::Value::String("NOTFOUND".to_string()) {
        return Err(format!("expected NOTFOUND error code, got: {json}"));
    }

    Ok(())
}

#[test]
fn init_dry_response_masks_database_password() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"init","dry":true,"seed_agents":4,"database_url":"postgres://swarm_user:supersecret@localhost:5432/swarm_db"}"#,
    )?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);

    let steps = json["d"]["would_do"]
        .as_array()
        .ok_or_else(|| "missing init dry-run steps".to_string())?;
    let init_db_step = steps
        .iter()
        .find(|step| step["action"] == "init_db")
        .ok_or_else(|| "init_db step missing from dry-run output".to_string())?;
    let target = init_db_step["target"]
        .as_str()
        .ok_or_else(|| "init_db target is not a string".to_string())?;

    if !target.contains("********") {
        return Err(format!(
            "expected masked password in init_db target, got '{target}'"
        ));
    }
    if target.contains("supersecret") || json.to_string().contains("supersecret") {
        return Err("init response leaked raw database password".to_string());
    }

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

    let checks = json["d"]["c"]
        .as_array()
        .ok_or_else(|| "doctor response missing checks array".to_string())?;
    let database_check = checks
        .iter()
        .find(|check| check["n"] == "database")
        .ok_or_else(|| "doctor response missing database check".to_string())?;

    if database_check["ok"] != serde_json::Value::Bool(true) {
        return Err(format!(
            "expected unreachable explicit database_url to recover via fallback, got {database_check}"
        ));
    }

    Ok(())
}

#[test]
fn state_command_reports_repo_scoped_resource_metadata() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"state"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert!(json["d"]["repo_id"].is_string());
    assert!(json["d"]["resources"].is_array());
    assert!(json["d"]["resources_total"].is_number());
    assert!(json["d"]["resources_truncated"].is_boolean());

    Ok(())
}

#[test]
fn monitor_active_reports_repo_id_and_repo_scoped_rows() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"monitor","view":"active"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);

    let repo_id = json["d"]["repo_id"]
        .as_str()
        .ok_or_else(|| "monitor active payload missing repo_id string".to_string())?;
    let rows = json["d"]["rows"]
        .as_array()
        .ok_or_else(|| "monitor active payload missing rows array".to_string())?;

    rows.iter().try_for_each(|row| {
        let row_repo = row["repo"]
            .as_str()
            .ok_or_else(|| format!("row missing repo field: {row}"))?;
        if row_repo != repo_id {
            return Err(format!(
                "monitor active row escaped repo scope: expected {repo_id}, got {row_repo}"
            ));
        }
        Ok(())
    })
}
