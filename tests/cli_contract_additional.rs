use assert_cmd::Command;
use predicates::str::contains;
use serde_json::Value;

fn assert_protocol_envelope_has_ok_and_timestamp(output: &Value) -> Result<(), String> {
    match (output.get("ok"), output.get("t")) {
        (Some(_), Some(timestamp)) if timestamp.is_number() => Ok(()),
        _ => Err(format!(
            "Given protocol envelope contract, When asserting response shape, Then ok/t should exist: {output}"
        )),
    }
}

fn parse_json_stdout(output: &[u8]) -> Result<Value, String> {
    let raw = String::from_utf8_lossy(output).trim().to_string();
    serde_json::from_str::<Value>(&raw).map_err(|err| {
        format!(
            "Given CLI JSON output, When parsed, Then parsing should succeed: {err}. Raw: {raw}"
        )
    })
}

#[test]
fn given_global_version_flag_when_invoked_then_protocol_version_envelope_is_returned(
) -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["--version"])
        .assert()
        .success();

    let json = parse_json_stdout(&assert.get_output().stdout)?;
    assert_protocol_envelope_has_ok_and_timestamp(&json)?;
    if json["ok"] != Value::Bool(true) {
        return Err(format!(
            "Given --version, When command executes, Then ok should be true. Got: {json}"
        ));
    }
    if json["d"]["n"] != Value::String("swarm".to_string()) {
        return Err(format!(
            "Given --version, When command executes, Then name should be swarm. Got: {json}"
        ));
    }
    if json["d"]["proto"] != Value::String("v1".to_string()) {
        return Err(format!(
            "Given --version, When command executes, Then proto should be v1. Got: {json}"
        ));
    }

    Ok(())
}

#[test]
fn given_json_mode_without_command_when_invoked_then_missing_required_command_error_is_returned() {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    Command::new(binary_path)
        .args(["--json"])
        .assert()
        .failure()
        .stderr(contains("Missing required argument: command"));
}

#[test]
fn given_json_mode_with_help_symbol_when_invoked_then_help_contract_is_returned(
) -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args(["--json", "?"])
        .assert()
        .success();

    let json = parse_json_stdout(&assert.get_output().stdout)?;
    assert_protocol_envelope_has_ok_and_timestamp(&json)?;
    if json["ok"] != Value::Bool(true) {
        return Err(format!(
            "Given --json ?, When processed, Then help should succeed. Got: {json}"
        ));
    }
    if !json["d"].is_object() {
        return Err(format!(
            "Given --json ?, When processed, Then payload d should be object. Got: {json}"
        ));
    }

    Ok(())
}

#[test]
fn given_typo_command_when_invoked_then_suggested_command_is_rendered_on_stderr() {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    Command::new(binary_path)
        .args(["statu"])
        .assert()
        .failure()
        .stderr(contains("Unknown command: statu"))
        .stderr(contains("Did you mean: status?"));
}

#[test]
fn given_doctor_command_with_unknown_flag_when_invoked_then_parser_fails_fast() {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    Command::new(binary_path)
        .args(["doctor", "--unknown"])
        .assert()
        .failure()
        .stderr(contains("Unknown command: --unknown"));
}

#[test]
fn given_assign_command_without_agent_id_value_when_invoked_then_required_argument_error_is_returned(
) {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    Command::new(binary_path)
        .args(["assign", "--bead-id", "bd-123", "--agent-id", "--dry"])
        .assert()
        .failure()
        .stderr(contains("Missing required argument: agent_id"));
}

#[test]
fn given_lock_command_with_dry_flag_when_invoked_then_non_mutating_plan_contract_is_returned(
) -> Result<(), String> {
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let assert = Command::new(binary_path)
        .args([
            "lock",
            "--resource",
            "res-contract",
            "--agent",
            "agent-contract",
            "--ttl-ms",
            "1000",
            "--dry",
        ])
        .assert()
        .success();

    let json = parse_json_stdout(&assert.get_output().stdout)?;
    assert_protocol_envelope_has_ok_and_timestamp(&json)?;
    if json["ok"] != Value::Bool(true) {
        return Err(format!(
            "Given lock --dry, When processed, Then command should succeed. Got: {json}"
        ));
    }
    if json["d"]["dry"] != Value::Bool(true) {
        return Err(format!(
            "Given lock --dry, When processed, Then response should mark dry=true. Got: {json}"
        ));
    }
    if !json["d"]["would_do"].is_array() {
        return Err(format!(
            "Given lock --dry, When processed, Then would_do should be array. Got: {json}"
        ));
    }

    Ok(())
}

#[test]
fn given_prompt_command_without_id_when_invoked_then_default_agent_id_is_used() -> Result<(), String>
{
    let binary_path = assert_cmd::cargo::cargo_bin!("swarm");
    let without_id = Command::new(&binary_path)
        .args(["prompt"])
        .assert()
        .success();
    let explicit_id = Command::new(binary_path)
        .args(["prompt", "--id", "1"])
        .assert()
        .success();

    let without_id_json = parse_json_stdout(&without_id.get_output().stdout)?;
    let explicit_id_json = parse_json_stdout(&explicit_id.get_output().stdout)?;
    assert_protocol_envelope_has_ok_and_timestamp(&without_id_json)?;
    assert_protocol_envelope_has_ok_and_timestamp(&explicit_id_json)?;

    if without_id_json["ok"] != Value::Bool(true) || explicit_id_json["ok"] != Value::Bool(true) {
        return Err(format!(
            "Given prompt calls, When processed, Then both responses should succeed. Without id: {without_id_json}. Explicit id: {explicit_id_json}"
        ));
    }
    if without_id_json["d"]["prompt"] != explicit_id_json["d"]["prompt"] {
        return Err(format!(
            "Given prompt default id contract, When comparing prompt vs --id 1, Then payload should match. Without id: {without_id_json}. Explicit id: {explicit_id_json}"
        ));
    }

    Ok(())
}
