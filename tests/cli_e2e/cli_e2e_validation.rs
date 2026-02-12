use crate::support::contract_harness::{assert_protocol_envelope, ProtocolScenarioHarness};

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
fn state_command_reports_repo_scoped_resource_metadata() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"state"}"#)?;
    let json = scenario.output;
    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert!(json["d"]["repo_id"].is_string());
    assert!(json["d"]["resources"].is_array());
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

#[test]
fn monitor_command_rejects_negative_watch_ms() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"monitor","view":"active","watch_ms":-1}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn monitor_command_accepts_zero_watch_ms() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"monitor","view":"active","watch_ms":0}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);

    Ok(())
}

#[test]
fn history_command_rejects_negative_limit() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"history","limit":-1}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn history_command_accepts_zero_limit() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"history","limit":0}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);

    Ok(())
}

#[test]
fn register_command_rejects_zero_count() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"register","count":0,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn register_command_rejects_negative_count() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"register","count":-3,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn register_command_rejects_count_above_maximum() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"register","count":101,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn init_db_command_rejects_negative_seed_agents() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"init-db","seed_agents":-1,"dry":true}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}
