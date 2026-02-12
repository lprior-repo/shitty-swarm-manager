use std::fs;
use std::path::Path;

use crate::support::contract_harness::{assert_protocol_envelope, ProtocolScenarioHarness};

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
fn prompt_command_rejects_negative_id() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario =
        harness.run_protocol(r#"{"cmd":"prompt","id":-1,"rid":"prompt-negative-id"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn prompt_command_rejects_zero_id() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"prompt","id":0,"rid":"prompt-zero-id"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

    Ok(())
}

#[test]
fn resume_context_command_exposes_context_payload() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"resume-context"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert!(json["d"]["contexts"].is_array());

    Ok(())
}

#[test]
fn resume_context_with_unknown_bead_returns_notfound() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"resume-context","bead_id":"swm-unknown"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "NOTFOUND");

    Ok(())
}

#[test]
fn resume_context_rejects_empty_bead_id() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"resume-context","bead_id":""}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");

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
