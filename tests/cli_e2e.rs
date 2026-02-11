mod support;
use std::fs;
use std::path::Path;

use support::contract_harness::{
    assert_contract_test_is_decoupled, assert_protocol_envelope, ProtocolScenarioHarness,
};
use swarm::CANONICAL_COORDINATOR_SCHEMA_PATH;

#[test]
fn help_command_returns_protocol_envelope() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"?","rid":"r-1"}"#)?;
    let json = scenario.output;

    assert_protocol_envelope(&json)?;
    assert_eq!(json["ok"], true);
    assert_eq!(json["rid"], "r-1");
    assert!(json["t"].is_i64());
    assert!(json["ms"].is_i64());
    assert!(json["d"]["commands"].is_object());
    assert!(json["state"]["total"].is_number());

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

    if target != CANONICAL_COORDINATOR_SCHEMA_PATH {
        return Err(format!(
            "unexpected schema target {target}, expected canonical path {CANONICAL_COORDINATOR_SCHEMA_PATH}",
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
