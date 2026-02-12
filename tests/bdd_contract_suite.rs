mod support;

use serde_json::Value;
use support::contract_harness::{
    assert_contract_test_is_decoupled, assert_protocol_envelope, ProtocolScenarioHarness,
};

#[derive(Debug, Clone)]
struct LifecycleScenario {
    id: &'static str,
    given: &'static str,
    when: &'static str,
    then: &'static str,
    public_commands: Vec<&'static str>,
}

fn swarm_lifecycle_scenarios() -> Vec<LifecycleScenario> {
    vec![
        LifecycleScenario {
            id: "swarm-lifecycle-happy-path",
            given: "Given a registered swarm with available agents and ready backlog work",
            when: "When one agent run is triggered through the protocol command interface",
            then: "Then monitor progress reports work completion and status reflects one additional done unit",
            public_commands: vec!["register", "agent", "monitor", "status"],
        },
        LifecycleScenario {
            id: "swarm-lifecycle-retry-loop",
            given: "Given a bead execution fails with retryable diagnostics before attempt budget is exhausted",
            when: "When operators inspect monitor failures and run the next recommended agent command",
            then: "Then a retry transition is observable through public failure/resume views and execution continues without terminal closure",
            public_commands: vec!["agent", "monitor", "resume", "status"],
        },
        LifecycleScenario {
            id: "swarm-lifecycle-blocked-terminal-state",
            given: "Given repeated implementation failures consume the configured attempt budget",
            when: "When another failing run is processed through the same public command path",
            then: "Then monitor failures expose a non-retryable blocked terminal outcome and status errors increase",
            public_commands: vec!["agent", "monitor", "status", "resume"],
        },
        LifecycleScenario {
            id: "swarm-lifecycle-crash-resume",
            given: "Given an agent crashes after persisting stage events but before finishing the lifecycle",
            when: "When resume projections are requested and a replacement agent is invoked",
            then: "Then resume returns actionable context and the replacement agent continues from persisted state instead of restarting blindly",
            public_commands: vec!["resume", "monitor", "agent", "status"],
        },
    ]
}

fn scenario_by_id<'a>(
    scenarios: &'a [LifecycleScenario],
    scenario_id: &'static str,
) -> Result<&'a LifecycleScenario, String> {
    scenarios
        .iter()
        .find(|scenario| scenario.id == scenario_id)
        .ok_or_else(|| format!("missing required lifecycle scenario: {scenario_id}"))
}

#[test]
fn given_lifecycle_suite_when_loaded_then_contains_required_swarm_scenarios() -> Result<(), String>
{
    let scenarios = swarm_lifecycle_scenarios();

    [
        "swarm-lifecycle-happy-path",
        "swarm-lifecycle-retry-loop",
        "swarm-lifecycle-blocked-terminal-state",
        "swarm-lifecycle-crash-resume",
    ]
    .iter()
    .try_for_each(|scenario_id| scenario_by_id(&scenarios, scenario_id).map(|_| ()))
}

#[test]
fn given_lifecycle_suite_when_reviewed_then_scenarios_are_public_interface_only(
) -> Result<(), String> {
    let scenarios = swarm_lifecycle_scenarios();
    let allowed = [
        "init", "register", "agent", "monitor", "status", "resume", "release", "batch",
    ];

    scenarios.iter().try_for_each(|scenario| {
        scenario.public_commands.iter().try_for_each(|command| {
            if allowed.contains(command) {
                Ok(())
            } else {
                Err(format!(
                    "scenario {} references non-public lifecycle command: {command}",
                    scenario.id
                ))
            }
        })
    })
}

#[test]
fn given_retry_and_blocked_scenarios_when_authored_then_they_describe_expected_outcomes(
) -> Result<(), String> {
    let scenarios = swarm_lifecycle_scenarios();

    let retry = scenario_by_id(&scenarios, "swarm-lifecycle-retry-loop")?;
    if !retry.then.contains("retry") {
        return Err(format!(
            "retry scenario should capture retry outcome in Then clause: {}",
            retry.then
        ));
    }

    let blocked = scenario_by_id(&scenarios, "swarm-lifecycle-blocked-terminal-state")?;
    if !blocked.then.contains("blocked") {
        return Err(format!(
            "blocked scenario should capture blocked outcome in Then clause: {}",
            blocked.then
        ));
    }

    Ok(())
}

#[test]
fn given_lifecycle_suite_when_reviewed_then_each_scenario_has_complete_bdd_clauses(
) -> Result<(), String> {
    swarm_lifecycle_scenarios().iter().try_for_each(|scenario| {
        if scenario.given.trim().is_empty() {
            return Err(format!(
                "scenario {} has an empty Given clause",
                scenario.id
            ));
        }
        if scenario.when.trim().is_empty() {
            return Err(format!("scenario {} has an empty When clause", scenario.id));
        }
        if scenario.then.trim().is_empty() {
            return Err(format!("scenario {} has an empty Then clause", scenario.id));
        }

        Ok(())
    })
}

#[test]
fn given_help_request_when_processed_then_returns_protocol_contract() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"?","rid":"bdd-help"}"#)?;

    assert_protocol_envelope(&scenario.output)?;
    if scenario.output["ok"] != Value::Bool(true) {
        return Err(format!(
            "expected successful help response, got: {}",
            scenario.output
        ));
    }
    if scenario.output["rid"] != Value::String("bdd-help".to_string()) {
        return Err(format!(
            "expected response rid to echo request rid, got: {}",
            scenario.output
        ));
    }
    if !scenario.output["d"].is_object() {
        return Err(format!(
            "expected help payload object in d, got: {}",
            scenario.output
        ));
    }
    if !scenario.output["state"].is_object() {
        return Err(format!(
            "expected state object in help response, got: {}",
            scenario.output
        ));
    }

    Ok(())
}

#[test]
fn given_unknown_command_when_processed_then_returns_actionable_error_contract(
) -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"not-a-command"}"#)?;

    assert_protocol_envelope(&scenario.output)?;
    if scenario.output["ok"] != Value::Bool(false) {
        return Err(format!(
            "expected failed response for invalid command, got: {}",
            scenario.output
        ));
    }
    if scenario.output["err"]["code"] != Value::String("INVALID".to_string()) {
        return Err(format!(
            "expected INVALID error code, got: {}",
            scenario.output
        ));
    }
    if !scenario.output["fix"].is_string() {
        return Err(format!(
            "expected actionable fix guidance for invalid command, got: {}",
            scenario.output
        ));
    }

    Ok(())
}

#[test]
fn given_dry_lock_request_when_processed_then_returns_non_mutating_plan_contract(
) -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"lock","resource":"res_bdd","agent":"agent-bdd","ttl_ms":30000,"dry":true}"#,
    )?;

    assert_protocol_envelope(&scenario.output)?;
    if scenario.output["ok"] != Value::Bool(true) {
        return Err(format!(
            "expected dry lock scenario success, got: {}",
            scenario.output
        ));
    }
    if scenario.output["d"]["dry"] != Value::Bool(true) {
        return Err(format!(
            "expected dry marker in response, got: {}",
            scenario.output
        ));
    }
    if !scenario.output["d"]["would_do"].is_array() {
        return Err(format!(
            "expected non-mutating execution plan array, got: {}",
            scenario.output
        ));
    }

    Ok(())
}

#[test]
fn given_mixed_batch_when_processed_then_reports_pass_and_fail_contract() -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(
        r#"{"cmd":"batch","ops":[{"cmd":"?"},{"cmd":"invalid-batch-op"}],"dry":false}"#,
    )?;

    assert_protocol_envelope(&scenario.output)?;
    if scenario.output["ok"] != Value::Bool(true) {
        return Err(format!(
            "expected batch command to return summary envelope, got: {}",
            scenario.output
        ));
    }
    if scenario.output["d"]["summary"]["total"] != 2 {
        return Err(format!(
            "expected summary total=2 for mixed batch, got: {}",
            scenario.output
        ));
    }
    if scenario.output["d"]["summary"]["pass"] != 1 {
        return Err(format!(
            "expected summary pass=1 for mixed batch, got: {}",
            scenario.output
        ));
    }
    if scenario.output["d"]["summary"]["fail"] != 1 {
        return Err(format!(
            "expected summary fail=1 for mixed batch, got: {}",
            scenario.output
        ));
    }

    Ok(())
}

#[test]
fn given_contract_suite_when_reviewed_then_assertions_remain_decoupled_from_internals(
) -> Result<(), String> {
    ["tests/bdd_contract_suite.rs", "tests/cli_e2e.rs"]
        .iter()
        .try_for_each(|relative_path| assert_contract_test_is_decoupled(relative_path))
}

#[test]
fn given_ai_runs_init_db_outside_repo_root_when_schema_not_provided_then_embedded_schema_is_used(
) -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"init-db","dry":true}"#)?;

    assert_protocol_envelope(&scenario.output)?;
    if scenario.output["ok"] != Value::Bool(true) {
        return Err(format!(
            "expected dry init-db success envelope, got: {}",
            scenario.output
        ));
    }

    let steps = scenario.output["d"]["would_do"]
        .as_array()
        .ok_or_else(|| "expected would_do array in init-db dry response".to_string())?;
    let apply_schema = steps
        .iter()
        .find(|step| step["action"] == "apply_schema")
        .ok_or_else(|| "expected apply_schema step in init-db dry response".to_string())?;
    let target = apply_schema["target"]
        .as_str()
        .ok_or_else(|| "expected apply_schema target to be a string".to_string())?;

    if !target.starts_with("embedded:") {
        return Err(format!(
            "expected embedded schema target for AI-safe init-db, got: {target}"
        ));
    }

    Ok(())
}

#[test]
fn given_run_once_payload_with_unknown_field_when_processed_then_command_rejects_it_with_actionable_contract(
) -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"run-once","agent_id":7,"dry":true}"#)?;

    assert_protocol_envelope(&scenario.output)?;
    if scenario.output["ok"] != Value::Bool(false) {
        return Err(format!(
            "expected unknown field to fail validation, got: {}",
            scenario.output
        ));
    }
    if scenario.output["err"]["code"] != Value::String("INVALID".to_string()) {
        return Err(format!(
            "expected INVALID code for unknown field, got: {}",
            scenario.output
        ));
    }
    let message = scenario.output["err"]["msg"]
        .as_str()
        .ok_or_else(|| format!("expected error message string, got: {}", scenario.output))?;
    if !message.contains("Unknown field(s) for run-once") {
        return Err(format!(
            "expected unknown field message for run-once, got: {}",
            scenario.output
        ));
    }

    Ok(())
}

#[test]
fn given_agent_request_with_string_id_when_processed_then_response_reports_explicit_type_mismatch(
) -> Result<(), String> {
    let harness = ProtocolScenarioHarness::new();
    let scenario = harness.run_protocol(r#"{"cmd":"agent","id":"abc","dry":true}"#)?;

    assert_protocol_envelope(&scenario.output)?;
    if scenario.output["ok"] != Value::Bool(false) {
        return Err(format!(
            "expected agent request with string id to fail, got: {}",
            scenario.output
        ));
    }
    if scenario.output["err"]["code"] != Value::String("INVALID".to_string()) {
        return Err(format!(
            "expected INVALID code for id type mismatch, got: {}",
            scenario.output
        ));
    }
    let message = scenario.output["err"]["msg"]
        .as_str()
        .ok_or_else(|| format!("expected error message string, got: {}", scenario.output))?;
    if !message.contains("Invalid type for field id") {
        return Err(format!(
            "expected explicit id type mismatch message, got: {}",
            scenario.output
        ));
    }

    Ok(())
}
