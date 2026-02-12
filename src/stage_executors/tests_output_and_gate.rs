#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use crate::gate_cache::GateExecutionCache;
use crate::skill_execution::SkillOutput;
use crate::types::{BeadId, RepoId, StageResult};
use crate::AgentId;

use super::contract_stage::execute_rust_contract_stage;
use super::gate_stage::run_moon_task;
use super::implement_stage::{append_section, format_retry_packet};
use super::output_mapping::{error_output, failure_output, output_to_stage_result, success_output};

#[test]
fn given_failed_output_when_feedback_present_then_stage_result_uses_feedback() {
    let output = SkillOutput {
        full_log: "full log".to_string(),
        success: false,
        exit_code: Some(1),
        artifacts: std::collections::HashMap::new(),
        feedback: "feedback message".to_string(),
        contract_document: None,
        implementation_code: None,
        modified_files: None,
        test_results: None,
        adversarial_report: None,
    };

    assert_eq!(
        output_to_stage_result(&output),
        StageResult::Failed("feedback message".to_string())
    );
}

#[test]
fn given_successful_output_when_mapping_stage_result_then_result_is_passed() {
    let output = SkillOutput {
        full_log: "ok".to_string(),
        success: true,
        exit_code: Some(0),
        artifacts: std::collections::HashMap::new(),
        feedback: String::new(),
        contract_document: None,
        implementation_code: None,
        modified_files: None,
        test_results: None,
        adversarial_report: None,
    };

    assert_eq!(output_to_stage_result(&output), StageResult::Passed);
}

#[test]
fn given_success_output_builder_when_called_then_output_is_marked_successful() {
    let output = success_output("done".to_string());
    assert!(output.success);
    assert_eq!(output.exit_code, Some(0));
    assert_eq!(output.full_log, "done");
}

#[test]
fn given_failure_output_builder_when_called_then_feedback_matches_full_log() {
    let output = failure_output("failed".to_string());
    assert!(!output.success);
    assert_eq!(output.exit_code, Some(1));
    assert_eq!(output.feedback, "failed");
}

#[test]
fn given_error_output_builder_when_called_then_message_is_preserved() {
    let output = error_output("boom".to_string());
    assert!(!output.success);
    assert_eq!(output.exit_code, Some(1));
    assert_eq!(output.full_log, "boom");
    assert_eq!(output.feedback, "boom");
}

#[test]
fn given_failed_output_when_feedback_missing_then_stage_result_uses_full_log() {
    let output = SkillOutput {
        full_log: "full log fallback".to_string(),
        success: false,
        exit_code: Some(1),
        artifacts: std::collections::HashMap::new(),
        feedback: "   ".to_string(),
        contract_document: None,
        implementation_code: None,
        modified_files: None,
        test_results: None,
        adversarial_report: None,
    };

    assert_eq!(
        output_to_stage_result(&output),
        StageResult::Failed("full log fallback".to_string())
    );
}

#[test]
fn given_failed_output_when_feedback_and_log_missing_then_stage_result_uses_default_message() {
    let output = SkillOutput {
        full_log: "   ".to_string(),
        success: false,
        exit_code: Some(1),
        artifacts: std::collections::HashMap::new(),
        feedback: "   ".to_string(),
        contract_document: None,
        implementation_code: None,
        modified_files: None,
        test_results: None,
        adversarial_report: None,
    };

    assert_eq!(
        output_to_stage_result(&output),
        StageResult::Failed("Stage failed".to_string())
    );
}

#[test]
fn given_json_retry_packet_when_formatted_then_it_is_pretty_printed() {
    let payload = r#"{"attempt":1,"reason":"compile"}"#;
    let formatted = format_retry_packet(payload);
    assert!(formatted.contains('\n'));
    assert!(formatted.contains("\"attempt\": 1"));
}

#[test]
fn given_non_json_retry_packet_when_formatted_then_original_payload_is_preserved() {
    let payload = "not-json";
    assert_eq!(format_retry_packet(payload), payload);
}

#[test]
fn given_optional_context_sections_when_appending_then_only_non_empty_content_is_added() {
    let mut sections = Vec::new();
    append_section(&mut sections, "Retry Packet", Some("  attempt details  "));
    append_section(&mut sections, "Failure", Some("   "));
    append_section(&mut sections, "Missing", None);

    assert_eq!(sections.len(), 1);
    assert_eq!(sections[0], "## Retry Packet\nattempt details");
}

#[test]
fn given_contract_stage_when_executed_then_contract_document_and_artifacts_are_emitted() {
    let bead_id = BeadId::new("bead-123");
    let agent_id = AgentId::new(RepoId::new("local"), 7);

    let output = execute_rust_contract_stage(&bead_id, &agent_id);

    assert!(output.success);
    assert!(output.contract_document.is_some());
    assert!(!output.artifacts.is_empty());
    assert!(output.full_log.contains(bead_id.value()));
}

#[tokio::test]
async fn given_cached_gate_result_when_running_moon_task_then_cached_output_is_returned() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");

    cache
        .put(
            ":quick".to_string(),
            true,
            Some(0),
            "cached stdout".to_string(),
            "".to_string(),
        )
        .await
        .expect("cache write");

    let output = run_moon_task(":quick", Some(&cache))
        .await
        .expect("cached command output");

    assert!(output.success);
    assert_eq!(output.full_log, "cached stdout");
}
