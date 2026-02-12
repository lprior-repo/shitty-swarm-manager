#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::types::{FailureDiagnosticsPayload, StageTransition};
use crate::ddd::{
    runtime_determine_transition_decision, RuntimeStage, RuntimeStageResult, RuntimeStageTransition,
};
use crate::types::{BeadId, RepoId, Stage, StageResult};
use crate::BrSyncStatus;

pub(crate) fn build_failure_diagnostics(message: Option<&str>) -> FailureDiagnosticsPayload {
    let detail = message
        .map(redact_sensitive)
        .filter(|value| !value.trim().is_empty());
    FailureDiagnosticsPayload {
        category: message.map_or_else(
            || "stage_failure".to_string(),
            |value| classify_failure_category(value).to_string(),
        ),
        retryable: true,
        next_command: "swarm stage --stage implement".to_string(),
        detail,
    }
}

pub(crate) fn classify_failure_category(message: &str) -> &'static str {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("timeout") {
        "timeout"
    } else if lowered.contains("syntax") || lowered.contains("compile") {
        "compile_error"
    } else if lowered.contains("test") || lowered.contains("assert") {
        "test_failure"
    } else {
        "stage_failure"
    }
}

pub(crate) fn redact_sensitive(message: &str) -> String {
    message
        .split_whitespace()
        .map(redact_token)
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn landing_retry_causation_id(reason: &str) -> String {
    format!(
        "landing-sync:retry:{}",
        reason
            .trim()
            .to_ascii_lowercase()
            .replace(char::is_whitespace, "-")
    )
}

pub(crate) const fn landing_sync_status_key(status: BrSyncStatus) -> &'static str {
    match status {
        BrSyncStatus::Synchronized => "synchronized",
        BrSyncStatus::RetryScheduled => "retry_scheduled",
        BrSyncStatus::Diverged => "diverged",
    }
}

pub(crate) fn landing_sync_causation_id(status: BrSyncStatus, reason: Option<&str>) -> String {
    match reason {
        Some(reason)
            if matches!(
                status,
                BrSyncStatus::RetryScheduled | BrSyncStatus::Diverged
            ) =>
        {
            format!(
                "landing-sync:{}:{}",
                landing_sync_status_key(status),
                reason
                    .trim()
                    .to_ascii_lowercase()
                    .replace(char::is_whitespace, "-")
            )
        }
        _ => format!("landing-sync:{}", landing_sync_status_key(status)),
    }
}

fn redact_token(token: &str) -> String {
    token.split_once('=').map_or_else(
        || token.to_string(),
        |(key, _)| {
            let normalized = key.to_ascii_lowercase();
            if ["token", "password", "secret", "api_key", "database_url"]
                .iter()
                .any(|sensitive| normalized.contains(sensitive))
            {
                format!("{key}=<redacted>")
            } else {
                token.to_string()
            }
        },
    )
}

pub(crate) fn event_entity_id(bead_id: &BeadId, repo_id: &RepoId) -> String {
    format!("repo:{}:bead:{}", repo_id.value(), bead_id.value())
}

#[must_use]
pub fn determine_transition(stage: Stage, result: &StageResult) -> StageTransition {
    let decision = runtime_determine_transition_decision(
        to_runtime_stage(stage),
        &to_runtime_stage_result(result),
        0,
        1,
    );

    match decision.transition() {
        RuntimeStageTransition::Advance(next_stage) => {
            StageTransition::Advance(to_stage(next_stage))
        }
        RuntimeStageTransition::Retry => StageTransition::RetryImplement,
        RuntimeStageTransition::Complete => StageTransition::Finalize,
        RuntimeStageTransition::Block | RuntimeStageTransition::NoOp => StageTransition::NoOp,
    }
}

const fn to_runtime_stage(stage: Stage) -> RuntimeStage {
    match stage {
        Stage::RustContract => RuntimeStage::RustContract,
        Stage::Implement => RuntimeStage::Implement,
        Stage::QaEnforcer => RuntimeStage::QaEnforcer,
        Stage::RedQueen => RuntimeStage::RedQueen,
        Stage::Done => RuntimeStage::Done,
    }
}

fn to_runtime_stage_result(result: &StageResult) -> RuntimeStageResult {
    match result {
        StageResult::Started => RuntimeStageResult::Started,
        StageResult::Passed => RuntimeStageResult::Passed,
        StageResult::Failed(message) => RuntimeStageResult::Failed(message.clone()),
        StageResult::Error(message) => RuntimeStageResult::Error(message.clone()),
    }
}

fn to_stage(runtime_stage: RuntimeStage) -> Stage {
    match runtime_stage {
        RuntimeStage::RustContract => Stage::RustContract,
        RuntimeStage::Implement => Stage::Implement,
        RuntimeStage::QaEnforcer => Stage::QaEnforcer,
        RuntimeStage::RedQueen => Stage::RedQueen,
        RuntimeStage::Done => Stage::Done,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn landing_retry_causation_id_is_stable() {
        assert_eq!(
            landing_retry_causation_id("  JJ push FAILED with timeout  "),
            "landing-sync:retry:jj-push-failed-with-timeout"
        );
    }

    #[test]
    fn landing_sync_causation_id_includes_reason_for_retryable_states() {
        assert_eq!(
            landing_sync_causation_id(BrSyncStatus::RetryScheduled, Some("transport timeout")),
            "landing-sync:retry_scheduled:transport-timeout"
        );
        assert_eq!(
            landing_sync_causation_id(BrSyncStatus::Synchronized, Some("ignored")),
            "landing-sync:synchronized"
        );
    }

    #[test]
    fn given_diverged_status_when_building_sync_causation_id_then_reason_is_normalized() {
        assert_eq!(
            landing_sync_causation_id(BrSyncStatus::Diverged, Some("  JJ Push Rejected  ")),
            "landing-sync:diverged:jj-push-rejected"
        );
    }

    #[test]
    fn given_each_sync_status_when_mapping_status_key_then_expected_key_is_returned() {
        assert_eq!(
            landing_sync_status_key(BrSyncStatus::Synchronized),
            "synchronized"
        );
        assert_eq!(
            landing_sync_status_key(BrSyncStatus::RetryScheduled),
            "retry_scheduled"
        );
        assert_eq!(landing_sync_status_key(BrSyncStatus::Diverged), "diverged");
    }

    #[test]
    fn given_timeout_message_when_classifying_failure_then_timeout_category_is_returned() {
        assert_eq!(
            classify_failure_category("command timeout after 30s"),
            "timeout"
        );
    }

    #[test]
    fn given_compile_message_when_classifying_failure_then_compile_error_category_is_returned() {
        assert_eq!(
            classify_failure_category("compile failed with syntax error"),
            "compile_error"
        );
    }

    #[test]
    fn given_mixed_case_timeout_message_when_classifying_failure_then_case_is_ignored() {
        assert_eq!(
            classify_failure_category("Network TIMEOUT while fetching dependencies"),
            "timeout"
        );
    }

    #[test]
    fn given_message_matching_multiple_categories_when_classifying_then_timeout_takes_precedence() {
        assert_eq!(
            classify_failure_category("test suite hit timeout and assert failed"),
            "timeout"
        );
    }

    #[test]
    fn given_assert_message_when_classifying_failure_then_test_failure_category_is_returned() {
        assert_eq!(
            classify_failure_category("assert failed in test suite"),
            "test_failure"
        );
    }

    #[test]
    fn given_sensitive_tokens_when_redacting_then_secret_values_are_removed() {
        let input = "token=abc password=123 ok=value";
        assert_eq!(
            redact_sensitive(input),
            "token=<redacted> password=<redacted> ok=value"
        );
    }

    #[test]
    fn given_mixed_case_sensitive_keys_when_redacting_then_values_are_removed() {
        let input = "API_KEY=topsecret DataBase_Url=postgres://localhost safe=yes";
        assert_eq!(
            redact_sensitive(input),
            "API_KEY=<redacted> DataBase_Url=<redacted> safe=yes"
        );
    }

    #[test]
    fn given_message_without_key_value_tokens_when_redacting_then_message_is_unchanged() {
        let input = "plain text without assignments";
        assert_eq!(redact_sensitive(input), input);
    }

    #[test]
    fn given_failure_message_when_building_diagnostics_then_payload_contains_expected_defaults() {
        let payload = build_failure_diagnostics(Some("test assertion failed"));
        assert_eq!(payload.category, "test_failure");
        assert!(payload.retryable);
        assert_eq!(payload.next_command, "swarm stage --stage implement");
        assert_eq!(payload.detail, Some("test assertion failed".to_string()));
    }

    #[test]
    fn given_whitespace_only_failure_message_when_building_diagnostics_then_detail_is_omitted() {
        let payload = build_failure_diagnostics(Some("   \n\t   "));
        assert_eq!(payload.category, "stage_failure");
        assert!(payload.detail.is_none());
    }

    #[test]
    fn given_missing_failure_message_when_building_diagnostics_then_defaults_are_used() {
        let payload = build_failure_diagnostics(None);
        assert_eq!(payload.category, "stage_failure");
        assert!(payload.retryable);
        assert_eq!(payload.next_command, "swarm stage --stage implement");
        assert!(payload.detail.is_none());
    }

    #[test]
    fn given_repo_and_bead_when_building_event_entity_id_then_id_is_repo_scoped() {
        let repo = RepoId::new("local");
        let bead = BeadId::new("bd-7");
        assert_eq!(event_entity_id(&bead, &repo), "repo:local:bead:bd-7");
    }

    #[test]
    fn given_started_result_when_determining_transition_then_noop_is_returned() {
        assert_eq!(
            determine_transition(Stage::Implement, &StageResult::Started),
            StageTransition::NoOp
        );
    }

    #[test]
    fn given_done_stage_pass_result_when_determining_transition_then_noop_is_returned() {
        assert_eq!(
            determine_transition(Stage::Done, &StageResult::Passed),
            StageTransition::NoOp
        );
    }
}
