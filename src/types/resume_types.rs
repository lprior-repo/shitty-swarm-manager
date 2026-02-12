#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::agent_types::AgentStatus;
use super::artifacts::ArtifactType;
use super::identifiers::BeadId;
use super::observability::FailureDiagnostics;
use super::stage::Stage;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeContextProjection {
    pub agent_id: u32,
    pub bead_id: BeadId,
    pub status: AgentStatus,
    pub current_stage: Option<Stage>,
    pub implementation_attempt: u32,
    pub feedback: Option<String>,
    pub attempts: Vec<ResumeStageAttempt>,
    pub artifacts: Vec<ResumeArtifactSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeStageAttempt {
    pub stage: Stage,
    pub attempt_number: u32,
    pub status: String,
    pub feedback: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeArtifactSummary {
    pub artifact_type: ArtifactType,
    pub created_at: DateTime<Utc>,
    pub content_hash: Option<String>,
    pub byte_length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeContextContract {
    pub agent_id: u32,
    pub bead_id: String,
    pub status: String,
    pub current_stage: Option<String>,
    pub implementation_attempt: u32,
    pub feedback: Option<String>,
    pub latest_attempt: Option<ResumeStageAttemptContract>,
    pub artifacts: Vec<ResumeArtifactSummaryContract>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeArtifactDetailContract {
    pub artifact_type: String,
    pub created_at: DateTime<Utc>,
    pub content: String,
    pub metadata: Option<Value>,
    pub content_hash: Option<String>,
    pub byte_length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeepResumeContextContract {
    pub agent_id: u32,
    pub bead_id: String,
    pub status: String,
    pub current_stage: Option<String>,
    pub implementation_attempt: u32,
    pub feedback: Option<String>,
    pub attempts: Vec<ResumeStageAttemptContract>,
    pub diagnostics: Option<FailureDiagnostics>,
    pub artifacts: Vec<ResumeArtifactDetailContract>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeStageAttemptContract {
    pub stage: String,
    pub attempt_number: u32,
    pub status: String,
    pub feedback: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeArtifactSummaryContract {
    pub artifact_type: String,
    pub created_at: DateTime<Utc>,
    pub content_hash: Option<String>,
    pub byte_length: u64,
}

impl ResumeContextContract {
    #[must_use]
    pub fn from_projection(projection: &ResumeContextProjection) -> Self {
        let latest_attempt = projection
            .attempts
            .last()
            .map(|attempt| ResumeStageAttemptContract {
                stage: attempt.stage.as_str().to_string(),
                attempt_number: attempt.attempt_number,
                status: attempt.status.clone(),
                feedback: attempt.feedback.clone(),
                started_at: attempt.started_at,
                completed_at: attempt.completed_at,
            });

        let artifacts = projection
            .artifacts
            .iter()
            .map(|artifact| ResumeArtifactSummaryContract {
                artifact_type: artifact.artifact_type.as_str().to_string(),
                created_at: artifact.created_at,
                content_hash: artifact.content_hash.clone(),
                byte_length: artifact.byte_length,
            })
            .collect::<Vec<_>>();

        Self {
            agent_id: projection.agent_id,
            bead_id: projection.bead_id.value().to_string(),
            status: projection.status.as_str().to_string(),
            current_stage: projection
                .current_stage
                .map(|stage| stage.as_str().to_string()),
            implementation_attempt: projection.implementation_attempt,
            feedback: projection.feedback.clone(),
            latest_attempt,
            artifacts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BeadId;

    fn sample_projection() -> ResumeContextProjection {
        let now = Utc::now();
        ResumeContextProjection {
            agent_id: 7,
            bead_id: BeadId::new("swm-o5s.1"),
            status: AgentStatus::Working,
            current_stage: Some(Stage::Implement),
            implementation_attempt: 2,
            feedback: Some("rerun with focused fix".to_string()),
            attempts: vec![ResumeStageAttempt {
                stage: Stage::Implement,
                attempt_number: 2,
                status: "failed".to_string(),
                feedback: Some("missing edge case".to_string()),
                started_at: now,
                completed_at: Some(now),
            }],
            artifacts: vec![ResumeArtifactSummary {
                artifact_type: ArtifactType::FailureDetails,
                created_at: now,
                content_hash: Some("abc123".to_string()),
                byte_length: 42,
            }],
        }
    }

    #[test]
    fn resume_context_contract_from_projection_exposes_stable_minimal_payload() {
        let projection = sample_projection();
        let contract = ResumeContextContract::from_projection(&projection);

        let payload_result = serde_json::to_value(contract);
        assert!(payload_result.is_ok());
        let payload = match payload_result {
            Ok(value) => value,
            Err(_) => serde_json::Value::Null,
        };

        assert_eq!(payload["agent_id"], serde_json::Value::from(7));
        assert_eq!(payload["bead_id"], serde_json::Value::from("swm-o5s.1"));
        assert_eq!(payload["status"], serde_json::Value::from("working"));
        assert_eq!(
            payload["current_stage"],
            serde_json::Value::from("implement")
        );
        assert_eq!(
            payload["latest_attempt"]["attempt_number"],
            serde_json::Value::from(2)
        );
        assert_eq!(
            payload["artifacts"][0]["artifact_type"],
            serde_json::Value::from("failure_details")
        );
    }

    #[test]
    fn given_persisted_stage_artifacts_when_replacement_agent_runs_then_pipeline_continues_from_persisted_state(
    ) {
        let now = Utc::now();
        let projection = ResumeContextProjection {
            agent_id: 42,
            bead_id: BeadId::new("swm-fbc"),
            status: AgentStatus::Waiting,
            current_stage: Some(Stage::Implement),
            implementation_attempt: 3,
            feedback: Some("continue from persisted state".to_string()),
            attempts: vec![
                ResumeStageAttempt {
                    stage: Stage::RustContract,
                    attempt_number: 1,
                    status: "passed".to_string(),
                    feedback: None,
                    started_at: now,
                    completed_at: Some(now),
                },
                ResumeStageAttempt {
                    stage: Stage::Implement,
                    attempt_number: 3,
                    status: "passed".to_string(),
                    feedback: Some("implementation ready".to_string()),
                    started_at: now,
                    completed_at: Some(now),
                },
            ],
            artifacts: vec![
                ResumeArtifactSummary {
                    artifact_type: ArtifactType::ContractDocument,
                    created_at: now,
                    content_hash: Some("hash-a".to_string()),
                    byte_length: 128,
                },
                ResumeArtifactSummary {
                    artifact_type: ArtifactType::FailureDetails,
                    created_at: now,
                    content_hash: Some("hash-b".to_string()),
                    byte_length: 64,
                },
            ],
        };

        let contract = ResumeContextContract::from_projection(&projection);
        let latest_attempt = contract.latest_attempt.expect("latest attempt");
        assert_eq!(latest_attempt.attempt_number, 3);
        assert_eq!(latest_attempt.stage, "implement");
        assert_eq!(latest_attempt.status, "passed");
        assert!(contract
            .artifacts
            .iter()
            .any(|artifact| artifact.artifact_type == "failure_details"));
        assert_eq!(
            contract.feedback.as_deref(),
            Some("continue from persisted state")
        );
    }

    #[test]
    fn given_resume_context_request_after_simulated_crash_then_payload_contains_actionable_continuation_data(
    ) {
        let now = Utc::now();
        let projection = ResumeContextProjection {
            agent_id: 33,
            bead_id: BeadId::new("swm-fbc-crash"),
            status: AgentStatus::Error,
            current_stage: Some(Stage::Implement),
            implementation_attempt: 2,
            feedback: Some("resume with fresh agent".to_string()),
            attempts: vec![ResumeStageAttempt {
                stage: Stage::Implement,
                attempt_number: 2,
                status: "failed".to_string(),
                feedback: Some("missing artifact".to_string()),
                started_at: now,
                completed_at: Some(now),
            }],
            artifacts: vec![ResumeArtifactSummary {
                artifact_type: ArtifactType::ErrorMessage,
                created_at: now,
                content_hash: None,
                byte_length: 34,
            }],
        };

        let contract = ResumeContextContract::from_projection(&projection);
        let latest_attempt = contract
            .latest_attempt
            .expect("latest attempt should exist");
        assert_eq!(contract.status, "error");
        assert_eq!(latest_attempt.status, "failed");
        assert!(contract
            .artifacts
            .iter()
            .any(|artifact| artifact.artifact_type == "error_message"));
        assert_eq!(
            contract.feedback.as_deref(),
            Some("resume with fresh agent")
        );
    }

    #[test]
    fn given_missing_required_artifacts_after_crash_when_replacement_agent_runs_then_system_surfaces_explicit_failure_and_next_action(
    ) {
        let now = Utc::now();
        let projection = ResumeContextProjection {
            agent_id: 77,
            bead_id: BeadId::new("swm-fbc-empty"),
            status: AgentStatus::Error,
            current_stage: Some(Stage::QaEnforcer),
            implementation_attempt: 5,
            feedback: Some("missing artifacts, contact monitor".to_string()),
            attempts: vec![ResumeStageAttempt {
                stage: Stage::QaEnforcer,
                attempt_number: 5,
                status: "error".to_string(),
                feedback: Some("artifact not found".to_string()),
                started_at: now,
                completed_at: Some(now),
            }],
            artifacts: Vec::new(),
        };

        let contract = ResumeContextContract::from_projection(&projection);
        assert!(contract.artifacts.is_empty());
        assert_eq!(contract.status, "error");
        assert!(contract
            .feedback
            .as_deref()
            .map_or(false, |f| f.contains("missing artifacts")));
    }
}
