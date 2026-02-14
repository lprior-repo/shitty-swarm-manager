#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::helpers::{build_failure_diagnostics, landing_retry_causation_id, redact_sensitive};
use super::types::{ExecutionEventWriteInput, FailureDiagnosticsPayload};
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, ArtifactType, BeadId, Stage};
use crate::BrSyncStatus;
use chrono::Utc;
use serde_json::json;
use sqlx::Acquire;
use std::collections::HashSet;

const CONTEXT_ARTIFACT_TYPES: [ArtifactType; 3] = [
    ArtifactType::ImplementationCode,
    ArtifactType::TestResults,
    ArtifactType::TestOutput,
];

impl SwarmDb {
    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn persist_retry_packet(
        &self,
        stage_history_id: Option<i64>,
        stage: Stage,
        attempt: u32,
        bead_id: &BeadId,
        agent_id: &AgentId,
        message: Option<&str>,
    ) -> Result<()> {
        let Some(stage_history_id) = stage_history_id else {
            return Ok(());
        };

        let config = self.get_config(agent_id.repo_id()).await?;
        let max_attempts = config.max_implementation_attempts;
        let remaining_attempts = max_attempts.saturating_sub(attempt);

        let FailureDiagnosticsPayload {
            category: failure_category,
            retryable,
            next_command,
            detail: failure_detail,
        } = build_failure_diagnostics(message);

        let mut artifact_refs = Vec::new();
        let mut seen_ids = HashSet::new();
        let mut seen_types = HashSet::new();

        let stage_artifacts = self
            .get_stage_artifacts(agent_id.repo_id(), stage_history_id)
            .await?;
        for artifact in stage_artifacts {
            let artifact_type_name = artifact.artifact_type.as_str().to_string();
            if seen_ids.insert(artifact.id) {
                seen_types.insert(artifact_type_name.clone());
                artifact_refs.push(json!({
                    "artifact_id": artifact.id,
                    "artifact_type": artifact.artifact_type.as_str(),
                    "content_hash": artifact.content_hash,
                    "metadata": artifact.metadata,
                    "created_at": artifact.created_at.to_rfc3339(),
                    "stage_history_id": artifact.stage_history_id,
                    "context": "current_stage",
                }));
            }
        }

        for artifact_type in CONTEXT_ARTIFACT_TYPES {
            let artifact_type_name = artifact_type.as_str().to_string();
            if seen_types.contains(&artifact_type_name) {
                continue;
            }

            let artifacts = self
                .get_bead_artifacts_by_type(agent_id.repo_id(), bead_id, artifact_type)
                .await?;
            if let Some(artifact) = artifacts.last() {
                if seen_ids.insert(artifact.id) {
                    seen_types.insert(artifact_type_name.clone());
                    artifact_refs.push(json!({
                        "artifact_id": artifact.id,
                        "artifact_type": artifact.artifact_type.as_str(),
                        "content_hash": artifact.content_hash,
                        "metadata": artifact.metadata,
                        "created_at": artifact.created_at.to_rfc3339(),
                        "stage_history_id": artifact.stage_history_id,
                        "context": "latest_per_type",
                    }));
                    continue;
                }
            }

            seen_types.insert(artifact_type_name.clone());
            artifact_refs.push(json!({
                "artifact_type": artifact_type.as_str(),
                "missing": true,
                "context": "latest_per_type",
            }));
        }

        let retry_packet = json!({
            "bead_id": bead_id.value(),
            "agent_id": agent_id.number(),
            "stage": stage.as_str(),
            "stage_history_id": stage_history_id,
            "attempt": attempt,
            "max_attempts": max_attempts,
            "remaining_attempts": remaining_attempts,
            "failure_category": failure_category,
            "failure_detail": failure_detail,
            "failure_message": message.map(redact_sensitive),
            "retryable": retryable,
            "next_command": next_command,
            "artifact_refs": artifact_refs,
            "created_at": Utc::now().to_rfc3339(),
        });

        self.store_stage_artifact(
            stage_history_id,
            ArtifactType::RetryPacket,
            &retry_packet.to_string(),
            Some(json!({
                "stage": stage.as_str(),
                "attempt": attempt,
                "failure_category": failure_category,
            })),
        )
        .await
        .map(|_| ())
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn mark_landing_retryable(&self, agent_id: &AgentId, reason: &str) -> Result<()> {
        let bead_id = {
            let mut tx = self
                .pool()
                .begin()
                .await
                .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

            let conn = tx.acquire().await.map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}"))
            })?;

            sqlx::query(
                "UPDATE agent_state
                 SET status = 'waiting', feedback = $3, current_stage = 'red-queen'
                 WHERE repo_id = $1 AND agent_id = $2",
            )
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .bind(reason)
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to mark landing retryable: {e}"))
            })?;

            let bead_id = sqlx::query_scalar::<_, Option<String>>(
                "SELECT bead_id FROM agent_state WHERE repo_id = $1 AND agent_id = $2",
            )
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to lookup bead in tx: {e}")))?
            .flatten();

            tx.commit()
                .await
                .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))?;

            bead_id
        };

        if let Some(bead_id) = bead_id {
            let bead_id = BeadId::new(bead_id);
            let causation_id = Some(landing_retry_causation_id(reason));
            self.record_execution_event_if_absent(
                &bead_id,
                agent_id,
                ExecutionEventWriteInput {
                    stage: Some(Stage::RedQueen),
                    event_type: "transition_retry",
                    causation_id,
                    payload: json!({"transition": "retry", "next_stage": Stage::RedQueen.as_str()}),
                    diagnostics: Some(FailureDiagnosticsPayload {
                        category: "landing_failure".to_string(),
                        retryable: true,
                        next_command: "swarm monitor --view failures".to_string(),
                        detail: Some(redact_sensitive(reason)),
                    }),
                },
            )
            .await?;
            self.record_landing_sync_outcome_if_absent(
                &bead_id,
                agent_id,
                BrSyncStatus::RetryScheduled,
                Some(reason),
            )
            .await?;
        }

        Ok(())
    }
}
