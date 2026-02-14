#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::helpers::event_entity_id;
use super::types::ExecutionEventWriteInput;
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, BeadId, EventSchemaVersion, Stage, StageResult};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::Acquire;
use tracing::debug;

impl SwarmDb {
    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn record_stage_started(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
    ) -> Result<i64> {
        self.ensure_stage_history_repo_scope().await?;
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}")))?;

        let stage_history_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO stage_history (repo_id, agent_id, bead_id, stage, attempt_number, status)
             VALUES ($1, $2, $3, $4, $5, 'started')
             RETURNING id",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt.cast_signed())
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to record stage start: {e}")))?;

        sqlx::query(
            "UPDATE agent_state
             SET current_stage = $3, stage_started_at = NOW(), status = 'working'
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(stage.as_str())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update stage start: {e}")))?;

        sqlx::query(
            "INSERT INTO execution_events (
                schema_version,
                event_type,
                entity_id,
                bead_id,
                agent_id,
                stage,
                causation_id,
                payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(EventSchemaVersion::V1.as_i32())
        .bind("stage_started")
        .bind(event_entity_id(bead_id, agent_id.repo_id()))
        .bind(bead_id.value())
        .bind(agent_id.number().cast_signed())
        .bind(stage.as_str())
        .bind(Some(format!("stage-history:{stage_history_id}")))
        .bind(json!({"attempt": attempt, "status": "started"}))
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to write stage start event: {e}"))
        })?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))
            .map(|()| stage_history_id)
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn record_stage_complete(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
        result: StageResult,
        duration_ms: u64,
    ) -> Result<()> {
        let message = result.message();
        let stage_history_id = self
            .record_stage_complete_without_transition(
                agent_id,
                bead_id,
                stage,
                attempt,
                &result,
                duration_ms,
            )
            .await?;

        self.apply_stage_transition(super::types::StageTransitionInput {
            transition: &super::helpers::determine_transition(stage, &result),
            agent_id,
            bead_id,
            stage,
            stage_history_id: Some(stage_history_id),
            attempt,
            message,
        })
        .await?;

        debug!(
            "Agent {} completed stage {} for bead {}: {:?}",
            agent_id, stage, bead_id, result
        );
        Ok(())
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn record_stage_complete_without_transition(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
        result: &StageResult,
        duration_ms: u64,
    ) -> Result<i64> {
        self.ensure_stage_history_repo_scope().await?;
        let status = result.as_str();
        let message = result.message();

        let duration_value = i32::try_from(duration_ms).map_err(|_| {
            SwarmError::DatabaseError("Duration overflow updating stage history".to_string())
        })?;

        let stage_history_row = sqlx::query_as::<_, (i64, Option<DateTime<Utc>>)>(
            "UPDATE stage_history
             SET status = $6, result = $7, feedback = $8, completed_at = NOW(), duration_ms = $9
             WHERE id = (
                 SELECT id FROM stage_history
                 WHERE repo_id = $1
                   AND agent_id = $2
                   AND bead_id = $3
                   AND stage = $4
                   AND attempt_number = $5
                   AND status = 'started'
                 ORDER BY started_at DESC LIMIT 1
             )
             RETURNING id, completed_at",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt.cast_signed())
        .bind(&status)
        .bind(message)
        .bind(message)
        .bind(duration_value)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update stage history: {e}")))?
        .ok_or_else(|| {
            SwarmError::DatabaseError(
                "Failed to locate active stage history row for completion update".to_string(),
            )
        })?;

        let completed_at = stage_history_row.1.ok_or_else(|| {
            SwarmError::DatabaseError("Failed to capture stage completion timestamp".to_string())
        })?;

        let stage_history_id = stage_history_row.0;

        self.persist_stage_transcript(
            agent_id,
            stage_history_id,
            stage,
            attempt,
            result,
            completed_at,
        )
        .await?;

        self.record_execution_event(
            bead_id,
            agent_id,
            ExecutionEventWriteInput {
                stage: Some(stage),
                event_type: "stage_completed",
                causation_id: Some(format!("stage-history:{stage_history_id}")),
                payload: json!({
                    "attempt": attempt,
                    "status": status,
                    "result": message,
                    "duration_ms": duration_ms,
                }),
                diagnostics: None,
            },
        )
        .await
        .map(|()| stage_history_id)
    }

    async fn persist_stage_transcript(
        &self,
        agent_id: &AgentId,
        stage_history_id: i64,
        stage: Stage,
        attempt: u32,
        result: &StageResult,
        completed_at: DateTime<Utc>,
    ) -> Result<()> {
        let artifacts = self
            .get_stage_artifacts(agent_id.repo_id(), stage_history_id)
            .await?;
        let mut sorted_artifacts = artifacts.clone();
        sorted_artifacts.sort_by(|a, b| {
            a.artifact_type
                .as_str()
                .cmp(b.artifact_type.as_str())
                .then_with(|| a.id.cmp(&b.id))
        });

        let artifact_types = sorted_artifacts
            .iter()
            .map(|artifact| artifact.artifact_type.as_str().to_string())
            .collect::<Vec<_>>();

        let artifact_refs = sorted_artifacts
            .iter()
            .map(|artifact| {
                json!({
                    "id": artifact.id,
                    "artifact_type": artifact.artifact_type.as_str(),
                    "content_hash": artifact.content_hash,
                    "created_at": artifact.created_at.to_rfc3339(),
                })
            })
            .collect::<Vec<_>>();

        let message = result
            .message()
            .map_or_else(String::new, ToString::to_string);

        let metadata = json!({
            "stage_history_id": stage_history_id,
            "stage": stage.as_str(),
            "attempt": attempt,
            "status": result.as_str(),
            "artifact_count": sorted_artifacts.len(),
            "artifact_types": artifact_types,
            "completed_at": completed_at.to_rfc3339(),
        });

        let transcript_body = json!({
            "stage": stage.as_str(),
            "attempt": attempt,
            "status": result.as_str(),
            "message": message,
            "artifacts": artifact_refs,
            "metadata": metadata.clone(),
        });

        let transcript_text = serde_json::to_string(&transcript_body).map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to serialize stage transcript: {e}"))
        })?;

        sqlx::query(
            "UPDATE stage_history\n             SET transcript = $1\n             WHERE id = $2 AND transcript IS DISTINCT FROM $1",
        )
        .bind(&transcript_text)
        .bind(stage_history_id)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update stage transcript: {e}")))?;

        self.store_stage_artifact(
            stage_history_id,
            crate::types::ArtifactType::StageLog,
            &transcript_text,
            Some(metadata),
        )
        .await
        .map(|_| ())?;

        Ok(())
    }

    pub(crate) async fn ensure_stage_history_repo_scope(&self) -> Result<()> {
        sqlx::query(
            "ALTER TABLE stage_history
             ADD COLUMN IF NOT EXISTS repo_id TEXT NOT NULL DEFAULT 'local'",
        )
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| {
            SwarmError::DatabaseError(format!(
                "Failed to ensure stage_history.repo_id column exists: {e}"
            ))
        })
    }
}
