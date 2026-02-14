#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::helpers::build_failure_diagnostics;
use super::types::{ExecutionEventWriteInput, StageTransitionInput};
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, BeadId, Stage};
use crate::BrSyncStatus;
use serde_json::json;
use sqlx::Acquire;

impl SwarmDb {
    pub async fn finalize_after_push_confirmation(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        push_confirmed: bool,
    ) -> Result<()> {
        crate::runtime::validate_completion_requires_push_confirmation(
            crate::runtime::RuntimeStageTransition::Complete,
            push_confirmed,
        )
        .map_err(|err| SwarmError::AgentError(err.to_string()))?;
        self.finalize_agent_and_bead(agent_id, bead_id).await?;
        self.record_landing_sync_outcome_if_absent(
            bead_id,
            agent_id,
            BrSyncStatus::Synchronized,
            None,
        )
        .await
    }

    pub(super) async fn apply_stage_transition(
        &self,
        input: StageTransitionInput<'_>,
    ) -> Result<()> {
        match input.transition {
            super::types::StageTransition::Finalize => {
                self.finalize_agent_and_bead(input.agent_id, input.bead_id)
                    .await?;
                self.record_execution_event(
                    input.bead_id,
                    input.agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(input.stage),
                        event_type: "transition_finalize",
                        causation_id: input
                            .stage_history_id
                            .map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "finalize"}),
                        diagnostics: None,
                    },
                )
                .await
            }
            super::types::StageTransition::Advance(next_stage) => {
                self.advance_to_stage(input.agent_id, *next_stage).await?;
                self.record_execution_event(
                    input.bead_id,
                    input.agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(input.stage),
                        event_type: "transition_advance",
                        causation_id: input
                            .stage_history_id
                            .map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "advance", "next_stage": next_stage.as_str()}),
                        diagnostics: None,
                    },
                )
                .await
            }
            super::types::StageTransition::RetryImplement => {
                self.persist_retry_packet(
                    input.stage_history_id,
                    input.stage,
                    input.attempt,
                    input.bead_id,
                    input.agent_id,
                    input.message,
                )
                .await?;

                let mut tx =
                    self.pool().begin().await.map_err(|e| {
                        SwarmError::DatabaseError(format!("Failed to begin tx: {e}"))
                    })?;

                let conn = tx.acquire().await.map_err(|e| {
                    SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}"))
                })?;

                sqlx::query(
                    "UPDATE agent_state
                     SET status = 'waiting', feedback = $3, implementation_attempt = implementation_attempt + 1, current_stage = 'implement'
                     WHERE repo_id = $1 AND agent_id = $2",
                )
                .bind(input.agent_id.repo_id().value())
                .bind(input.agent_id.number().cast_signed())
                .bind(input.message)
                .execute(&mut *conn)
                .await
                .map_err(|e| SwarmError::DatabaseError(format!("Failed to record failed stage: {e}")))?;

                tx.commit()
                    .await
                    .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))?;

                self.record_execution_event(
                    input.bead_id,
                    input.agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(input.stage),
                        event_type: "transition_retry",
                        causation_id: input
                            .stage_history_id
                            .map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "retry", "next_stage": Stage::Implement.as_str()}),
                        diagnostics: Some(build_failure_diagnostics(input.message)),
                    },
                )
                .await
            }
            super::types::StageTransition::NoOp => {
                self.record_execution_event(
                    input.bead_id,
                    input.agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(input.stage),
                        event_type: "transition_noop",
                        causation_id: input
                            .stage_history_id
                            .map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "noop"}),
                        diagnostics: None,
                    },
                )
                .await
            }
        }
    }

    async fn finalize_agent_and_bead(&self, agent_id: &AgentId, bead_id: &BeadId) -> Result<()> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}")))?;

        let claim_update = sqlx::query(
            "UPDATE bead_claims
             SET status = 'completed'
             WHERE repo_id = $1
               AND bead_id = $2
               AND claimed_by = $3
               AND status = 'in_progress'",
        )
        .bind(agent_id.repo_id().value())
        .bind(bead_id.value())
        .bind(agent_id.number().cast_signed())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to finalize bead: {e}")))?;

        if claim_update.rows_affected() != 1 {
            let existing_status = sqlx::query_scalar::<_, String>(
                "SELECT status
                 FROM bead_claims
                 WHERE repo_id = $1 AND bead_id = $2 AND claimed_by = $3
                 ORDER BY claimed_at DESC
                 LIMIT 1",
            )
            .bind(agent_id.repo_id().value())
            .bind(bead_id.value())
            .bind(agent_id.number().cast_signed())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!(
                    "Failed to read existing claim while finalizing bead: {e}"
                ))
            })?;

            if existing_status.as_deref() == Some("completed") {
                tx.commit()
                    .await
                    .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))?;
                return Ok(());
            }

            return Err(SwarmError::AgentError(format!(
                "Agent {} does not own active claim for bead {}",
                agent_id.number(),
                bead_id.value()
            )));
        }

        sqlx::query(
            "UPDATE agent_state
             SET status = 'done', current_stage = 'done'
             WHERE repo_id = $1 AND agent_id = $2 AND bead_id = $3",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.value())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to finalize agent: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))
    }

    async fn advance_to_stage(&self, agent_id: &AgentId, next_stage: Stage) -> Result<()> {
        sqlx::query(
            "UPDATE agent_state
             SET current_stage = $3, stage_started_at = NOW(), status = 'working'
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(next_stage.as_str())
        .execute(self.pool())
        .await
        .map(|_result| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to advance stage: {e}")))
    }
}
