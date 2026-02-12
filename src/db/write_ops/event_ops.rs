#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::helpers::{
    event_entity_id, landing_sync_causation_id, landing_sync_status_key, redact_sensitive,
};
use super::types::ExecutionEventWriteInput;
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{BeadId, EventSchemaVersion, Stage};
use crate::BrSyncStatus;
use serde_json::json;

impl SwarmDb {
    pub(crate) async fn record_execution_event(
        &self,
        bead_id: &BeadId,
        agent_id: &crate::types::AgentId,
        input: ExecutionEventWriteInput,
    ) -> Result<()> {
        let diagnostics_category = input
            .diagnostics
            .as_ref()
            .map(|value| value.category.as_str());
        let diagnostics_retryable = input.diagnostics.as_ref().map(|value| value.retryable);
        let diagnostics_next_command = input
            .diagnostics
            .as_ref()
            .map(|value| value.next_command.as_str());
        let diagnostics_detail = input
            .diagnostics
            .as_ref()
            .and_then(|value| value.detail.clone());

        sqlx::query(
            "INSERT INTO execution_events (
                schema_version,
                event_type,
                entity_id,
                bead_id,
                agent_id,
                stage,
                causation_id,
                diagnostics_category,
                diagnostics_retryable,
                diagnostics_next_command,
                diagnostics_detail,
                payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(EventSchemaVersion::V1.as_i32())
        .bind(input.event_type)
        .bind(event_entity_id(bead_id, agent_id.repo_id()))
        .bind(bead_id.value())
        .bind(agent_id.number().cast_signed())
        .bind(input.stage.map(|value| value.as_str()))
        .bind(input.causation_id)
        .bind(diagnostics_category)
        .bind(diagnostics_retryable)
        .bind(diagnostics_next_command)
        .bind(diagnostics_detail)
        .bind(input.payload)
        .execute(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to write execution event: {e}")))
        .map(|_| ())
    }

    pub(crate) async fn record_execution_event_if_absent(
        &self,
        bead_id: &BeadId,
        agent_id: &crate::types::AgentId,
        input: ExecutionEventWriteInput,
    ) -> Result<()> {
        let should_insert = match input.causation_id.as_deref() {
            Some(causation_id) => {
                !self
                    .execution_event_exists(bead_id, agent_id, input.event_type, causation_id)
                    .await?
            }
            None => true,
        };

        if should_insert {
            self.record_execution_event(bead_id, agent_id, input).await
        } else {
            Ok(())
        }
    }

    async fn execution_event_exists(
        &self,
        bead_id: &BeadId,
        agent_id: &crate::types::AgentId,
        event_type: &str,
        causation_id: &str,
    ) -> Result<bool> {
        let repo_entity_id = event_entity_id(bead_id, agent_id.repo_id());
        let legacy_entity_id = format!("bead:{}", bead_id.value());

        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM execution_events
                WHERE bead_id = $1
                  AND event_type = $2
                  AND causation_id = $3
                  AND (entity_id = $4 OR entity_id = $5)
            )",
        )
        .bind(bead_id.value())
        .bind(event_type)
        .bind(causation_id)
        .bind(repo_entity_id)
        .bind(legacy_entity_id)
        .fetch_one(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to check existing execution event: {e}"))
        })
    }

    pub(crate) async fn record_landing_sync_outcome_if_absent(
        &self,
        bead_id: &BeadId,
        agent_id: &crate::types::AgentId,
        status: BrSyncStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        self.record_execution_event_if_absent(
            bead_id,
            agent_id,
            ExecutionEventWriteInput {
                stage: Some(Stage::RedQueen),
                event_type: "landing_sync",
                causation_id: Some(landing_sync_causation_id(status, reason)),
                payload: json!({
                    "status": landing_sync_status_key(status),
                    "reason": reason.map(redact_sensitive),
                }),
                diagnostics: None,
            },
        )
        .await
    }
}
