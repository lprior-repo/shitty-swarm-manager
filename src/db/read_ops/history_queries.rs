use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{ExecutionEvent, FailureDiagnostics, RepoId};

use super::resume_queries::diagnostics_from_row;
use super::types::{CommandAuditRow, ExecutionEventRow, ResourceLockRow};

impl SwarmDb {
    pub async fn get_command_history(
        &self,
        limit: i64,
    ) -> Result<
        Vec<(
            i64,
            i64,
            String,
            serde_json::Value,
            bool,
            u64,
            Option<String>,
        )>,
    > {
        sqlx::query("DELETE FROM resource_locks WHERE until_at <= NOW()")
            .execute(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to cleanup locks: {e}")))?;

        sqlx::query_as::<_, CommandAuditRow>(
            "SELECT seq, t, cmd, args, ok, ms, error_code
             FROM command_audit
             ORDER BY seq DESC
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to load command history: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        row.seq,
                        row.t.timestamp_millis(),
                        row.cmd,
                        row.args,
                        row.ok,
                        u64::from(row.ms.cast_unsigned()),
                        row.error_code,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    pub async fn list_active_resource_locks(&self) -> Result<Vec<(String, String, i64, i64)>> {
        sqlx::query("DELETE FROM resource_locks WHERE until_at <= NOW()")
            .execute(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to cleanup locks: {e}")))?;

        sqlx::query_as::<_, ResourceLockRow>(
            "SELECT resource, agent, since, until_at
             FROM resource_locks
             ORDER BY since ASC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to load resource locks: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        row.resource,
                        row.agent,
                        row.since.timestamp_millis(),
                        row.until_at.timestamp_millis(),
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    pub async fn get_execution_events(
        &self,
        repo_id: &RepoId,
        bead_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ExecutionEvent>> {
        sqlx::query_as::<_, ExecutionEventRow>(
            "SELECT
                seq,
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
                payload,
                created_at
             FROM execution_events
             WHERE bead_id IN (
                 SELECT bc.bead_id FROM bead_claims bc WHERE bc.repo_id = $1
             )
             AND ($2::TEXT IS NULL OR bead_id = $2)
             ORDER BY seq DESC
             LIMIT $3",
        )
        .bind(repo_id.value())
        .bind(bead_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to load execution events: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    let diagnostics = diagnostics_from_row(&row);
                    ExecutionEvent {
                        seq: row.seq,
                        schema_version: row.schema_version,
                        event_type: row.event_type,
                        entity_id: row.entity_id,
                        bead_id: row.bead_id,
                        agent_id: row.agent_id.map(|id| id as u32),
                        stage: row.stage,
                        causation_id: row.causation_id,
                        diagnostics,
                        payload: row.payload,
                        created_at: row.created_at,
                    }
                })
                .collect::<Vec<_>>()
        })
    }
}
