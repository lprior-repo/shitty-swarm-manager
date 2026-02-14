#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::helpers::redact_sensitive;
use super::types::{ExecutionEventWriteInput, FailureDiagnosticsPayload};
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, BeadId, RepoId};
use serde_json::json;
use sqlx::Acquire;

impl SwarmDb {
    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn claim_bead(&self, agent_id: &AgentId, bead_id: &BeadId) -> Result<bool> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}")))?;

        sqlx::query(
            "SELECT 1
             FROM bead_backlog
             WHERE repo_id = $1 AND bead_id = $2
             FOR UPDATE",
        )
        .bind(agent_id.repo_id().value())
        .bind(bead_id.value())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to lock backlog bead: {e}")))?;

        let already_claimed = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1
                 FROM bead_claims
                 WHERE repo_id = $1
                   AND bead_id = $2
                   AND status = 'in_progress'
                 FOR UPDATE
             )",
        )
        .bind(agent_id.repo_id().value())
        .bind(bead_id.value())
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to inspect bead claims: {e}")))?;

        if already_claimed {
            tx.rollback()
                .await
                .map_err(|e| SwarmError::DatabaseError(format!("Failed to rollback tx: {e}")))?;
            return Ok(false);
        }

        sqlx::query(
            "INSERT INTO bead_backlog (repo_id, bead_id, priority, status)
             VALUES ($1, $2, 'p0', 'in_progress')
             ON CONFLICT (repo_id, bead_id)
             DO UPDATE SET status = 'in_progress'",
        )
        .bind(agent_id.repo_id().value())
        .bind(bead_id.value())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update backlog bead: {e}")))?;

        let claim_insert = sqlx::query(
            "INSERT INTO bead_claims (repo_id, bead_id, claimed_by, status, heartbeat_at, lease_expires_at)
             VALUES ($1, $2, $3, 'in_progress', NOW(), NOW() + INTERVAL '5 minutes')
             ON CONFLICT (repo_id, bead_id) DO NOTHING",
        )
        .bind(agent_id.repo_id().value())
        .bind(bead_id.value())
        .bind(agent_id.number().cast_signed())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to claim bead: {e}")))?;

        if claim_insert.rows_affected() != 1 {
            tx.rollback()
                .await
                .map_err(|e| SwarmError::DatabaseError(format!("Failed to rollback tx: {e}")))?;
            return Ok(false);
        }

        sqlx::query(
            "UPDATE agent_state
             SET bead_id = $3,
                 current_stage = 'rust-contract',
                 stage_started_at = NOW(),
                 status = 'working',
                 last_update = NOW()
             WHERE repo_id = $1
               AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.value())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update agent state: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))?;

        Ok(true)
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn heartbeat_claim(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        lease_extension_ms: i32,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>("SELECT heartbeat_bead_claim($1, $2, $3, $4)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .bind(bead_id.value())
            .bind(lease_extension_ms)
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to heartbeat bead claim: {e}")))
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn recover_expired_claims(&self, repo_id: &RepoId) -> Result<u32> {
        sqlx::query_scalar::<_, i32>("SELECT recover_expired_bead_claims($1)")
            .bind(repo_id.value())
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to recover expired claims: {e}"))
            })
            .map(i32::cast_unsigned)
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn enqueue_backlog_batch(
        &self,
        repo_id: &RepoId,
        prefix: &str,
        count: u32,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO bead_backlog (repo_id, bead_id, priority, status)
             SELECT $1, format('%s-%s', $2, g), 'p0', 'pending'
             FROM generate_series(1, $3) AS g",
        )
        .bind(repo_id.value())
        .bind(prefix)
        .bind(count.cast_signed())
        .execute(self.pool())
        .await
        .map(|_result| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to enqueue backlog batch: {e}")))
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn mark_bead_blocked(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        reason: &str,
    ) -> Result<()> {
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
             SET status = 'blocked'
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
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to block claim: {e}")))?;

        if claim_update.rows_affected() != 1 {
            return Err(SwarmError::AgentError(format!(
                "Agent {} does not own active claim for bead {}",
                agent_id.number(),
                bead_id.value()
            )));
        }

        sqlx::query(
            "UPDATE bead_backlog SET status = 'blocked' WHERE repo_id = $1 AND bead_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(bead_id.value())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to block backlog bead: {e}")))?;

        sqlx::query(
            "UPDATE agent_state
             SET status = 'error', feedback = $3
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(reason)
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to mark agent error: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))?;

        self.record_execution_event(
            bead_id,
            agent_id,
            ExecutionEventWriteInput {
                stage: None,
                event_type: "transition_blocked",
                causation_id: None,
                payload: json!({"transition": "blocked"}),
                diagnostics: Some(FailureDiagnosticsPayload {
                    category: "max_attempts_exhausted".to_string(),
                    retryable: false,
                    next_command: "swarm monitor --view failures".to_string(),
                    detail: Some(redact_sensitive(reason)),
                }),
            },
        )
        .await
    }
}
