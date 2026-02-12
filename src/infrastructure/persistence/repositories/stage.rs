#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use sqlx::PgPool;

use crate::error::{Result, SwarmError};
use crate::types::{AgentId, BeadId, Stage, StageResult};

pub struct StageRepository {
    pool: PgPool,
}

impl StageRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn record_started(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
    ) -> Result<i64> {
        self.ensure_repo_scope().await?;

        sqlx::query_scalar::<_, i64>(
            "INSERT INTO stage_history (repo_id, agent_id, bead_id, stage, attempt_number, status)
             VALUES ($1, $2, $3, $4, $5, 'started') RETURNING id",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number() as i32)
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt as i32)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to record stage start: {e}")))
    }

    pub async fn record_completed(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
        result: &StageResult,
        duration_ms: u64,
    ) -> Result<()> {
        let status = result.as_str();
        let message = result.message();

        sqlx::query(
            "UPDATE stage_history
             SET status = $6, result = $7, feedback = $8, completed_at = NOW(), duration_ms = $9
             WHERE id = (
                 SELECT id FROM stage_history
                 WHERE repo_id = $1 AND agent_id = $2 AND bead_id = $3
                   AND stage = $4 AND attempt_number = $5 AND status = 'started'
                 ORDER BY started_at DESC LIMIT 1
             )",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number() as i32)
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt as i32)
        .bind(status)
        .bind(message)
        .bind(message)
        .bind(duration_ms as i32)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update stage history: {e}")))?;

        Ok(())
    }

    pub async fn advance_agent_stage(&self, agent_id: &AgentId, next_stage: Stage) -> Result<()> {
        sqlx::query(
            "UPDATE agent_state
             SET current_stage = $3, stage_started_at = NOW(), status = 'working'
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number() as i32)
        .bind(next_stage.as_str())
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to advance stage: {e}")))
    }

    async fn ensure_repo_scope(&self) -> Result<()> {
        sqlx::query(
            "ALTER TABLE stage_history
             ADD COLUMN IF NOT EXISTS repo_id TEXT NOT NULL DEFAULT 'local'",
        )
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to ensure repo scope: {e}")))
    }
}
