#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::runtime::shared::RuntimeError;
use crate::runtime::shared::{RuntimeAgentId, RuntimeBeadId};
use crate::runtime::stage::{Stage, StageResult};
use sqlx::PgPool;

pub struct RuntimePgStageRepository {
    pool: PgPool,
}

impl RuntimePgStageRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn record_started(
        &self,
        agent_id: &RuntimeAgentId,
        bead_id: &RuntimeBeadId,
        stage: Stage,
        attempt: u32,
    ) -> crate::runtime::shared::Result<i64> {
        self.ensure_stage_history_repo_scope().await?;
        sqlx::query_scalar::<_, i64>(
            "INSERT INTO stage_history (repo_id, agent_id, bead_id, stage, attempt_number, status) VALUES ($1, $2, $3, $4, $5, 'started') RETURNING id",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt.cast_signed())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| RuntimeError::RepositoryError(format!("record_started: {e}")))
    }

    pub async fn record_completed(
        &self,
        agent_id: &RuntimeAgentId,
        bead_id: &RuntimeBeadId,
        stage: Stage,
        attempt: u32,
        result: StageResult,
        duration_ms: u64,
    ) -> crate::runtime::shared::Result<()> {
        self.ensure_stage_history_repo_scope().await?;
        sqlx::query("UPDATE stage_history SET status = $6, result = $7, feedback = $8, completed_at = NOW(), duration_ms = $9 WHERE id = (SELECT id FROM stage_history WHERE repo_id = $1 AND agent_id = $2 AND bead_id = $3 AND stage = $4 AND attempt_number = $5 AND status = 'started' ORDER BY started_at DESC LIMIT 1)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .bind(bead_id.value())
            .bind(stage.as_str())
            .bind(attempt.cast_signed())
            .bind(result.message().map_or("passed", |_| "failed"))
            .bind(result.message())
            .bind(result.message())
            .bind(duration_ms.cast_signed())
            .execute(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("record_completed: {e}")))
            .map(|_| ())
    }

    async fn ensure_stage_history_repo_scope(&self) -> crate::runtime::shared::Result<()> {
        sqlx::query(
            "ALTER TABLE stage_history
             ADD COLUMN IF NOT EXISTS repo_id TEXT NOT NULL DEFAULT 'local'",
        )
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| RuntimeError::RepositoryError(format!("ensure stage_history repo scope: {e}")))
    }
}
