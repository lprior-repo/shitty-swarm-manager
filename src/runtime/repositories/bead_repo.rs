#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::runtime::shared::{RuntimeAgentId, RuntimeBeadId, RuntimeError, RuntimeRepoId};
use sqlx::PgPool;

pub struct RuntimePgBeadRepository {
    pool: PgPool,
}

impl RuntimePgBeadRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn claim_next(
        &self,
        agent_id: &RuntimeAgentId,
    ) -> crate::runtime::shared::Result<Option<RuntimeBeadId>> {
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_bead($1, $2)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("claim_next: {e}")))
            .map(|opt| opt.map(RuntimeBeadId::new))
    }

    pub async fn release(&self, agent_id: &RuntimeAgentId) -> crate::runtime::shared::Result<()> {
        sqlx::query("UPDATE agent_state SET bead_id = NULL, current_stage = NULL, status = 'idle' WHERE repo_id = $1 AND agent_id = $2")
             .bind(agent_id.repo_id().value())
             .bind(agent_id.number().cast_signed())
             .execute(&self.pool)
             .await
            .map_err(|e| RuntimeError::RepositoryError(format!("release: {e}")))
            .map(|_| ())
    }

    pub async fn mark_blocked(
        &self,
        repo_id: &RuntimeRepoId,
        bead_id: &RuntimeBeadId,
        _reason: &str,
    ) -> crate::runtime::shared::Result<()> {
        sqlx::query(
            "UPDATE bead_backlog SET status = 'blocked' WHERE repo_id = $1 AND bead_id = $2",
        )
        .bind(repo_id.value())
        .bind(bead_id.value())
        .execute(&self.pool)
        .await
        .map_err(|e| RuntimeError::RepositoryError(format!("mark_blocked: {e}")))
        .map(|_| ())
    }

    pub async fn recover_stale_claims(
        &self,
        repo_id: &RuntimeRepoId,
    ) -> crate::runtime::shared::Result<u32> {
        sqlx::query_scalar::<_, i32>("SELECT recover_expired_bead_claims($1)")
            .bind(repo_id.value())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("recover_stale_claims: {e}")))
            .map(|count| u32::try_from(count).unwrap_or(0))
    }

    pub async fn heartbeat_claim(
        &self,
        agent_id: &RuntimeAgentId,
        bead_id: &RuntimeBeadId,
        lease_extension_ms: i32,
    ) -> crate::runtime::shared::Result<bool> {
        sqlx::query_scalar::<_, bool>("SELECT heartbeat_bead_claim($1, $2, $3, $4)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .bind(bead_id.value())
            .bind(lease_extension_ms)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("heartbeat_claim: {e}")))
    }
}
