#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use sqlx::PgPool;

use crate::error::{Result, SwarmError};
use crate::types::{AgentId, BeadId, RepoId};

pub struct BeadRepository {
    pool: PgPool,
}

impl BeadRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn claim_next(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_bead($1, $2)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number() as i32)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to claim next bead: {e}")))
            .map(|value| value.map(BeadId::new))
    }

    pub async fn heartbeat_claim(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        lease_extension_ms: i32,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>("SELECT heartbeat_bead_claim($1, $2, $3, $4)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number() as i32)
            .bind(bead_id.value())
            .bind(lease_extension_ms)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to heartbeat bead claim: {e}")))
    }

    pub async fn recover_expired_claims(&self, repo_id: &RepoId) -> Result<u32> {
        sqlx::query_scalar::<_, i32>("SELECT recover_expired_bead_claims($1)")
            .bind(repo_id.value())
            .fetch_one(&self.pool)
            .await
            .map(|count| count as u32)
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to recover expired claims: {e}"))
            })
    }

    pub async fn enqueue_batch(&self, repo_id: &RepoId, prefix: &str, count: u32) -> Result<()> {
        sqlx::query(
            "INSERT INTO bead_backlog (repo_id, bead_id, priority, status)
             SELECT $1, format('%s-%s', $2, g), 'p0', 'pending'
             FROM generate_series(1, $3) AS g",
        )
        .bind(repo_id.value())
        .bind(prefix)
        .bind(count as i32)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to enqueue backlog batch: {e}")))
    }
}
