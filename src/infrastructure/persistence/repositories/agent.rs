#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use sqlx::PgPool;

use crate::error::{Result, SwarmError};
use crate::types::{AgentId, RepoId};

pub struct AgentRepository {
    pool: PgPool,
}

impl AgentRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn register(&self, agent_id: &AgentId) -> Result<bool> {
        let repo_scoped = self.table_has_column("agent_state", "repo_id").await?;

        if repo_scoped {
            self.register_repo(agent_id.repo_id()).await?;

            sqlx::query(
                "INSERT INTO agent_state (repo_id, agent_id, status) VALUES ($1, $2, 'idle')
                 ON CONFLICT (repo_id, agent_id) DO NOTHING",
            )
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number() as i32)
            .execute(&self.pool)
            .await
            .map(|rows| rows.rows_affected() > 0)
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to register agent: {e}")))
        } else {
            sqlx::query(
                "INSERT INTO agent_state (agent_id, status) VALUES ($1, 'idle')
                 ON CONFLICT (agent_id) DO NOTHING",
            )
            .bind(agent_id.number() as i32)
            .execute(&self.pool)
            .await
            .map(|rows| rows.rows_affected() > 0)
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to register agent: {e}")))
        }
    }

    async fn register_repo(&self, repo_id: &RepoId) -> Result<()> {
        sqlx::query(
            "INSERT INTO repos (repo_id, name, path) VALUES ($1, $2, $3)
             ON CONFLICT (repo_id) DO NOTHING",
        )
        .bind(repo_id.value())
        .bind(repo_id.value())
        .bind(repo_id.value())
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to register repo: {e}")))
    }

    async fn table_has_column(&self, table: &str, column: &str) -> Result<bool> {
        let result: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_name = $1 AND column_name = $2
            )",
        )
        .bind(table)
        .bind(column)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Schema check failed: {e}")))?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_repository_can_be_constructed() {}
}
