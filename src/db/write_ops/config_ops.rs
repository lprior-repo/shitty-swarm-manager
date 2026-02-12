#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{RepoId, SwarmStatus};
use tracing::info;

impl SwarmDb {
    pub async fn set_swarm_status(&self, repo_id: &RepoId, status: SwarmStatus) -> Result<()> {
        let repo_scoped = self.table_has_column("swarm_config", "repo_id").await?;

        if repo_scoped {
            sqlx::query(
                "INSERT INTO swarm_config (repo_id, swarm_status)
                 VALUES ($1, $2)
                 ON CONFLICT (repo_id) DO UPDATE
                 SET swarm_status = EXCLUDED.swarm_status",
            )
            .bind(repo_id.value())
            .bind(status.as_str())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to update status: {e}")))
        } else {
            sqlx::query("UPDATE swarm_config SET swarm_status = $1 WHERE id = TRUE")
                .bind(status.as_str())
                .execute(self.pool())
                .await
                .map(|_result| ())
                .map_err(|e| SwarmError::DatabaseError(format!("Failed to update status: {e}")))
        }
    }

    pub async fn update_config(&self, max_agents: u32) -> Result<()> {
        sqlx::query("UPDATE swarm_config SET max_agents = $1")
            .bind(max_agents.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to update config: {e}")))
    }

    pub async fn start_swarm(&self, repo_id: &RepoId) -> Result<()> {
        let repo_scoped = self.table_has_column("swarm_config", "repo_id").await?;

        if repo_scoped {
            sqlx::query(
                "INSERT INTO swarm_config (repo_id, swarm_status, swarm_started_at)
                 VALUES ($1, 'running', NOW())
                 ON CONFLICT (repo_id) DO UPDATE
                 SET swarm_status = 'running',
                     swarm_started_at = NOW()",
            )
            .bind(repo_id.value())
            .execute(self.pool())
            .await
            .map(|_| info!("Started swarm"))
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to start swarm: {e}")))
        } else {
            sqlx::query(
                "UPDATE swarm_config SET swarm_status = 'running', swarm_started_at = NOW() WHERE id = TRUE",
            )
            .execute(self.pool())
            .await
            .map(|_| info!("Started swarm"))
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to start swarm: {e}")))
        }
    }

    pub async fn initialize_schema_from_sql(&self, schema_sql: &str) -> Result<()> {
        sqlx::raw_sql(schema_sql)
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to initialize schema: {e}")))
    }
}
