#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, BeadId, RepoId};
use sqlx::Acquire;
use std::collections::HashSet;

impl SwarmDb {
    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn register_repo(&self, repo_id: &RepoId, name: &str, path: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO repos (repo_id, name, path) VALUES ($1, $2, $3)
             ON CONFLICT (repo_id) DO NOTHING",
        )
        .bind(repo_id.value())
        .bind(name)
        .bind(path)
        .execute(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to register repo: {e}")))
        .map(|_result| ())
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn register_agent(&self, agent_id: &AgentId) -> Result<bool> {
        let repo_scoped = self.table_has_column("agent_state", "repo_id").await?;

        if repo_scoped {
            self.register_repo(
                agent_id.repo_id(),
                agent_id.repo_id().value(),
                agent_id.repo_id().value(),
            )
            .await?;

            sqlx::query(
                "INSERT INTO agent_state (repo_id, agent_id, status) VALUES ($1, $2, 'idle')
                 ON CONFLICT (repo_id, agent_id) DO NOTHING",
            )
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .execute(self.pool())
            .await
            .map(|rows| rows.rows_affected() > 0)
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to register agent: {e}")))
        } else {
            sqlx::query(
                "INSERT INTO agent_state (agent_id, status) VALUES ($1, 'idle')
                 ON CONFLICT (agent_id) DO NOTHING",
            )
            .bind(agent_id.number().cast_signed())
            .execute(self.pool())
            .await
            .map(|rows| rows.rows_affected() > 0)
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to register agent: {e}")))
        }
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn seed_idle_agents(&self, count: u32) -> Result<()> {
        let repo_scoped = self.table_has_column("agent_state", "repo_id").await?;
        let default_repo = RepoId::new("local");

        if repo_scoped {
            self.register_repo(&default_repo, default_repo.value(), default_repo.value())
                .await?;
        }
        self.prune_idle_unassigned_agents(repo_scoped, &default_repo, count)
            .await?;

        let existing_agent_ids = self
            .load_existing_agent_ids(repo_scoped, &default_repo)
            .await?;

        let mut occupied_ids = existing_agent_ids
            .into_iter()
            .map(i32::cast_unsigned)
            .collect::<HashSet<_>>();

        let idle_unassigned_count = self
            .count_idle_unassigned_agents(repo_scoped, &default_repo)
            .await?;

        let target_count = i64::from(count);
        if idle_unassigned_count >= target_count {
            return Ok(());
        }

        let mut next_candidate = 1_u32;
        let agents_to_add = target_count - idle_unassigned_count;
        for _ in 0..agents_to_add {
            while occupied_ids.contains(&next_candidate) {
                next_candidate = next_candidate.saturating_add(1);
            }

            self.insert_idle_agent(repo_scoped, &default_repo, next_candidate)
                .await?;

            occupied_ids.insert(next_candidate);
            next_candidate = next_candidate.saturating_add(1);
        }

        Ok(())
    }

    async fn prune_idle_unassigned_agents(
        &self,
        repo_scoped: bool,
        default_repo: &RepoId,
        count: u32,
    ) -> Result<()> {
        if repo_scoped {
            sqlx::query(
                "DELETE FROM agent_state
                 WHERE repo_id = $1
                   AND status = 'idle'
                   AND bead_id IS NULL
                   AND agent_id IN (
                     SELECT agent_id
                     FROM agent_state
                     WHERE repo_id = $1 AND status = 'idle' AND bead_id IS NULL
                     ORDER BY agent_id DESC
                     OFFSET $2
                   )",
            )
            .bind(default_repo.value())
            .bind(count.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to prune idle agents: {e}")))
        } else {
            sqlx::query(
                "DELETE FROM agent_state
                 WHERE status = 'idle'
                   AND bead_id IS NULL
                   AND agent_id IN (
                     SELECT agent_id
                     FROM agent_state
                     WHERE status = 'idle' AND bead_id IS NULL
                     ORDER BY agent_id DESC
                     OFFSET $1
                   )",
            )
            .bind(count.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to prune idle agents: {e}")))
        }
    }

    async fn load_existing_agent_ids(
        &self,
        repo_scoped: bool,
        default_repo: &RepoId,
    ) -> Result<Vec<i32>> {
        if repo_scoped {
            sqlx::query_scalar::<_, i32>(
                "SELECT agent_id
                 FROM agent_state
                 WHERE repo_id = $1
                 ORDER BY agent_id ASC",
            )
            .bind(default_repo.value())
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to load existing seeded agents: {e}"))
            })
        } else {
            sqlx::query_scalar::<_, i32>("SELECT agent_id FROM agent_state ORDER BY agent_id ASC")
                .fetch_all(self.pool())
                .await
                .map_err(|e| {
                    SwarmError::DatabaseError(format!("Failed to load existing seeded agents: {e}"))
                })
        }
    }

    async fn count_idle_unassigned_agents(
        &self,
        repo_scoped: bool,
        default_repo: &RepoId,
    ) -> Result<i64> {
        if repo_scoped {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)
                 FROM agent_state
                 WHERE repo_id = $1 AND status = 'idle' AND bead_id IS NULL",
            )
            .bind(default_repo.value())
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to count idle unassigned agents: {e}"))
            })
        } else {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_state WHERE status = 'idle' AND bead_id IS NULL",
            )
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to count idle unassigned agents: {e}"))
            })
        }
    }

    async fn insert_idle_agent(
        &self,
        repo_scoped: bool,
        default_repo: &RepoId,
        agent_number: u32,
    ) -> Result<()> {
        if repo_scoped {
            sqlx::query(
                "INSERT INTO agent_state (repo_id, agent_id, status)
                 VALUES ($1, $2, 'idle')
                 ON CONFLICT (repo_id, agent_id) DO NOTHING",
            )
            .bind(default_repo.value())
            .bind(agent_number.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to seed agent {agent_number}: {e}"))
            })
        } else {
            sqlx::query(
                "INSERT INTO agent_state (agent_id, status)
                 VALUES ($1, 'idle')
                 ON CONFLICT (agent_id) DO NOTHING",
            )
            .bind(agent_number.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to seed agent {agent_number}: {e}"))
            })
        }
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn release_agent(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}")))?;

        let bead = sqlx::query_scalar::<_, Option<String>>(
            "SELECT bead_id
             FROM agent_state
             WHERE repo_id = $1 AND agent_id = $2
             FOR UPDATE",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to read agent state: {e}")))?
        .flatten();

        sqlx::query(
            "UPDATE agent_state
             SET bead_id = NULL,
                 current_stage = NULL,
                 stage_started_at = NULL,
                 status = 'idle',
                 feedback = NULL,
                 implementation_attempt = 0
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to reset agent state: {e}")))?;

        if let Some(bead_id) = bead.as_deref() {
            sqlx::query("DELETE FROM agent_messages WHERE bead_id = $1")
                .bind(bead_id)
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    SwarmError::DatabaseError(format!(
                        "Failed to clear bead messages on release: {e}"
                    ))
                })?;

            sqlx::query("DELETE FROM bead_claims WHERE repo_id = $1 AND bead_id = $2")
                .bind(agent_id.repo_id().value())
                .bind(bead_id)
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    SwarmError::DatabaseError(format!("Failed to clear bead claim on release: {e}"))
                })?;

            sqlx::query(
                "UPDATE bead_backlog
                 SET status = 'pending'
                 WHERE repo_id = $1
                   AND bead_id = $2
                   AND status <> 'completed'",
            )
            .bind(agent_id.repo_id().value())
            .bind(bead_id)
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to reset backlog status on release: {e}"))
            })?;
        }

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))?;

        Ok(bead.map(BeadId::new))
    }
}
