use crate::db::mappers::{parse_swarm_config, to_u32_i32};
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, BeadId, ProgressSummary, RepoId, SwarmConfig};

use super::resume_queries::repo_id_from_context;
use super::types::ProgressRow;
use super::types::SwarmConfigRow;

impl SwarmDb {
    pub async fn get_progress(&self, repo_id: &RepoId) -> Result<ProgressSummary> {
        sqlx::query_as::<_, ProgressRow>(
            "SELECT
                done_agents AS done,
                working_agents AS working,
                waiting_agents AS waiting,
                error_agents AS error,
                idle_agents AS idle,
                total_agents AS total
             FROM v_swarm_progress
             WHERE repo_id = $1",
        )
        .bind(repo_id.value())
        .fetch_optional(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get progress: {e}")))
        .map(|row_opt| {
            let row = row_opt.unwrap_or(ProgressRow {
                done: 0,
                working: 0,
                waiting: 0,
                error: 0,
                idle: 0,
                total: 0,
            });
            ProgressSummary {
                completed: row.done.cast_unsigned(),
                working: row.working.cast_unsigned(),
                waiting: row.waiting.cast_unsigned(),
                errors: row.error.cast_unsigned(),
                idle: row.idle.cast_unsigned(),
                total_agents: row.total.cast_unsigned(),
            }
        })
    }

    pub async fn get_config(&self, _repo_id: &RepoId) -> Result<SwarmConfig> {
        sqlx::query_as::<_, SwarmConfigRow>(
            "SELECT max_agents, max_implementation_attempts, claim_label, swarm_started_at, swarm_status
             FROM swarm_config WHERE id = TRUE",
        )
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get config: {e}")))
            .and_then(|row| {
                parse_swarm_config(
                    row.max_agents,
                    row.max_implementation_attempts,
                    row.claim_label,
                    row.swarm_started_at,
                    &row.swarm_status,
                )
            })
    }

    pub fn list_repos(&self) -> Result<Vec<(RepoId, String)>> {
        let repo_id = repo_id_from_context();
        Ok(vec![(repo_id.clone(), repo_id.value().to_string())])
    }

    pub async fn claim_next_bead(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        let claim_agent_id = agent_id.number();
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_bead($1, $2)")
            .bind(agent_id.repo_id().value())
            .bind(claim_agent_id.cast_signed())
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to claim next bead: {e}")))
            .map(|value| value.map(BeadId::new))
    }
}
