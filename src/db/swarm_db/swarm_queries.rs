use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, BeadId, ProgressSummary, RepoId, SwarmConfig, SwarmStatus};

impl SwarmDb {
    pub async fn get_config(&self, repo_id: &RepoId) -> Result<SwarmConfig> {
        let row = sqlx::query_as::<
            _,
            (
                i32,
                i32,
                Option<String>,
                Option<chrono::DateTime<chrono::Utc>>,
                String,
            ),
        >(
            "SELECT max_agents, max_implementation_attempts, claim_label, swarm_started_at, swarm_status
             FROM swarm_config
             WHERE repo_id = $1
             ORDER BY swarm_started_at DESC NULLS LAST
             LIMIT 1",
        )
        .bind(repo_id.value())
        .fetch_optional(self.pool())
        .await
        .map_err(|error| SwarmError::DatabaseError(format!("Failed to load swarm config: {error}")))?;

        if let Some((max_agents, max_attempts, claim_label, swarm_started_at, swarm_status)) = row {
            let status =
                SwarmStatus::try_from(swarm_status.as_str()).map_err(SwarmError::DatabaseError)?;
            return Ok(SwarmConfig {
                repo_id: repo_id.clone(),
                max_agents: max_agents.max(0).cast_unsigned(),
                max_implementation_attempts: max_attempts.max(0).cast_unsigned(),
                claim_label: claim_label.unwrap_or_else(|| "swarm".to_string()),
                swarm_started_at,
                swarm_status: status,
            });
        }

        Ok(SwarmConfig {
            repo_id: repo_id.clone(),
            max_agents: 10,
            max_implementation_attempts: 3,
            claim_label: "swarm".to_string(),
            swarm_started_at: None,
            swarm_status: SwarmStatus::Initializing,
        })
    }

    pub async fn get_progress(&self, repo_id: &RepoId) -> Result<ProgressSummary> {
        let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
            "SELECT
                COUNT(*) FILTER (WHERE status = 'working') AS working,
                COUNT(*) FILTER (WHERE status = 'idle') AS idle,
                COUNT(*) FILTER (WHERE status = 'waiting') AS waiting,
                COUNT(*) FILTER (WHERE status = 'done') AS done,
                COUNT(*) FILTER (WHERE status = 'error') AS errors
             FROM agent_state
             WHERE repo_id = $1",
        )
        .bind(repo_id.value())
        .fetch_one(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load progress summary: {error}"))
        })?;

        let (working, idle, waiting, completed, errors) = row;
        Ok(ProgressSummary {
            completed: completed.max(0).cast_unsigned(),
            working: working.max(0).cast_unsigned(),
            waiting: waiting.max(0).cast_unsigned(),
            errors: errors.max(0).cast_unsigned(),
            idle: idle.max(0).cast_unsigned(),
            total_agents: (working + idle + waiting + completed + errors)
                .max(0)
                .cast_unsigned(),
        })
    }

    pub async fn claim_next_bead(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_bead($1, $2)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .fetch_one(self.pool())
            .await
            .map_err(|error| {
                SwarmError::DatabaseError(format!("Failed to claim next bead: {error}"))
            })
            .map(|value| value.map(BeadId::new))
    }
}
