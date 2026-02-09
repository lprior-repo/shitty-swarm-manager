use crate::db::mappers::{parse_agent_state, parse_swarm_config, to_u32_i32};
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{
    AgentId, AgentState, AgentStatus, AvailableAgent, BeadId, ProgressSummary, RepoId, SwarmConfig,
};
use sqlx::FromRow;

#[derive(FromRow)]
struct AgentStateRow {
    bead_id: Option<String>,
    current_stage: Option<String>,
    stage_started_at: Option<chrono::DateTime<chrono::Utc>>,
    status: String,
    last_update: chrono::DateTime<chrono::Utc>,
    implementation_attempt: i32,
    feedback: Option<String>,
}

#[derive(FromRow)]
struct AvailableAgentRow {
    agent_id: i32,
    status: String,
    implementation_attempt: i32,
    max_implementation_attempts: i32,
    max_agents: i32,
}

#[derive(FromRow)]
struct ProgressRow {
    done_agents: i64,
    working_agents: i64,
    waiting_agents: i64,
    error_agents: i64,
    idle_agents: i64,
    total_agents: i64,
}

#[derive(FromRow)]
struct SwarmConfigRow {
    max_agents: i32,
    max_implementation_attempts: i32,
    claim_label: String,
    swarm_started_at: Option<chrono::DateTime<chrono::Utc>>,
    swarm_status: String,
}

#[derive(FromRow)]
struct ActiveAgentRow {
    agent_id: i32,
    bead_id: Option<String>,
    status: String,
}

#[derive(FromRow)]
struct FeedbackRow {
    bead_id: String,
    agent_id: i32,
    stage: String,
    attempt_number: i32,
    feedback: Option<String>,
    completed_at: Option<String>,
}

impl SwarmDb {
    pub async fn get_agent_state(&self, agent_id: &AgentId) -> Result<Option<AgentState>> {
        sqlx::query_as::<_, AgentStateRow>(
            "SELECT bead_id, current_stage, stage_started_at, status, last_update, implementation_attempt, feedback
             FROM agent_state WHERE agent_id = $1",
        )
            .bind(agent_id.number() as i32)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get agent state: {}", e)))
            .and_then(|row_opt| {
                row_opt
                    .map(|row| {
                        parse_agent_state(
                            agent_id,
                            row.bead_id,
                            row.current_stage,
                            row.stage_started_at,
                            row.status,
                            row.last_update,
                            row.implementation_attempt,
                            row.feedback,
                        )
                    })
                    .transpose()
            })
    }

    pub async fn get_available_agents(&self, repo_id: &RepoId) -> Result<Vec<AvailableAgent>> {
        let local_repo = repo_id.clone();
        sqlx::query_as::<_, AvailableAgentRow>(
            "SELECT agent_id, status, implementation_attempt, max_implementation_attempts, max_agents
             FROM v_available_agents",
        )
            .fetch_all(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get available agents: {}", e)))
            .and_then(|rows| {
                rows.into_iter()
                    .map(|row| {
                        AgentStatus::try_from(row.status.as_str())
                            .map_err(SwarmError::DatabaseError)
                            .map(|status| AvailableAgent {
                                repo_id: local_repo.clone(),
                                agent_id: to_u32_i32(row.agent_id),
                                status,
                                implementation_attempt: to_u32_i32(row.implementation_attempt),
                                max_implementation_attempts: to_u32_i32(row.max_implementation_attempts),
                                max_agents: to_u32_i32(row.max_agents),
                            })
                    })
                    .collect::<Result<Vec<_>>>()
            })
    }

    pub async fn get_progress(&self, _repo_id: &RepoId) -> Result<ProgressSummary> {
        sqlx::query_as::<_, ProgressRow>(
            "SELECT done_agents, working_agents, waiting_agents, error_agents, idle_agents, total_agents
             FROM v_swarm_progress",
        )
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get progress: {}", e)))
            .map(|row| ProgressSummary {
                completed: row.done_agents as u64,
                working: row.working_agents as u64,
                waiting: row.waiting_agents as u64,
                errors: row.error_agents as u64,
                idle: row.idle_agents as u64,
                total_agents: row.total_agents as u64,
            })
    }

    pub async fn get_config(&self, _repo_id: &RepoId) -> Result<SwarmConfig> {
        sqlx::query_as::<_, SwarmConfigRow>(
            "SELECT max_agents, max_implementation_attempts, claim_label, swarm_started_at, swarm_status
             FROM swarm_config WHERE id = TRUE",
        )
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get config: {}", e)))
            .and_then(|row| {
                parse_swarm_config(
                    row.max_agents,
                    row.max_implementation_attempts,
                    row.claim_label,
                    row.swarm_started_at,
                    row.swarm_status,
                )
            })
    }

    pub async fn list_repos(&self) -> Result<Vec<(RepoId, String)>> {
        Ok(vec![(RepoId::new("local"), "local".to_string())])
    }

    pub async fn get_all_active_agents(
        &self,
    ) -> Result<Vec<(RepoId, u32, Option<String>, String)>> {
        sqlx::query_as::<_, ActiveAgentRow>(
            "SELECT agent_id, bead_id, status FROM v_active_agents ORDER BY last_update DESC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get active agents: {}", e)))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        RepoId::new("local"),
                        to_u32_i32(row.agent_id),
                        row.bead_id,
                        row.status,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    pub async fn claim_next_bead(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_p0_bead($1)")
            .bind(agent_id.number() as i32)
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to claim next bead: {}", e)))
            .map(|value| value.map(BeadId::new))
    }

    pub async fn get_feedback_required(
        &self,
    ) -> Result<Vec<(String, u32, String, u32, Option<String>, Option<String>)>> {
        sqlx::query_as::<_, FeedbackRow>(
            "SELECT bead_id, agent_id, stage, attempt_number, feedback, completed_at::TEXT
             FROM v_feedback_required
             ORDER BY completed_at DESC NULLS LAST",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to query feedback: {}", e)))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        row.bead_id,
                        to_u32_i32(row.agent_id),
                        row.stage,
                        to_u32_i32(row.attempt_number),
                        row.feedback,
                        row.completed_at,
                    )
                })
                .collect::<Vec<_>>()
        })
    }
}
