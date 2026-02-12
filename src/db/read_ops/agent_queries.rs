use crate::db::mappers::{parse_agent_state, to_u32_i32, AgentStateFields};
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, AgentState, AgentStatus, AvailableAgent, RepoId};

use super::types::{ActiveAgentRow, AgentStateRow, AvailableAgentRow};

impl SwarmDb {
    pub async fn get_agent_state(&self, agent_id: &AgentId) -> Result<Option<AgentState>> {
        let agent_id_number = agent_id.number();
        sqlx::query_as::<_, AgentStateRow>(
            "SELECT bead_id, current_stage, stage_started_at, status, last_update, implementation_attempt, feedback
             FROM agent_state WHERE repo_id = $1 AND agent_id = $2",
        )
            .bind(agent_id.repo_id().value())
            .bind(agent_id_number.cast_signed())
            .fetch_optional(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get agent state: {e}")))
            .and_then(|row_opt| {
                row_opt
                    .map(|row| {
                        parse_agent_state(
                            agent_id,
                            AgentStateFields {
                                bead_id: row.bead_id,
                                stage_str: row.current_stage,
                                stage_started_at: row.stage_started_at,
                                status_str: row.status,
                                last_update: row.last_update,
                                implementation_attempt: row.implementation_attempt,
                                feedback: row.feedback,
                            },
                        )
                    })
                    .transpose()
            })
    }

    pub async fn get_available_agents(&self, repo_id: &RepoId) -> Result<Vec<AvailableAgent>> {
        sqlx::query_as::<_, AvailableAgentRow>(
            "SELECT agent_id, status, implementation_attempt, max_implementation_attempts, max_agents
             FROM v_available_agents
             WHERE repo_id = $1",
        )
            .bind(repo_id.value())
            .fetch_all(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get available agents: {e}")))
            .and_then(|rows| {
                rows.into_iter()
                    .map(|row| {
                        AgentStatus::try_from(row.status.as_str())
                            .map_err(SwarmError::DatabaseError)
                            .map(|status| AvailableAgent {
                                repo_id: repo_id.clone(),
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

    pub async fn get_active_agents(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<(RepoId, u32, Option<String>, String)>> {
        sqlx::query_as::<_, ActiveAgentRow>(
            "SELECT repo_id, agent_id, bead_id, status
             FROM v_active_agents
             WHERE repo_id = $1
             ORDER BY last_update DESC",
        )
        .bind(repo_id.value())
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get active agents: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        RepoId::new(row.repo_id),
                        to_u32_i32(row.agent_id),
                        row.bead_id,
                        row.status,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    pub async fn get_all_active_agents(
        &self,
    ) -> Result<Vec<(RepoId, u32, Option<String>, String)>> {
        sqlx::query_as::<_, ActiveAgentRow>(
            "SELECT repo_id, agent_id, bead_id, status FROM v_active_agents ORDER BY last_update DESC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get active agents: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        RepoId::new(row.repo_id),
                        to_u32_i32(row.agent_id),
                        row.bead_id,
                        row.status,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    pub async fn get_feedback_required(
        &self,
    ) -> Result<Vec<(RepoId, u32, String, u32, Option<String>, Option<String>)>> {
        sqlx::query_as::<
            _,
            (
                String,
                String,
                i32,
                String,
                i32,
                Option<String>,
                Option<String>,
            ),
        >(
            "SELECT repo_id, bead_id, agent_id, stage, attempt_number, feedback, completed_at::TEXT
             FROM v_feedback_required
             ORDER BY completed_at DESC NULLS LAST",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to query feedback: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        RepoId::new(row.0),
                        to_u32_i32(row.2),
                        row.3,
                        to_u32_i32(row.4),
                        row.5,
                        row.6,
                    )
                })
                .collect::<Vec<_>>()
        })
    }
}
