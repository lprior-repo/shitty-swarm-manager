use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::runtime::{
    RuntimeAgentId, RuntimeAgentState, RuntimeAgentStatus, RuntimeBeadId, RuntimeRepoId,
    RuntimeStage,
};
use crate::types::{AgentId, AgentStatus, AvailableAgent, RepoId};

impl SwarmDb {
    pub async fn get_agent_state(&self, agent_id: &AgentId) -> Result<Option<RuntimeAgentState>> {
        let row = sqlx::query_as::<_, (Option<String>, Option<String>, String, i32)>(
            "SELECT bead_id, current_stage, status, implementation_attempt
             FROM agent_state
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .fetch_optional(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load agent state: {error}"))
        })?;

        row.map_or(
            Ok(None),
            |(bead_id, current_stage, status, implementation_attempt)| {
                let parsed_stage = current_stage
                    .map(|value| RuntimeStage::try_from(value.as_str()))
                    .transpose()
                    .map_err(SwarmError::DatabaseError)?;
                let parsed_status = RuntimeAgentStatus::try_from(status.as_str())
                    .map_err(SwarmError::DatabaseError)?;
                let runtime_agent = RuntimeAgentId::new(
                    RuntimeRepoId::new(agent_id.repo_id().value().to_string()),
                    agent_id.number(),
                );
                let state = RuntimeAgentState::new(
                    runtime_agent,
                    bead_id.map(RuntimeBeadId::new),
                    parsed_stage,
                    parsed_status,
                    implementation_attempt.max(0).cast_unsigned(),
                );
                state
                    .validate_invariants()
                    .map_err(|error| SwarmError::DatabaseError(error.to_string()))?;
                Ok(Some(state))
            },
        )
    }

    pub async fn get_available_agents(&self, repo_id: &RepoId) -> Result<Vec<AvailableAgent>> {
        let rows = sqlx::query_as::<_, (i32, String, i32, i32, i32)>(
            "SELECT
                a.agent_id,
                a.status,
                a.implementation_attempt,
                c.max_implementation_attempts,
                c.max_agents
             FROM agent_state a
             JOIN swarm_config c ON c.repo_id = a.repo_id
             WHERE a.repo_id = $1
             ORDER BY a.agent_id ASC",
        )
        .bind(repo_id.value())
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load available agents: {error}"))
        })?;

        rows.into_iter()
            .map(
                |(agent_id, status, implementation_attempt, max_attempts, max_agents)| {
                    let status = AgentStatus::try_from(status.as_str())
                        .map_err(SwarmError::DatabaseError)?;
                    Ok(AvailableAgent {
                        repo_id: repo_id.clone(),
                        agent_id: agent_id.max(0).cast_unsigned(),
                        status,
                        implementation_attempt: implementation_attempt.max(0).cast_unsigned(),
                        max_implementation_attempts: max_attempts.max(0).cast_unsigned(),
                        max_agents: max_agents.max(0).cast_unsigned(),
                    })
                },
            )
            .collect()
    }

    pub async fn get_active_agents(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<(RepoId, u32, Option<String>, String)>> {
        sqlx::query_as::<_, (i32, Option<String>, String)>(
            "SELECT agent_id, bead_id, status
             FROM agent_state
             WHERE repo_id = $1 AND status <> 'idle'
             ORDER BY agent_id ASC",
        )
        .bind(repo_id.value())
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load active agents: {error}"))
        })
        .map(|rows| {
            rows.into_iter()
                .map(|(agent_id, bead_id, status)| {
                    (
                        repo_id.clone(),
                        agent_id.max(0).cast_unsigned(),
                        bead_id,
                        status,
                    )
                })
                .collect::<Vec<_>>()
        })
    }
}
