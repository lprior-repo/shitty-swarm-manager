use crate::error::{Result, SwarmError};
use crate::types::{AgentState, AgentStatus, BeadId, Stage, SwarmConfig, SwarmStatus};

pub fn parse_agent_state(
    agent_id: &crate::types::AgentId,
    bead_id: Option<String>,
    stage_str: Option<String>,
    stage_started_at: Option<chrono::DateTime<chrono::Utc>>,
    status_str: String,
    last_update: chrono::DateTime<chrono::Utc>,
    implementation_attempt: i32,
    feedback: Option<String>,
) -> Result<AgentState> {
    let status = AgentStatus::try_from(status_str.as_str()).map_err(SwarmError::DatabaseError)?;

    Ok(AgentState {
        agent_id: agent_id.clone(),
        bead_id: bead_id.map(BeadId::new),
        current_stage: stage_str.and_then(|s| Stage::try_from(s.as_str()).ok()),
        stage_started_at,
        status,
        last_update,
        implementation_attempt: to_u32_i32(implementation_attempt),
        feedback,
    })
}

pub fn parse_swarm_config(
    max_agents: i32,
    max_attempts: i32,
    claim_label: String,
    swarm_started_at: Option<chrono::DateTime<chrono::Utc>>,
    status_str: String,
) -> Result<SwarmConfig> {
    let swarm_status =
        SwarmStatus::try_from(status_str.as_str()).map_err(SwarmError::DatabaseError)?;

    Ok(SwarmConfig {
        repo_id: crate::types::RepoId::new("local"),
        max_agents: to_u32_i32(max_agents),
        max_implementation_attempts: to_u32_i32(max_attempts),
        claim_label,
        swarm_started_at,
        swarm_status,
    })
}

pub fn to_u32_i32(value: i32) -> u32 {
    u32::try_from(value).map_or_else(|_| 0, |v| v)
}

#[cfg(test)]
mod tests {
    use super::to_u32_i32;

    #[test]
    fn signed_to_unsigned_helpers_clamp_at_zero() {
        assert_eq!(to_u32_i32(3), 3);
        assert_eq!(to_u32_i32(-2), 0);
    }
}
