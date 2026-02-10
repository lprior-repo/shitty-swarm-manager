use crate::error::{Result, SwarmError};
use crate::types::{AgentState, AgentStatus, BeadId, Stage, SwarmConfig, SwarmStatus};

pub struct AgentStateFields {
    pub bead_id: Option<String>,
    pub stage_str: Option<String>,
    pub stage_started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub status_str: String,
    pub last_update: chrono::DateTime<chrono::Utc>,
    pub implementation_attempt: i32,
    pub feedback: Option<String>,
}

pub fn parse_agent_state(
    agent_id: &crate::types::AgentId,
    fields: AgentStateFields,
) -> Result<AgentState> {
    let status =
        AgentStatus::try_from(fields.status_str.as_str()).map_err(SwarmError::DatabaseError)?;

    Ok(AgentState {
        agent_id: agent_id.clone(),
        bead_id: fields.bead_id.map(BeadId::new),
        current_stage: fields
            .stage_str
            .and_then(|s| Stage::try_from(s.as_str()).ok()),
        stage_started_at: fields.stage_started_at,
        status,
        last_update: fields.last_update,
        implementation_attempt: to_u32_i32(fields.implementation_attempt),
        feedback: fields.feedback,
    })
}

pub fn parse_swarm_config(
    max_agents: i32,
    max_attempts: i32,
    claim_label: String,
    swarm_started_at: Option<chrono::DateTime<chrono::Utc>>,
    status_str: &str,
) -> Result<SwarmConfig> {
    let swarm_status = SwarmStatus::try_from(status_str).map_err(SwarmError::DatabaseError)?;

    Ok(SwarmConfig {
        repo_id: crate::types::RepoId::new("local"),
        max_agents: to_u32_i32(max_agents),
        max_implementation_attempts: to_u32_i32(max_attempts),
        claim_label,
        swarm_started_at,
        swarm_status,
    })
}

pub const fn to_u32_i32(value: i32) -> u32 {
    if value < 0 {
        0
    } else {
        value.cast_unsigned()
    }
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
