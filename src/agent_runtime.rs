use crate::error::Result;
use crate::types::AgentId;
use crate::SwarmDb;

/// # Errors
/// Returns database or runtime errors from stage execution.
pub async fn run_agent(db: &SwarmDb, agent_id: &AgentId, _stage_commands: &[String]) -> Result<()> {
    let _ = db;
    let _ = agent_id;
    Ok(())
}

/// # Errors
/// Returns database or runtime errors from stage execution.
pub async fn run_smoke_once(db: &SwarmDb, agent_id: &AgentId) -> Result<()> {
    run_agent(db, agent_id, &[]).await
}
