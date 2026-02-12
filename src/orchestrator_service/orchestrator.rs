use super::ports::{OrchestratorPorts, StageExecutionRequest};
use crate::{Result, RuntimeAgentId, RuntimeAgentStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrchestratorTickOutcome {
    AgentMissing,
    Progressed,
    Idle,
    Completed,
}

pub struct OrchestratorService<P> {
    ports: P,
}

impl<P> OrchestratorService<P>
where
    P: OrchestratorPorts + Sync,
{
    const LEASE_EXTENSION_MS: i32 = 300_000;

    #[must_use]
    pub const fn new(ports: P) -> Self {
        Self { ports }
    }

    /// Advance exactly one deterministic orchestration tick.
    ///
    /// # Errors
    /// Returns any infrastructure/port failure without mutating service decision state.
    pub async fn tick(&self, agent_id: &RuntimeAgentId) -> Result<OrchestratorTickOutcome> {
        self.ports.recover_stale_claims(agent_id.repo_id()).await?;
        let maybe_state = self.ports.get_agent_state(agent_id).await?;

        match maybe_state {
            None => Ok(OrchestratorTickOutcome::AgentMissing),
            Some(state) => match state.status() {
                RuntimeAgentStatus::Idle => {
                    let maybe_bead = self.ports.claim_next_bead(agent_id).await?;
                    if let Some(bead_id) = maybe_bead {
                        self.ports.create_workspace(agent_id, &bead_id).await?;
                        Ok(OrchestratorTickOutcome::Progressed)
                    } else {
                        Ok(OrchestratorTickOutcome::Idle)
                    }
                }
                RuntimeAgentStatus::Done => Ok(OrchestratorTickOutcome::Completed),
                RuntimeAgentStatus::Working | RuntimeAgentStatus::Waiting => {
                    if let Some(bead_id) = state.bead_id() {
                        let heartbeat_ok = self
                            .ports
                            .heartbeat_claim(agent_id, bead_id, Self::LEASE_EXTENSION_MS)
                            .await?;
                        if !heartbeat_ok {
                            return Ok(OrchestratorTickOutcome::Idle);
                        }
                    }

                    let execution = self
                        .ports
                        .execute_work(StageExecutionRequest::new(agent_id.clone(), state))
                        .await?;
                    if execution.is_progressed() {
                        Ok(OrchestratorTickOutcome::Progressed)
                    } else {
                        Ok(OrchestratorTickOutcome::Idle)
                    }
                }
                RuntimeAgentStatus::Error => Ok(OrchestratorTickOutcome::Idle),
            },
        }
    }
}
