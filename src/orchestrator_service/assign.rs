use super::ports::PortFuture;
use crate::{Result, RuntimeAgentStatus, RuntimeRepoId};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct AssignCommand {
    pub repo_id: RuntimeRepoId,
    pub bead_id: String,
    pub agent_id: u32,
}

#[derive(Debug, Clone)]
pub struct AssignAgentSnapshot {
    pub valid_ids: Vec<u32>,
    pub status: RuntimeAgentStatus,
    pub current_bead: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AssignResult {
    pub bead_id: String,
    pub agent_id: u32,
    pub assignee: String,
    pub br_update: Value,
    pub bead_verify: Value,
    pub verified_status: Option<String>,
    pub verified_id: Option<String>,
}

pub trait AssignPorts {
    fn load_agent_snapshot<'a>(
        &'a self,
        repo_id: &'a RuntimeRepoId,
        agent_id: u32,
    ) -> PortFuture<'a, Option<AssignAgentSnapshot>>;

    fn br_show_bead<'a>(&'a self, bead_id: &'a str) -> PortFuture<'a, Value>;

    fn claim_bead<'a>(
        &'a self,
        repo_id: &'a RuntimeRepoId,
        agent_id: u32,
        bead_id: &'a str,
    ) -> PortFuture<'a, bool>;

    fn release_agent<'a>(&'a self, repo_id: &'a RuntimeRepoId, agent_id: u32)
        -> PortFuture<'a, ()>;

    fn br_assign_in_progress<'a>(
        &'a self,
        bead_id: &'a str,
        assignee: &'a str,
    ) -> PortFuture<'a, Value>;
}

pub struct AssignAppService<P> {
    ports: P,
}

impl<P> AssignAppService<P>
where
    P: AssignPorts + Sync,
{
    #[must_use]
    pub const fn new(ports: P) -> Self {
        Self { ports }
    }

    /// Execute one explicit assign command through repository and external ports.
    ///
    /// # Errors
    /// Returns an error when agent/bead preconditions are not met or when
    /// claim/sync side effects fail.
    pub async fn execute<S, I>(
        &self,
        command: AssignCommand,
        issue_status_from_payload: S,
        issue_id_from_payload: I,
    ) -> Result<AssignResult>
    where
        S: Fn(&Value) -> Option<String>,
        I: Fn(&Value) -> Option<String>,
    {
        let snapshot = self
            .ports
            .load_agent_snapshot(&command.repo_id, command.agent_id)
            .await?
            .ok_or_else(|| {
                crate::Error::BeadError(format!("Agent {} is not registered", command.agent_id))
            })?;

        if snapshot.status != RuntimeAgentStatus::Idle || snapshot.current_bead.is_some() {
            return Err(crate::Error::AgentError(format!(
                "Agent {} is not idle",
                command.agent_id
            )));
        }

        let bead_before = self.ports.br_show_bead(&command.bead_id).await?;
        let current_status = issue_status_from_payload(&bead_before).ok_or_else(|| {
            crate::Error::ConfigError("br show returned payload without status".to_string())
        })?;

        if current_status != "open" {
            return Err(crate::Error::StageError(format!(
                "Bead {} is not assignable: status={current_status}",
                command.bead_id
            )));
        }

        let claimed = self
            .ports
            .claim_bead(&command.repo_id, command.agent_id, &command.bead_id)
            .await?;
        if !claimed {
            return Err(crate::Error::StageError(format!(
                "Failed to claim bead {} for agent {}",
                command.bead_id, command.agent_id
            )));
        }

        let assignee = format!("swarm-agent-{}", command.agent_id);
        let update_result = self
            .ports
            .br_assign_in_progress(&command.bead_id, assignee.as_str())
            .await;

        let br_update = match update_result {
            Ok(value) => value,
            Err(err) => {
                let _ = self
                    .ports
                    .release_agent(&command.repo_id, command.agent_id)
                    .await;
                return Err(err);
            }
        };

        let bead_verify = self.ports.br_show_bead(&command.bead_id).await?;
        let verified_status = issue_status_from_payload(&bead_verify);
        let verified_id = issue_id_from_payload(&bead_verify);

        Ok(AssignResult {
            bead_id: command.bead_id,
            agent_id: command.agent_id,
            assignee,
            br_update,
            bead_verify,
            verified_status,
            verified_id,
        })
    }
}
