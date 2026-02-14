#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::runtime::shared::{RuntimeAgentId, RuntimeBeadId, RuntimeError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentStatus {
    Idle,
    Working,
    Waiting,
    Error,
    Done,
}

impl AgentStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::Waiting => "waiting",
            Self::Error => "error",
            Self::Done => "done",
        }
    }

    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Working | Self::Waiting | Self::Error)
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Done)
    }
}

impl TryFrom<&str> for AgentStatus {
    type Error = String;

    fn try_from(s: &str) -> std::result::Result<Self, String> {
        match s {
            "idle" => Ok(Self::Idle),
            "working" => Ok(Self::Working),
            "waiting" => Ok(Self::Waiting),
            "error" => Ok(Self::Error),
            "done" => Ok(Self::Done),
            _ => Err(format!("Unknown status: {s}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentState {
    agent_id: RuntimeAgentId,
    bead_id: Option<RuntimeBeadId>,
    current_stage: Option<crate::runtime::stage::Stage>,
    status: AgentStatus,
    implementation_attempt: u32,
}

impl AgentState {
    #[must_use]
    pub const fn new(
        agent_id: RuntimeAgentId,
        bead_id: Option<RuntimeBeadId>,
        current_stage: Option<crate::runtime::stage::Stage>,
        status: AgentStatus,
        implementation_attempt: u32,
    ) -> Self {
        Self {
            agent_id,
            bead_id,
            current_stage,
            status,
            implementation_attempt,
        }
    }

    #[must_use]
    pub const fn agent_id(&self) -> &RuntimeAgentId {
        &self.agent_id
    }

    #[must_use]
    pub const fn bead_id(&self) -> Option<&RuntimeBeadId> {
        self.bead_id.as_ref()
    }

    #[must_use]
    pub const fn current_stage(&self) -> Option<crate::runtime::stage::Stage> {
        self.current_stage
    }

    #[must_use]
    pub const fn status(&self) -> AgentStatus {
        self.status
    }

    #[must_use]
    pub const fn implementation_attempt(&self) -> u32 {
        self.implementation_attempt
    }

    #[must_use]
    pub const fn is_working(&self) -> bool {
        matches!(self.status, AgentStatus::Working)
    }

    #[must_use]
    pub const fn can_retry(&self, max_attempts: u32) -> bool {
        self.implementation_attempt < max_attempts
    }

    #[must_use]
    pub const fn has_bead(&self) -> bool {
        self.bead_id.is_some()
    }

    /// # Errors
    /// Returns an error if agent state invariants are violated.
    pub fn validate_invariants(&self) -> crate::runtime::shared::Result<()> {
        match self.status {
            AgentStatus::Working => {
                if self.bead_id.is_none() {
                    return Err(RuntimeError::InvariantViolation(
                        "Agent with Working status must have a bead".to_string(),
                    ));
                }
                if self.current_stage.is_none() {
                    return Err(RuntimeError::InvariantViolation(
                        "Agent with Working status must have a current_stage".to_string(),
                    ));
                }
            }
            AgentStatus::Done => {
                if self.bead_id.is_some() {
                    return Err(RuntimeError::InvariantViolation(
                        "Agent with Done status must not have a bead".to_string(),
                    ));
                }
                if self.current_stage.is_some()
                    && self.current_stage != Some(crate::runtime::stage::Stage::Done)
                {
                    return Err(RuntimeError::InvariantViolation(
                        "Agent with Done status must have current_stage = Done or None".to_string(),
                    ));
                }
            }
            AgentStatus::Idle | AgentStatus::Waiting | AgentStatus::Error => {
                if self.bead_id.is_some() {
                    return Err(RuntimeError::InvariantViolation(format!(
                        "Agent with {:?} status must not have a bead",
                        self.status
                    )));
                }
            }
        }
        Ok(())
    }
}
