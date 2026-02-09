use super::identifiers::{AgentId, BeadId, RepoId};
use super::stage::Stage;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AgentStatus {
    Idle,
    Working,
    Waiting,
    Error,
    Done,
}

impl AgentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::Waiting => "waiting",
            Self::Error => "error",
            Self::Done => "done",
        }
    }
}

impl fmt::Display for AgentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
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
            _ => Err(format!("Unknown status: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentState {
    pub agent_id: AgentId,
    pub bead_id: Option<BeadId>,
    pub current_stage: Option<Stage>,
    pub stage_started_at: Option<DateTime<Utc>>,
    pub status: AgentStatus,
    pub last_update: DateTime<Utc>,
    pub implementation_attempt: u32,
    pub feedback: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeadClaim {
    pub bead_id: BeadId,
    pub repo_id: RepoId,
    pub claimed_by: u32,
    pub claimed_at: DateTime<Utc>,
    pub status: ClaimStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimStatus {
    InProgress,
    Completed,
    Blocked,
}

impl ClaimStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
        }
    }
}

impl TryFrom<&str> for ClaimStatus {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "in_progress" => Ok(Self::InProgress),
            "completed" => Ok(Self::Completed),
            "blocked" => Ok(Self::Blocked),
            _ => Err(format!("Unknown claim status: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmConfig {
    pub repo_id: RepoId,
    pub max_agents: u32,
    pub max_implementation_attempts: u32,
    pub claim_label: String,
    pub swarm_started_at: Option<DateTime<Utc>>,
    pub swarm_status: SwarmStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SwarmStatus {
    Initializing,
    Running,
    Paused,
    Complete,
    Error,
}

impl SwarmStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Initializing => "initializing",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Complete => "complete",
            Self::Error => "error",
        }
    }
}

impl TryFrom<&str> for SwarmStatus {
    type Error = String;

    fn try_from(s: &str) -> std::result::Result<Self, String> {
        match s {
            "initializing" => Ok(Self::Initializing),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "complete" => Ok(Self::Complete),
            "error" => Ok(Self::Error),
            _ => Err(format!("Unknown swarm status: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressSummary {
    pub completed: u64,
    pub working: u64,
    pub waiting: u64,
    pub errors: u64,
    pub idle: u64,
    pub total_agents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailableAgent {
    pub repo_id: RepoId,
    pub agent_id: u32,
    pub status: AgentStatus,
    pub implementation_attempt: u32,
    pub max_implementation_attempts: u32,
    pub max_agents: u32,
}

#[cfg(test)]
mod tests {
    use super::{AgentStatus, ClaimStatus, SwarmStatus};

    #[test]
    fn status_parsing_handles_valid_and_invalid_values() {
        assert_eq!(AgentStatus::try_from("idle"), Ok(AgentStatus::Idle));
        assert_eq!(SwarmStatus::try_from("running"), Ok(SwarmStatus::Running));
        assert!(AgentStatus::try_from("bogus").is_err());
        assert!(SwarmStatus::try_from("bogus").is_err());
    }

    #[test]
    fn claim_status_roundtrip_and_invalid_value() {
        assert_eq!(ClaimStatus::InProgress.as_str(), "in_progress");
        assert_eq!(ClaimStatus::Completed.as_str(), "completed");
        assert_eq!(ClaimStatus::Blocked.as_str(), "blocked");
        assert_eq!(
            ClaimStatus::try_from("in_progress"),
            Ok(ClaimStatus::InProgress)
        );
        assert_eq!(
            ClaimStatus::try_from("completed"),
            Ok(ClaimStatus::Completed)
        );
        assert_eq!(ClaimStatus::try_from("blocked"), Ok(ClaimStatus::Blocked));
        assert!(ClaimStatus::try_from("invalid").is_err());
    }
}
