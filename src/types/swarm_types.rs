#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::agent_types::AgentStatus;
use super::identifiers::RepoId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
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
            _ => Err(format!("Unknown swarm status: {s}")),
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
    use super::*;

    #[test]
    fn swarm_status_roundtrip_preserves_values() {
        let cases = [
            (SwarmStatus::Initializing, "initializing"),
            (SwarmStatus::Running, "running"),
            (SwarmStatus::Paused, "paused"),
            (SwarmStatus::Complete, "complete"),
            (SwarmStatus::Error, "error"),
        ];

        for (status, expected) in cases {
            assert_eq!(status.as_str(), expected);
            assert_eq!(SwarmStatus::try_from(expected), Ok(status));
        }
    }

    #[test]
    fn swarm_status_rejects_invalid_values() {
        let invalid = ["invalid", "INITIALIZING", "Running", "", "error "];
        for value in invalid {
            assert!(SwarmStatus::try_from(value).is_err());
        }
    }

    #[test]
    fn progress_summary_aggregates_correctly() {
        let summary = ProgressSummary {
            completed: 10,
            working: 5,
            waiting: 3,
            errors: 2,
            idle: 0,
            total_agents: 20,
        };

        assert_eq!(summary.completed, 10);
        assert_eq!(summary.working, 5);
        assert_eq!(summary.total_agents, 20);
    }
}
