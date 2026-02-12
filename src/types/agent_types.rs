#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::identifiers::{AgentId, BeadId};
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
            _ => Err(format!("Unknown status: {s}")),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_status_roundtrip_preserves_values() {
        let cases = [
            (AgentStatus::Idle, "idle"),
            (AgentStatus::Working, "working"),
            (AgentStatus::Waiting, "waiting"),
            (AgentStatus::Error, "error"),
            (AgentStatus::Done, "done"),
        ];

        for (status, expected) in cases {
            assert_eq!(status.as_str(), expected);
            assert_eq!(AgentStatus::try_from(expected), Ok(status));
        }
    }

    #[test]
    fn agent_status_rejects_invalid_values() {
        let invalid = ["invalid", "IDLE", "Working", "", "done "];
        for value in invalid {
            assert!(AgentStatus::try_from(value).is_err());
        }
    }

    #[test]
    fn agent_status_display_matches_as_str() {
        let statuses = [
            AgentStatus::Idle,
            AgentStatus::Working,
            AgentStatus::Waiting,
            AgentStatus::Error,
            AgentStatus::Done,
        ];

        for status in statuses {
            assert_eq!(format!("{status}"), status.as_str());
        }
    }
}
