#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::identifiers::{BeadId, RepoId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
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
            _ => Err(format!("Unknown claim status: {s}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_status_roundtrip_preserves_values() {
        let cases = [
            (ClaimStatus::InProgress, "in_progress"),
            (ClaimStatus::Completed, "completed"),
            (ClaimStatus::Blocked, "blocked"),
        ];

        for (status, expected) in cases {
            assert_eq!(status.as_str(), expected);
            assert_eq!(ClaimStatus::try_from(expected), Ok(status));
        }
    }

    #[test]
    fn claim_status_rejects_invalid_values() {
        let invalid = ["invalid", "IN_PROGRESS", "Completed", "", "blocked "];
        for value in invalid {
            assert!(ClaimStatus::try_from(value).is_err());
        }
    }
}
