#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageResult {
    Started,
    Passed,
    Failed(String),
    Error(String),
}

impl StageResult {
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Passed)
    }

    #[must_use]
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Failed(m) | Self::Error(m) => Some(m),
            _ => None,
        }
    }
}
