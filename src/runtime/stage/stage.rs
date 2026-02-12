#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Stage {
    RustContract,
    Implement,
    QaEnforcer,
    RedQueen,
    Done,
}

impl Stage {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RustContract => "rust-contract",
            Self::Implement => "implement",
            Self::QaEnforcer => "qa-enforcer",
            Self::RedQueen => "red-queen",
            Self::Done => "done",
        }
    }

    #[must_use]
    pub const fn next(&self) -> Option<Self> {
        match self {
            Self::RustContract => Some(Self::Implement),
            Self::Implement => Some(Self::QaEnforcer),
            Self::QaEnforcer => Some(Self::RedQueen),
            Self::RedQueen => Some(Self::Done),
            Self::Done => None,
        }
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Done)
    }
}

impl TryFrom<&str> for Stage {
    type Error = String;

    fn try_from(s: &str) -> std::result::Result<Self, String> {
        match s {
            "rust-contract" => Ok(Self::RustContract),
            "implement" => Ok(Self::Implement),
            "qa-enforcer" => Ok(Self::QaEnforcer),
            "red-queen" => Ok(Self::RedQueen),
            "done" => Ok(Self::Done),
            _ => Err(format!("Unknown stage: {s}")),
        }
    }
}
