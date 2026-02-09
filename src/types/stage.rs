use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Stage {
    RustContract,
    Implement,
    QaEnforcer,
    RedQueen,
    Done,
}

impl Stage {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RustContract => "rust-contract",
            Self::Implement => "implement",
            Self::QaEnforcer => "qa-enforcer",
            Self::RedQueen => "red-queen",
            Self::Done => "done",
        }
    }

    pub fn next(&self) -> Option<Self> {
        match self {
            Self::RustContract => Some(Self::Implement),
            Self::Implement => Some(Self::QaEnforcer),
            Self::QaEnforcer => Some(Self::RedQueen),
            Self::RedQueen => Some(Self::Done),
            Self::Done => None,
        }
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for Stage {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "rust-contract" => Ok(Self::RustContract),
            "implement" => Ok(Self::Implement),
            "qa-enforcer" => Ok(Self::QaEnforcer),
            "red-queen" => Ok(Self::RedQueen),
            "done" => Ok(Self::Done),
            _ => Err(format!("Unknown stage: {}", s)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageResult {
    Started,
    Passed,
    Failed(String),
    Error(String),
}

impl StageResult {
    pub fn as_str(&self) -> String {
        match self {
            Self::Started => "started".to_string(),
            Self::Passed => "passed".to_string(),
            Self::Failed(_) => "failed".to_string(),
            Self::Error(_) => "error".to_string(),
        }
    }

    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Failed(msg) | Self::Error(msg) => Some(msg),
            _ => None,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Passed)
    }
}

#[cfg(test)]
mod tests {
    use super::{Stage, StageResult};

    #[test]
    fn stage_progression_and_string_roundtrip_work() {
        assert_eq!(Stage::RustContract.as_str(), "rust-contract");
        assert_eq!(Stage::RustContract.next(), Some(Stage::Implement));
        assert_eq!(Stage::Implement.next(), Some(Stage::QaEnforcer));
        assert_eq!(Stage::QaEnforcer.next(), Some(Stage::RedQueen));
        assert_eq!(Stage::RedQueen.next(), Some(Stage::Done));
        assert_eq!(Stage::Done.next(), None);
        assert_eq!(Stage::try_from("red-queen"), Ok(Stage::RedQueen));
    }

    #[test]
    fn stage_result_helpers_match_semantics() {
        let passed = StageResult::Passed;
        let failed = StageResult::Failed("oops".to_string());
        let errored = StageResult::Error("boom".to_string());

        assert_eq!(passed.as_str(), "passed");
        assert!(passed.message().is_none());
        assert!(passed.is_success());

        assert_eq!(failed.as_str(), "failed");
        assert_eq!(failed.message(), Some("oops"));
        assert!(!failed.is_success());

        assert_eq!(errored.as_str(), "error");
        assert_eq!(errored.message(), Some("boom"));
        assert!(!errored.is_success());
    }
}
