#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeRepoId(String);

impl RuntimeRepoId {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeAgentId {
    repo_id: RuntimeRepoId,
    number: u32,
}

impl RuntimeAgentId {
    #[must_use]
    pub const fn new(repo_id: RuntimeRepoId, number: u32) -> Self {
        Self { repo_id, number }
    }

    #[must_use]
    pub const fn repo_id(&self) -> &RuntimeRepoId {
        &self.repo_id
    }

    #[must_use]
    pub const fn number(&self) -> u32 {
        self.number
    }
}

impl std::fmt::Display for RuntimeAgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.repo_id.value(), self.number)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeBeadId(String);

impl RuntimeBeadId {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.0
    }
}
