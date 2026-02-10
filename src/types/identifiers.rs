use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RepoId(String);

impl RepoId {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn from_current_dir() -> Option<Self> {
        if let Ok(output) = std::process::Command::new("git")
            .args(["remote", "get-url", "origin"])
            .output()
        {
            if output.status.success() {
                let url = String::from_utf8_lossy(&output.stdout);
                return Some(Self::new(url.trim()));
            }
        }

        std::env::current_dir().ok().and_then(|cwd| {
            cwd.file_name()
                .map(|name| Self::new(name.to_string_lossy().to_string()))
        })
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId {
    repo_id: RepoId,
    number: u32,
}

impl AgentId {
    #[must_use]
    pub const fn new(repo_id: RepoId, number: u32) -> Self {
        Self { repo_id, number }
    }

    #[must_use]
    pub const fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    #[must_use]
    pub const fn number(&self) -> u32 {
        self.number
    }

    #[must_use]
    pub const fn to_db_agent_id(&self) -> i32 {
        self.number.cast_signed()
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.repo_id, self.number)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BeadId(String);

impl BeadId {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BeadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
