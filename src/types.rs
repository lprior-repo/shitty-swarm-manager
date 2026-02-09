use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Repository identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RepoId(String);

impl RepoId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn value(&self) -> &str {
        &self.0
    }

    /// Create from current git remote or directory
    pub fn from_current_dir() -> Option<Self> {
        // Try git remote first
        if let Ok(output) = std::process::Command::new("git")
            .args(["remote", "get-url", "origin"])
            .output()
        {
            if output.status.success() {
                let url = String::from_utf8_lossy(&output.stdout);
                return Some(Self::new(url.trim()));
            }
        }

        // Fall back to directory name
        if let Ok(cwd) = std::env::current_dir() {
            if let Some(name) = cwd.file_name() {
                return Some(Self::new(name.to_string_lossy()));
            }
        }

        None
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Agent identifier (repo-scoped)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId {
    repo_id: RepoId,
    number: u32,
}

impl AgentId {
    pub fn new(repo_id: RepoId, number: u32) -> Self {
        Self { repo_id, number }
    }

    pub fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub fn number(&self) -> u32 {
        self.number
    }

    pub fn to_db_agent_id(&self) -> i32 {
        self.number as i32
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.repo_id, self.number)
    }
}

/// Bead identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BeadId(String);

impl BeadId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn value(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BeadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Pipeline stage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Stage {
    Contract,
    Implement,
    Test,
    Qa,
    Done,
}

impl Stage {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Contract => "contract",
            Self::Implement => "implement",
            Self::Test => "test",
            Self::Qa => "qa",
            Self::Done => "done",
        }
    }

    pub fn next(&self) -> Option<Self> {
        match self {
            Self::Contract => Some(Self::Implement),
            Self::Implement => Some(Self::Test),
            Self::Test => Some(Self::Qa),
            Self::Qa => Some(Self::Done),
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
            "contract" => Ok(Self::Contract),
            "implement" => Ok(Self::Implement),
            "test" => Ok(Self::Test),
            "qa" => Ok(Self::Qa),
            "done" => Ok(Self::Done),
            _ => Err(format!("Unknown stage: {}", s)),
        }
    }
}

/// Agent status
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

    fn try_from(s: &str) -> Result<Self, Self::Error> {
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

/// Stage execution result
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

/// Agent state
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

/// Bead claim
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

/// Swarm configuration
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

    fn try_from(s: &str) -> Result<Self, Self::Error> {
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

/// Progress summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressSummary {
    pub completed: u64,
    pub working: u64,
    pub waiting: u64,
    pub errors: u64,
    pub idle: u64,
    pub total_agents: u64,
}

/// Available agent for claiming work
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailableAgent {
    pub repo_id: RepoId,
    pub agent_id: u32,
    pub status: AgentStatus,
    pub implementation_attempt: u32,
    pub max_implementation_attempts: u32,
    pub max_agents: u32,
}
