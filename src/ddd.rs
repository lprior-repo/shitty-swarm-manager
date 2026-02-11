#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("Repository error: {0}")]
    RepositoryError(String),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;

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
    pub repo_id: RuntimeRepoId,
    pub number: u32,
}

impl RuntimeAgentId {
    #[must_use]
    pub const fn new(repo_id: RuntimeRepoId, number: u32) -> Self {
        Self { repo_id, number }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeStage {
    RustContract,
    Implement,
    QaEnforcer,
    RedQueen,
    Done,
}

impl RuntimeStage {
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeStageResult {
    Started,
    Passed,
    Failed(String),
    Error(String),
}

impl RuntimeStageResult {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeAgentStatus {
    Idle,
    Working,
    Waiting,
    Error,
    Done,
}

impl RuntimeAgentStatus {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeAgentState {
    pub agent_id: RuntimeAgentId,
    pub bead_id: Option<RuntimeBeadId>,
    pub current_stage: Option<RuntimeStage>,
    pub status: RuntimeAgentStatus,
    pub implementation_attempt: u32,
}

pub enum RuntimeStageTransition {
    Advance(RuntimeStage),
    Retry,
    Complete,
    Block,
    NoOp,
}

pub fn runtime_determine_transition(
    stage: RuntimeStage,
    result: &RuntimeStageResult,
    attempt: u32,
    max_attempts: u32,
) -> RuntimeStageTransition {
    if !result.is_success() {
        return if attempt >= max_attempts {
            RuntimeStageTransition::Block
        } else {
            RuntimeStageTransition::Retry
        };
    }

    if stage == RuntimeStage::RedQueen {
        return RuntimeStageTransition::Complete;
    }

    stage.next().map_or(
        RuntimeStageTransition::NoOp,
        RuntimeStageTransition::Advance,
    )
}

pub struct RuntimePgAgentRepository {
    pool: PgPool,
}

impl RuntimePgAgentRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl RuntimePgAgentRepository {
    /// Finds the current runtime state for a specific agent id.
    ///
    /// # Errors
    /// Returns `RuntimeError::RepositoryError` when the database query fails.
    pub async fn find_by_id(&self, agent_id: &RuntimeAgentId) -> Result<Option<RuntimeAgentState>> {
        sqlx::query_as::<_, (Option<String>, Option<String>, String, i32)>(
            "SELECT bead_id, current_stage, status, implementation_attempt FROM agent_state WHERE agent_id = $1",
        )
        .bind(agent_id.number().cast_signed())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| RuntimeError::RepositoryError(format!("find_agent: {e}")))
        .map(|opt| {
            opt.map(|(bead_id, current_stage, status, impl_attempt)| RuntimeAgentState {
                agent_id: agent_id.clone(),
                bead_id: bead_id.map(RuntimeBeadId::new),
                current_stage: current_stage.and_then(|s| s.as_str().try_into().ok()),
                status: status
                    .as_str()
                    .try_into()
                    .unwrap_or(RuntimeAgentStatus::Error),
                implementation_attempt: if impl_attempt < 0 {
                    0
                } else {
                    impl_attempt.cast_unsigned()
                },
            })
        })
    }

    /// Updates a runtime agent status and refreshes `last_update`.
    ///
    /// # Errors
    /// Returns `RuntimeError::RepositoryError` when the database query fails.
    pub async fn update_status(
        &self,
        agent_id: &RuntimeAgentId,
        status: RuntimeAgentStatus,
    ) -> Result<()> {
        sqlx::query("UPDATE agent_state SET status = $2, last_update = NOW() WHERE agent_id = $1")
            .bind(agent_id.number().cast_signed())
            .bind(status.as_str())
            .execute(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("update_status: {e}")))
            .map(|_| ())
    }
}

pub struct RuntimePgBeadRepository {
    pool: PgPool,
}

impl RuntimePgBeadRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl RuntimePgBeadRepository {
    /// Attempts to claim the next pending P0 bead for the given agent.
    ///
    /// # Errors
    /// Returns `RuntimeError::RepositoryError` when the database query fails.
    pub async fn claim_next(&self, agent_id: &RuntimeAgentId) -> Result<Option<RuntimeBeadId>> {
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_p0_bead($1)")
            .bind(agent_id.number().cast_signed())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("claim_next: {e}")))
            .map(|opt| opt.map(RuntimeBeadId::new))
    }

    /// Releases the bead currently assigned to the given agent.
    ///
    /// # Errors
    /// Returns `RuntimeError::RepositoryError` when the database query fails.
    pub async fn release(&self, agent_id: &RuntimeAgentId) -> Result<()> {
        sqlx::query("UPDATE agent_state SET bead_id = NULL, current_stage = NULL, status = 'idle' WHERE agent_id = $1")
            .bind(agent_id.number().cast_signed())
            .execute(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("release: {e}")))
            .map(|_| ())
    }

    /// Marks a bead as blocked after max retries or manual intervention.
    ///
    /// # Errors
    /// Returns `RuntimeError::RepositoryError` when the database query fails.
    pub async fn mark_blocked(&self, bead_id: &RuntimeBeadId, _reason: &str) -> Result<()> {
        sqlx::query("UPDATE bead_backlog SET status = 'blocked' WHERE bead_id = $1")
            .bind(bead_id.value())
            .execute(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("mark_blocked: {e}")))
            .map(|_| ())
    }
}

pub struct RuntimePgStageRepository {
    pool: PgPool,
}

impl RuntimePgStageRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl RuntimePgStageRepository {
    /// Records that a stage attempt has started.
    ///
    /// # Errors
    /// Returns `RuntimeError::RepositoryError` when the insert fails.
    pub async fn record_started(
        &self,
        agent_id: &RuntimeAgentId,
        bead_id: &RuntimeBeadId,
        stage: RuntimeStage,
        attempt: u32,
    ) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            "INSERT INTO stage_history (agent_id, bead_id, stage, attempt_number, status) VALUES ($1, $2, $3, $4, 'started') RETURNING id",
        )
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt.cast_signed())
        .fetch_one(&self.pool)
        .await
        .map_err(|e| RuntimeError::RepositoryError(format!("record_started: {e}")))
    }

    /// Records completion metadata for the most recent started stage attempt.
    ///
    /// # Errors
    /// Returns `RuntimeError::RepositoryError` when the update fails.
    pub async fn record_completed(
        &self,
        agent_id: &RuntimeAgentId,
        bead_id: &RuntimeBeadId,
        stage: RuntimeStage,
        attempt: u32,
        result: RuntimeStageResult,
        duration_ms: u64,
    ) -> Result<()> {
        sqlx::query("UPDATE stage_history SET status = $5, result = $6, feedback = $7, completed_at = NOW(), duration_ms = $8 WHERE id = (SELECT id FROM stage_history WHERE agent_id = $1 AND bead_id = $2 AND stage = $3 AND attempt_number = $4 AND status = 'started' ORDER BY started_at DESC LIMIT 1)")
            .bind(agent_id.number().cast_signed())
            .bind(bead_id.value())
            .bind(stage.as_str())
            .bind(attempt.cast_signed())
            .bind(result.message().map_or("passed", |_| "failed"))
            .bind(result.message())
            .bind(result.message())
            .bind(duration_ms.cast_signed())
            .execute(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("record_completed: {e}")))
            .map(|_| ())
    }
}

impl TryFrom<&str> for RuntimeStage {
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

impl TryFrom<&str> for RuntimeAgentStatus {
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
