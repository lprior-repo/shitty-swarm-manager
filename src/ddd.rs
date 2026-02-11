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
    #[error("Domain invariant violation: {0}")]
    InvariantViolation(String),
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

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Done)
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

    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Working | Self::Waiting | Self::Error)
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Done)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeAgentState {
    agent_id: RuntimeAgentId,
    bead_id: Option<RuntimeBeadId>,
    current_stage: Option<RuntimeStage>,
    status: RuntimeAgentStatus,
    implementation_attempt: u32,
}

impl RuntimeAgentState {
    #[must_use]
    pub const fn new(
        agent_id: RuntimeAgentId,
        bead_id: Option<RuntimeBeadId>,
        current_stage: Option<RuntimeStage>,
        status: RuntimeAgentStatus,
        implementation_attempt: u32,
    ) -> Self {
        Self {
            agent_id,
            bead_id,
            current_stage,
            status,
            implementation_attempt,
        }
    }

    #[must_use]
    pub const fn agent_id(&self) -> &RuntimeAgentId {
        &self.agent_id
    }

    #[must_use]
    pub const fn bead_id(&self) -> Option<&RuntimeBeadId> {
        self.bead_id.as_ref()
    }

    #[must_use]
    pub const fn current_stage(&self) -> Option<RuntimeStage> {
        self.current_stage
    }

    #[must_use]
    pub const fn status(&self) -> RuntimeAgentStatus {
        self.status
    }

    #[must_use]
    pub const fn implementation_attempt(&self) -> u32 {
        self.implementation_attempt
    }

    #[must_use]
    pub const fn is_working(&self) -> bool {
        matches!(self.status, RuntimeAgentStatus::Working)
    }

    #[must_use]
    pub const fn can_retry(&self, max_attempts: u32) -> bool {
        self.implementation_attempt < max_attempts
    }

    #[must_use]
    pub const fn has_bead(&self) -> bool {
        self.bead_id.is_some()
    }

    /// # Errors
    /// Returns [`RuntimeError::InvariantViolation`] when agent state breaks domain invariants.
    pub fn validate_invariants(&self) -> Result<()> {
        match self.status {
            RuntimeAgentStatus::Working => {
                if self.bead_id.is_none() {
                    return Err(RuntimeError::InvariantViolation(
                        "Agent with Working status must have a bead".to_string(),
                    ));
                }
                if self.current_stage.is_none() {
                    return Err(RuntimeError::InvariantViolation(
                        "Agent with Working status must have a current_stage".to_string(),
                    ));
                }
            }
            RuntimeAgentStatus::Done => {
                if self.bead_id.is_some() {
                    return Err(RuntimeError::InvariantViolation(
                        "Agent with Done status must not have a bead".to_string(),
                    ));
                }
                if self.current_stage.is_some() && self.current_stage != Some(RuntimeStage::Done) {
                    return Err(RuntimeError::InvariantViolation(
                        "Agent with Done status must have current_stage = Done or None".to_string(),
                    ));
                }
            }
            RuntimeAgentStatus::Idle | RuntimeAgentStatus::Waiting | RuntimeAgentStatus::Error => {
                if self.bead_id.is_some() {
                    return Err(RuntimeError::InvariantViolation(format!(
                        "Agent with {:?} status must not have a bead",
                        self.status
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeStageTransition {
    Advance(RuntimeStage),
    Retry,
    Complete,
    Block,
    NoOp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BeadExecutionStatus {
    Active,
    Blocked,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadExecution {
    current_stage: RuntimeStage,
    implementation_attempt: u32,
    max_implementation_attempts: u32,
    status: BeadExecutionStatus,
}

impl BeadExecution {
    /// # Errors
    /// Returns [`RuntimeError::InvariantViolation`] when aggregate state violates contracts.
    pub fn new(
        current_stage: RuntimeStage,
        implementation_attempt: u32,
        max_implementation_attempts: u32,
        status: BeadExecutionStatus,
    ) -> Result<Self> {
        let execution = Self {
            current_stage,
            implementation_attempt,
            max_implementation_attempts,
            status,
        };
        execution.validate_invariants()?;
        Ok(execution)
    }

    #[must_use]
    pub const fn current_stage(&self) -> RuntimeStage {
        self.current_stage
    }

    #[must_use]
    pub const fn implementation_attempt(&self) -> u32 {
        self.implementation_attempt
    }

    #[must_use]
    pub const fn max_implementation_attempts(&self) -> u32 {
        self.max_implementation_attempts
    }

    #[must_use]
    pub const fn status(&self) -> BeadExecutionStatus {
        self.status
    }

    /// # Errors
    /// Returns [`RuntimeError::InvariantViolation`] when aggregate state or result input is invalid.
    pub fn determine_transition(
        &self,
        result: &RuntimeStageResult,
    ) -> Result<RuntimeTransitionDecision> {
        self.validate_invariants()?;

        if matches!(result, RuntimeStageResult::Started) {
            return Err(RuntimeError::InvariantViolation(
                "Stage result Started cannot produce a transition decision".to_string(),
            ));
        }

        let retry_exhausted = self.implementation_attempt >= self.max_implementation_attempts;
        Ok(decision_from_stage_dag(
            self.current_stage,
            result.is_success(),
            retry_exhausted,
        ))
    }

    /// # Errors
    /// Returns [`RuntimeError::InvariantViolation`] when aggregate state violates contracts.
    pub fn validate_invariants(&self) -> Result<()> {
        if self.max_implementation_attempts == 0 {
            return Err(RuntimeError::InvariantViolation(
                "BeadExecution max_implementation_attempts must be greater than zero".to_string(),
            ));
        }

        if self.implementation_attempt > self.max_implementation_attempts {
            return Err(RuntimeError::InvariantViolation(format!(
                "BeadExecution implementation_attempt {} exceeds max_implementation_attempts {}",
                self.implementation_attempt, self.max_implementation_attempts
            )));
        }

        if self.status == BeadExecutionStatus::Completed && self.current_stage != RuntimeStage::Done
        {
            return Err(RuntimeError::InvariantViolation(
                "BeadExecution with Completed status must be in Done stage".to_string(),
            ));
        }

        if self.current_stage == RuntimeStage::Done && self.status != BeadExecutionStatus::Completed
        {
            return Err(RuntimeError::InvariantViolation(
                "BeadExecution in Done stage must have Completed status".to_string(),
            ));
        }

        if self.status == BeadExecutionStatus::Blocked && self.current_stage == RuntimeStage::Done {
            return Err(RuntimeError::InvariantViolation(
                "BeadExecution cannot be Blocked in Done stage".to_string(),
            ));
        }

        Ok(())
    }
}

const fn decision_from_stage_dag(
    stage: RuntimeStage,
    is_success: bool,
    retry_exhausted: bool,
) -> RuntimeTransitionDecision {
    if is_success {
        return passed_stage_transition(stage);
    }

    if retry_exhausted {
        return RuntimeTransitionDecision::new(
            RuntimeStageTransition::Block,
            RuntimeTransitionReason::StageFailedMaxAttemptsReached,
        );
    }

    RuntimeTransitionDecision::new(
        RuntimeStageTransition::Retry,
        RuntimeTransitionReason::StageFailedRetry,
    )
}

const fn passed_stage_transition(stage: RuntimeStage) -> RuntimeTransitionDecision {
    match stage {
        RuntimeStage::RustContract => RuntimeTransitionDecision::new(
            RuntimeStageTransition::Advance(RuntimeStage::Implement),
            RuntimeTransitionReason::StagePassedAdvance,
        ),
        RuntimeStage::Implement => RuntimeTransitionDecision::new(
            RuntimeStageTransition::Advance(RuntimeStage::QaEnforcer),
            RuntimeTransitionReason::StagePassedAdvance,
        ),
        RuntimeStage::QaEnforcer => RuntimeTransitionDecision::new(
            RuntimeStageTransition::Advance(RuntimeStage::RedQueen),
            RuntimeTransitionReason::StagePassedAdvance,
        ),
        RuntimeStage::RedQueen => RuntimeTransitionDecision::new(
            RuntimeStageTransition::Complete,
            RuntimeTransitionReason::RedQueenPassedComplete,
        ),
        RuntimeStage::Done => RuntimeTransitionDecision::new(
            RuntimeStageTransition::NoOp,
            RuntimeTransitionReason::StagePassedNoNextStage,
        ),
    }
}

impl RuntimeStageTransition {
    #[must_use]
    pub const fn is_no_op(&self) -> bool {
        matches!(self, Self::NoOp)
    }

    #[must_use]
    pub const fn should_advance(&self) -> bool {
        matches!(self, Self::Advance(_))
    }

    #[must_use]
    pub const fn should_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    #[must_use]
    pub const fn should_block(&self) -> bool {
        matches!(self, Self::Block)
    }
}

/// # Errors
/// Returns [`RuntimeError::InvariantViolation`] when completion is requested
/// without landing push confirmation.
pub fn validate_completion_implies_push_confirmed(
    transition: RuntimeStageTransition,
    push_confirmed: bool,
) -> Result<()> {
    if transition.should_complete() && !push_confirmed {
        return Err(RuntimeError::InvariantViolation(
            "completion_implies_push_confirmed violated: completion requires push confirmation"
                .to_string(),
        ));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeTransitionReason {
    StagePassedAdvance,
    StagePassedNoNextStage,
    RedQueenPassedComplete,
    StageFailedRetry,
    StageFailedMaxAttemptsReached,
}

impl RuntimeTransitionReason {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::StagePassedAdvance => "stage_passed_advance",
            Self::StagePassedNoNextStage => "stage_passed_no_next_stage",
            Self::RedQueenPassedComplete => "red_queen_passed_complete",
            Self::StageFailedRetry => "stage_failed_retry",
            Self::StageFailedMaxAttemptsReached => "stage_failed_max_attempts_reached",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTransitionDecision {
    transition: RuntimeStageTransition,
    reason: RuntimeTransitionReason,
}

impl RuntimeTransitionDecision {
    #[must_use]
    pub const fn new(transition: RuntimeStageTransition, reason: RuntimeTransitionReason) -> Self {
        Self { transition, reason }
    }

    #[must_use]
    pub const fn transition(&self) -> RuntimeStageTransition {
        self.transition
    }

    #[must_use]
    pub const fn reason(&self) -> RuntimeTransitionReason {
        self.reason
    }

    #[must_use]
    pub const fn reason_code(&self) -> &'static str {
        self.reason.code()
    }
}

#[must_use]
pub fn runtime_determine_transition_decision(
    stage: RuntimeStage,
    result: &RuntimeStageResult,
    attempt: u32,
    max_attempts: u32,
) -> RuntimeTransitionDecision {
    let status = if stage == RuntimeStage::Done {
        BeadExecutionStatus::Completed
    } else {
        BeadExecutionStatus::Active
    };

    let computed_decision =
        BeadExecution::new(stage, attempt, max_attempts, status).and_then(|execution| {
            if matches!(result, RuntimeStageResult::Started) {
                return Err(RuntimeError::InvariantViolation(
                    "Stage result Started cannot produce a transition decision".to_string(),
                ));
            }

            Ok(decision_from_stage_dag(
                execution.current_stage(),
                result.is_success(),
                execution.implementation_attempt() >= execution.max_implementation_attempts(),
            ))
        });

    if let Ok(decision) = computed_decision {
        return decision;
    }

    RuntimeTransitionDecision::new(
        RuntimeStageTransition::Block,
        RuntimeTransitionReason::StageFailedMaxAttemptsReached,
    )
}

#[must_use]
pub fn runtime_determine_transition(
    stage: RuntimeStage,
    result: &RuntimeStageResult,
    attempt: u32,
    max_attempts: u32,
) -> RuntimeStageTransition {
    runtime_determine_transition_decision(stage, result, attempt, max_attempts).transition()
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
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl RuntimePgAgentRepository {
    /// # Errors
    /// Returns [`RuntimeError::RepositoryError`] for database or mapping failures.
    /// Returns [`RuntimeError::InvariantViolation`] when persisted state violates invariants.
    pub async fn find_by_id(&self, agent_id: &RuntimeAgentId) -> Result<Option<RuntimeAgentState>> {
        let maybe_row = sqlx::query_as::<_, (Option<String>, Option<String>, String, i32)>(
            "SELECT bead_id, current_stage, status, implementation_attempt FROM agent_state WHERE agent_id = $1",
        )
        .bind(agent_id.number().cast_signed())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| RuntimeError::RepositoryError(format!("find_agent: {e}")))?;

        maybe_row.map_or(
            Ok(None),
            |(bead_id, current_stage, status, impl_attempt)| {
                let parsed_stage = current_stage
                    .map(|stage| {
                        stage.as_str().try_into().map_err(|err: String| {
                            RuntimeError::RepositoryError(format!(
                                "find_agent invalid stage '{stage}': {err}"
                            ))
                        })
                    })
                    .transpose()?;

                let parsed_status = status.as_str().try_into().map_err(|err: String| {
                    RuntimeError::RepositoryError(format!(
                        "find_agent invalid status '{status}': {err}"
                    ))
                })?;

                let implementation_attempt = if impl_attempt < 0 {
                    0
                } else {
                    impl_attempt.cast_unsigned()
                };

                let state = RuntimeAgentState::new(
                    agent_id.clone(),
                    bead_id.map(RuntimeBeadId::new),
                    parsed_stage,
                    parsed_status,
                    implementation_attempt,
                );

                state.validate_invariants()?;
                Ok(Some(state))
            },
        )
    }

    /// # Errors
    /// Returns [`RuntimeError::RepositoryError`] when persistence fails.
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

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl RuntimePgBeadRepository {
    /// # Errors
    /// Returns [`RuntimeError::RepositoryError`] when persistence fails.
    pub async fn claim_next(&self, agent_id: &RuntimeAgentId) -> Result<Option<RuntimeBeadId>> {
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_p0_bead($1)")
            .bind(agent_id.number().cast_signed())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("claim_next: {e}")))
            .map(|opt| opt.map(RuntimeBeadId::new))
    }

    /// # Errors
    /// Returns [`RuntimeError::RepositoryError`] when persistence fails.
    pub async fn release(&self, agent_id: &RuntimeAgentId) -> Result<()> {
        sqlx::query("UPDATE agent_state SET bead_id = NULL, current_stage = NULL, status = 'idle' WHERE agent_id = $1")
            .bind(agent_id.number().cast_signed())
            .execute(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("release: {e}")))
            .map(|_| ())
    }

    /// # Errors
    /// Returns [`RuntimeError::RepositoryError`] when persistence fails.
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

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl RuntimePgStageRepository {
    /// # Errors
    /// Returns [`RuntimeError::RepositoryError`] when persistence fails.
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

    /// # Errors
    /// Returns [`RuntimeError::RepositoryError`] when persistence fails.
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

#[cfg(test)]
mod tests {
    use super::{
        runtime_determine_transition, runtime_determine_transition_decision,
        validate_completion_implies_push_confirmed, BeadExecution, BeadExecutionStatus,
        RuntimeStage, RuntimeStageResult, RuntimeStageTransition, RuntimeTransitionReason,
    };

    #[test]
    fn bead_execution_requires_positive_attempt_budget() {
        let result = BeadExecution::new(RuntimeStage::Implement, 0, 0, BeadExecutionStatus::Active);

        assert!(result.is_err());
    }

    #[test]
    fn bead_execution_completed_status_requires_done_stage() {
        let result = BeadExecution::new(
            RuntimeStage::Implement,
            1,
            3,
            BeadExecutionStatus::Completed,
        );

        assert!(result.is_err());
    }

    #[test]
    fn bead_execution_transition_advances_without_skipping() {
        let decision =
            BeadExecution::new(RuntimeStage::Implement, 1, 3, BeadExecutionStatus::Active)
                .and_then(|execution| execution.determine_transition(&RuntimeStageResult::Passed));

        assert!(matches!(
            decision,
            Ok(outcome)
                if outcome.transition() == RuntimeStageTransition::Advance(RuntimeStage::QaEnforcer)
        ));
    }

    #[test]
    fn bead_execution_transition_completes_only_from_red_queen_pass() {
        let decision =
            BeadExecution::new(RuntimeStage::RedQueen, 1, 3, BeadExecutionStatus::Active)
                .and_then(|execution| execution.determine_transition(&RuntimeStageResult::Passed));

        assert!(matches!(
            decision,
            Ok(outcome) if outcome.transition() == RuntimeStageTransition::Complete
        ));
    }

    #[test]
    fn bead_execution_transition_rejects_started_result_input() {
        let decision =
            BeadExecution::new(RuntimeStage::Implement, 1, 3, BeadExecutionStatus::Active)
                .and_then(|execution| execution.determine_transition(&RuntimeStageResult::Started));

        assert!(decision.is_err());
    }

    #[test]
    fn failed_stage_retries_when_attempt_budget_remains() {
        let decision = runtime_determine_transition_decision(
            RuntimeStage::Implement,
            &RuntimeStageResult::Failed("needs work".to_string()),
            1,
            3,
        );

        assert_eq!(decision.transition(), RuntimeStageTransition::Retry);
        assert_eq!(decision.reason(), RuntimeTransitionReason::StageFailedRetry);
        assert_eq!(decision.reason_code(), "stage_failed_retry");
    }

    #[test]
    fn failed_stage_blocks_when_attempt_budget_is_exhausted() {
        let decision = runtime_determine_transition_decision(
            RuntimeStage::QaEnforcer,
            &RuntimeStageResult::Error("quality gate failed".to_string()),
            3,
            3,
        );

        assert_eq!(decision.transition(), RuntimeStageTransition::Block);
        assert_eq!(
            decision.reason(),
            RuntimeTransitionReason::StageFailedMaxAttemptsReached
        );
        assert_eq!(decision.reason_code(), "stage_failed_max_attempts_reached");
    }

    #[test]
    fn successful_red_queen_stage_completes() {
        let decision = runtime_determine_transition_decision(
            RuntimeStage::RedQueen,
            &RuntimeStageResult::Passed,
            1,
            3,
        );

        assert_eq!(decision.transition(), RuntimeStageTransition::Complete);
        assert_eq!(
            decision.reason(),
            RuntimeTransitionReason::RedQueenPassedComplete
        );
    }

    #[test]
    fn successful_non_terminal_stage_advances_to_next_stage() {
        let decision = runtime_determine_transition_decision(
            RuntimeStage::RustContract,
            &RuntimeStageResult::Passed,
            1,
            3,
        );

        assert_eq!(
            decision.transition(),
            RuntimeStageTransition::Advance(RuntimeStage::Implement)
        );
        assert_eq!(
            decision.reason(),
            RuntimeTransitionReason::StagePassedAdvance
        );
    }

    #[test]
    fn successful_done_stage_is_a_no_op() {
        let decision = runtime_determine_transition_decision(
            RuntimeStage::Done,
            &RuntimeStageResult::Passed,
            1,
            3,
        );

        assert_eq!(decision.transition(), RuntimeStageTransition::NoOp);
        assert_eq!(
            decision.reason(),
            RuntimeTransitionReason::StagePassedNoNextStage
        );
    }

    #[test]
    fn compatibility_transition_api_returns_decision_transition() {
        let transition = runtime_determine_transition(
            RuntimeStage::Implement,
            &RuntimeStageResult::Passed,
            1,
            3,
        );

        assert_eq!(
            transition,
            RuntimeStageTransition::Advance(RuntimeStage::QaEnforcer)
        );
    }

    #[test]
    fn deterministic_stage_dag_pass_paths_have_explicit_reason_codes() {
        let cases = [
            (
                RuntimeStage::RustContract,
                RuntimeStageTransition::Advance(RuntimeStage::Implement),
                RuntimeTransitionReason::StagePassedAdvance,
            ),
            (
                RuntimeStage::Implement,
                RuntimeStageTransition::Advance(RuntimeStage::QaEnforcer),
                RuntimeTransitionReason::StagePassedAdvance,
            ),
            (
                RuntimeStage::QaEnforcer,
                RuntimeStageTransition::Advance(RuntimeStage::RedQueen),
                RuntimeTransitionReason::StagePassedAdvance,
            ),
            (
                RuntimeStage::RedQueen,
                RuntimeStageTransition::Complete,
                RuntimeTransitionReason::RedQueenPassedComplete,
            ),
        ];

        for (stage, expected_transition, expected_reason) in cases {
            let decision =
                runtime_determine_transition_decision(stage, &RuntimeStageResult::Passed, 1, 3);
            assert_eq!(decision.transition(), expected_transition);
            assert_eq!(decision.reason(), expected_reason);
        }
    }

    #[test]
    fn deterministic_stage_dag_failure_paths_retry_and_block_with_reason_codes() {
        let stages = [
            RuntimeStage::RustContract,
            RuntimeStage::Implement,
            RuntimeStage::QaEnforcer,
            RuntimeStage::RedQueen,
        ];

        for stage in stages {
            let retry = runtime_determine_transition_decision(
                stage,
                &RuntimeStageResult::Failed("retry please".to_string()),
                1,
                3,
            );
            assert_eq!(retry.transition(), RuntimeStageTransition::Retry);
            assert_eq!(retry.reason_code(), "stage_failed_retry");

            let block = runtime_determine_transition_decision(
                stage,
                &RuntimeStageResult::Error("attempt budget spent".to_string()),
                3,
                3,
            );
            assert_eq!(block.transition(), RuntimeStageTransition::Block);
            assert_eq!(block.reason_code(), "stage_failed_max_attempts_reached");
        }
    }

    #[test]
    fn completion_transition_requires_push_confirmation() {
        let validation =
            validate_completion_implies_push_confirmed(RuntimeStageTransition::Complete, false);

        assert!(validation.is_err());
    }

    #[test]
    fn completion_transition_allows_push_confirmed() {
        let validation =
            validate_completion_implies_push_confirmed(RuntimeStageTransition::Complete, true);

        assert!(validation.is_ok());
    }

    #[test]
    fn non_completion_transition_does_not_require_push_confirmation() {
        let validation =
            validate_completion_implies_push_confirmed(RuntimeStageTransition::Retry, false);

        assert!(validation.is_ok());
    }
}
