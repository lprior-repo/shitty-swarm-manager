#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::{Stage, StageTransition};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransitionReason {
    StagePassedAdvance,
    StagePassedNoNextStage,
    RedQueenPassedComplete,
    StageFailedRetry,
    StageFailedMaxAttemptsReached,
}

impl TransitionReason {
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
pub struct TransitionDecision {
    transition: StageTransition,
    reason: TransitionReason,
}

impl TransitionDecision {
    #[must_use]
    pub const fn new(transition: StageTransition, reason: TransitionReason) -> Self {
        Self { transition, reason }
    }

    #[must_use]
    pub const fn transition(&self) -> StageTransition {
        self.transition
    }

    #[must_use]
    pub const fn reason(&self) -> TransitionReason {
        self.reason
    }

    #[must_use]
    pub const fn reason_code(&self) -> &'static str {
        self.reason.code()
    }
}

#[must_use]
pub const fn decision_from_stage_dag(
    stage: Stage,
    is_success: bool,
    retry_exhausted: bool,
) -> TransitionDecision {
    if is_success {
        return passed_stage_transition(stage);
    }

    if retry_exhausted {
        return TransitionDecision::new(
            StageTransition::Block,
            TransitionReason::StageFailedMaxAttemptsReached,
        );
    }

    TransitionDecision::new(StageTransition::Retry, TransitionReason::StageFailedRetry)
}

#[must_use]
pub const fn passed_stage_transition(stage: Stage) -> TransitionDecision {
    match stage {
        Stage::RustContract => TransitionDecision::new(
            StageTransition::Advance(Stage::Implement),
            TransitionReason::StagePassedAdvance,
        ),
        Stage::Implement => TransitionDecision::new(
            StageTransition::Advance(Stage::QaEnforcer),
            TransitionReason::StagePassedAdvance,
        ),
        Stage::QaEnforcer => TransitionDecision::new(
            StageTransition::Advance(Stage::RedQueen),
            TransitionReason::StagePassedAdvance,
        ),
        Stage::RedQueen => TransitionDecision::new(
            StageTransition::Complete,
            TransitionReason::RedQueenPassedComplete,
        ),
        Stage::Done => TransitionDecision::new(
            StageTransition::NoOp,
            TransitionReason::StagePassedNoNextStage,
        ),
    }
}

/// # Errors
/// Returns an error if the transition requires push confirmation but it was not provided.
pub fn validate_completion_requires_push_confirmation(
    transition: StageTransition,
    push_confirmed: bool,
) -> crate::runtime::shared::Result<()> {
    if transition.should_complete() && !push_confirmed {
        return Err(crate::runtime::shared::RuntimeError::InvariantViolation(
            "completion_implies_push_confirmed violated: completion requires push confirmation"
                .to_string(),
        ));
    }

    Ok(())
}
