#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::runtime::bead::{BeadExecution, BeadExecutionStatus};
use crate::runtime::shared::RuntimeError;
use crate::runtime::stage::{Stage, StageResult, StageTransition, TransitionDecision};

/// # Errors
/// Returns an error if the transition requires push confirmation but it was not provided.
pub fn validate_completion_requires_push_confirmation(
    transition: StageTransition,
    push_confirmed: bool,
) -> crate::runtime::shared::Result<()> {
    if transition.should_complete() && !push_confirmed {
        return Err(RuntimeError::InvariantViolation(
            "completion_requires_push_confirmed violated: completion requires push confirmation"
                .to_string(),
        ));
    }

    Ok(())
}

#[must_use]
pub fn runtime_determine_transition_decision(
    stage: Stage,
    result: &StageResult,
    attempt: u32,
    max_attempts: u32,
) -> TransitionDecision {
    let status = if stage == Stage::Done {
        BeadExecutionStatus::Completed
    } else {
        BeadExecutionStatus::Active
    };

    let computed_decision =
        BeadExecution::new(stage, attempt, max_attempts, status).and_then(|execution| {
            if matches!(result, StageResult::Started) {
                return Err(RuntimeError::InvariantViolation(
                    "Stage result Started cannot produce a transition decision".to_string(),
                ));
            }

            Ok(crate::runtime::stage::decision_from_stage_dag(
                execution.current_stage(),
                result.is_success(),
                execution.implementation_attempt() >= execution.max_implementation_attempts(),
            ))
        });

    computed_decision.unwrap_or_else(|_| {
        TransitionDecision::new(
            StageTransition::Block,
            crate::runtime::stage::TransitionReason::StageFailedMaxAttemptsReached,
        )
    })
}

#[must_use]
pub fn runtime_determine_transition(
    stage: Stage,
    result: &StageResult,
    attempt: u32,
    max_attempts: u32,
) -> StageTransition {
    runtime_determine_transition_decision(stage, result, attempt, max_attempts).transition()
}
