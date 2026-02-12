#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

mod stage;
mod stage_result;
mod stage_transition;
mod transition_decision;

pub use stage::Stage;
pub use stage_result::StageResult;
pub use stage_transition::StageTransition;
pub use transition_decision::{
    decision_from_stage_dag, passed_stage_transition,
    validate_completion_requires_push_confirmation, TransitionDecision, TransitionReason,
};

#[cfg(test)]
mod tests;
