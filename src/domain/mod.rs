#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

pub mod agent;
pub mod bead;
pub mod shared;
pub mod stage;

pub use agent::{AgentState, AgentStatus};
pub use bead::{BeadExecution, BeadExecutionStatus};
pub use shared::{Result, RuntimeAgentId, RuntimeBeadId, RuntimeError, RuntimeRepoId};
pub use stage::{
    decision_from_stage_dag, passed_stage_transition,
    validate_completion_requires_push_confirmation, Stage, StageResult, StageTransition,
    TransitionDecision, TransitionReason,
};
