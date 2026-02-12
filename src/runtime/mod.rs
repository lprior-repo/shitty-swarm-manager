#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

pub mod agent;
pub mod bead;
pub mod repositories;
pub mod shared;
pub mod stage;
pub mod transition;

pub use agent::{AgentState as RuntimeAgentState, AgentStatus as RuntimeAgentStatus};
pub use bead::{BeadExecution, BeadExecutionStatus};
pub use repositories::{
    RuntimePgAgentRepository, RuntimePgBeadRepository, RuntimePgStageRepository,
};
pub use shared::{Result, RuntimeAgentId, RuntimeBeadId, RuntimeError, RuntimeRepoId};
pub use stage::{
    decision_from_stage_dag, passed_stage_transition, Stage as RuntimeStage,
    StageResult as RuntimeStageResult, StageTransition as RuntimeStageTransition,
    TransitionDecision as RuntimeTransitionDecision, TransitionReason as RuntimeTransitionReason,
};
pub use transition::{
    runtime_determine_transition, runtime_determine_transition_decision,
    validate_completion_requires_push_confirmation,
};
