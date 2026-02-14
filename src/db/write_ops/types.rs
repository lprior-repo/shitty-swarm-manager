#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::types::Stage;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StageTransition {
    #[error("finalize")]
    Finalize,
    #[error("advance to {0}")]
    Advance(Stage),
    #[error("retry implement")]
    RetryImplement,
    #[error("no op")]
    NoOp,
}

#[derive(Debug, Clone)]
pub struct FailureDiagnosticsPayload {
    pub category: String,
    pub retryable: bool,
    pub next_command: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExecutionEventWriteInput {
    pub stage: Option<Stage>,
    pub event_type: &'static str,
    pub causation_id: Option<String>,
    pub payload: serde_json::Value,
    pub diagnostics: Option<FailureDiagnosticsPayload>,
}

pub struct StageTransitionInput<'a> {
    pub transition: &'a StageTransition,
    pub agent_id: &'a crate::types::AgentId,
    pub bead_id: &'a crate::types::BeadId,
    pub stage: Stage,
    pub stage_history_id: Option<i64>,
    pub attempt: u32,
    pub message: Option<&'a str>,
}
