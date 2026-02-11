#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

pub mod beads_sync;
pub mod canonical_schema;
pub mod ddd;

pub use beads_sync::{
    map_terminal_sync_state, BrSyncAction, BrSyncDecision, BrSyncDivergence, BrSyncStatus,
    CoordinatorSyncTerminal,
};
pub use ddd::{
    runtime_determine_transition, runtime_determine_transition_decision, RuntimeAgentId,
    RuntimeAgentState, RuntimeAgentStatus, RuntimeBeadId, RuntimeError, RuntimePgAgentRepository,
    RuntimePgBeadRepository, RuntimePgStageRepository, RuntimeRepoId, RuntimeStage,
    RuntimeStageResult, RuntimeStageTransition, RuntimeTransitionDecision, RuntimeTransitionReason,
};

pub use canonical_schema::CANONICAL_COORDINATOR_SCHEMA_PATH;


pub use error::Result;
pub use error::SwarmError as Error;
pub use error::{code, SwarmError, ERROR_CODES};

pub mod contracts;
pub mod db;
pub mod diagnostics;
mod error;
pub mod gate_cache;
pub mod orchestrator_service;
pub mod prompts;
pub mod protocol_envelope;
pub mod skill_execution;
pub mod skill_execution_parsing;
pub mod skill_prompts;
pub mod stage_executor_content;
pub mod stage_executors;
pub mod types;

pub use contracts::*;
pub use db::SwarmDb;
pub use gate_cache::GateExecutionCache;
pub use orchestrator_service::{
    ArtifactStore, ClaimRepository, EventSink, LandingGateway, LandingOutcome, OrchestratorEvent,
    OrchestratorPorts, OrchestratorService, OrchestratorTickOutcome, StageArtifactRecord,
    StageExecutionOutcome, StageExecutionRequest, StageExecutor,
};

pub use types::{
    AgentId, AgentMessage, AgentState, AgentStatus, ArtifactType, BeadId, ClaimStatus,
    DeepResumeContextContract, EventSchemaVersion, ExecutionEvent, FailureDiagnostics, MessageType,
    ProgressSummary, RepoId, ResumeArtifactDetailContract, ResumeArtifactSummary,
    ResumeArtifactSummaryContract, ResumeContextContract, ResumeContextProjection,
    ResumeStageAttempt, ResumeStageAttemptContract, Stage, StageArtifact, StageResult, SwarmConfig,
    SwarmStatus,
};
