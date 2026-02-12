use crate::{
    Result, RuntimeAgentId, RuntimeAgentState, RuntimeBeadId, RuntimeStage, RuntimeStageResult,
};
use std::future::Future;
use std::pin::Pin;

pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageExecutionOutcome {
    Progressed,
    Idle,
}

impl StageExecutionOutcome {
    #[must_use]
    pub const fn is_progressed(self) -> bool {
        matches!(self, Self::Progressed)
    }
}

#[derive(Debug, Clone)]
pub struct StageExecutionRequest {
    agent_id: RuntimeAgentId,
    state: RuntimeAgentState,
}

impl StageExecutionRequest {
    #[must_use]
    pub const fn new(agent_id: RuntimeAgentId, state: RuntimeAgentState) -> Self {
        Self { agent_id, state }
    }

    #[must_use]
    pub const fn agent_id(&self) -> &RuntimeAgentId {
        &self.agent_id
    }

    #[must_use]
    pub const fn state(&self) -> &RuntimeAgentState {
        &self.state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageArtifactRecord {
    bead_id: RuntimeBeadId,
    stage: RuntimeStage,
    result: RuntimeStageResult,
    body: String,
}

impl StageArtifactRecord {
    #[must_use]
    pub fn new(
        bead_id: RuntimeBeadId,
        stage: RuntimeStage,
        result: RuntimeStageResult,
        body: impl Into<String>,
    ) -> Self {
        Self {
            bead_id,
            stage,
            result,
            body: body.into(),
        }
    }

    #[must_use]
    pub const fn bead_id(&self) -> &RuntimeBeadId {
        &self.bead_id
    }

    #[must_use]
    pub const fn stage(&self) -> RuntimeStage {
        self.stage
    }

    #[must_use]
    pub const fn result(&self) -> &RuntimeStageResult {
        &self.result
    }

    #[must_use]
    pub fn body(&self) -> &str {
        &self.body
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LandingOutcome {
    push_confirmed: bool,
    detail: String,
}

impl LandingOutcome {
    #[must_use]
    pub fn new(push_confirmed: bool, detail: impl Into<String>) -> Self {
        Self {
            push_confirmed,
            detail: detail.into(),
        }
    }

    #[must_use]
    pub const fn push_confirmed(&self) -> bool {
        self.push_confirmed
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrchestratorEvent {
    ClaimRecovered {
        count: u32,
    },
    BeadClaimed {
        agent_id: RuntimeAgentId,
        bead_id: RuntimeBeadId,
    },
    StageExecuted {
        agent_id: RuntimeAgentId,
        bead_id: RuntimeBeadId,
        outcome: StageExecutionOutcome,
    },
}

pub trait ClaimRepository {
    fn recover_stale_claims<'a>(&'a self, repo_id: &'a crate::RuntimeRepoId)
        -> PortFuture<'a, u32>;

    fn get_agent_state<'a>(
        &'a self,
        agent_id: &'a RuntimeAgentId,
    ) -> PortFuture<'a, Option<RuntimeAgentState>>;

    fn claim_next_bead<'a>(
        &'a self,
        agent_id: &'a RuntimeAgentId,
    ) -> PortFuture<'a, Option<RuntimeBeadId>>;

    fn create_workspace<'a>(
        &'a self,
        agent_id: &'a RuntimeAgentId,
        bead_id: &'a RuntimeBeadId,
    ) -> PortFuture<'a, ()>;

    fn heartbeat_claim<'a>(
        &'a self,
        agent_id: &'a RuntimeAgentId,
        bead_id: &'a RuntimeBeadId,
        lease_extension_ms: i32,
    ) -> PortFuture<'a, bool>;
}

pub trait StageExecutor {
    fn execute_work(&self, request: StageExecutionRequest)
        -> PortFuture<'_, StageExecutionOutcome>;
}

pub trait ArtifactStore {
    fn store_artifact(&self, record: StageArtifactRecord) -> PortFuture<'_, ()>;
}

pub trait LandingGateway {
    fn execute_landing<'a>(&'a self, bead_id: &'a RuntimeBeadId) -> PortFuture<'a, LandingOutcome>;
}

pub trait EventSink {
    fn append_event(&self, event: OrchestratorEvent) -> PortFuture<'_, ()>;
}

pub trait OrchestratorPorts:
    ClaimRepository + StageExecutor + ArtifactStore + LandingGateway + EventSink
{
}

impl<T> OrchestratorPorts for T where
    T: ClaimRepository + StageExecutor + ArtifactStore + LandingGateway + EventSink
{
}
