use crate::{
    Result, RuntimeAgentId, RuntimeAgentState, RuntimeAgentStatus, RuntimeBeadId, RuntimeStage,
    RuntimeStageResult,
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
    fn recover_stale_claims(&self) -> PortFuture<'_, u32>;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrchestratorTickOutcome {
    AgentMissing,
    Progressed,
    Idle,
    Completed,
}

pub struct OrchestratorService<P> {
    ports: P,
}

impl<P> OrchestratorService<P>
where
    P: OrchestratorPorts + Sync,
{
    const LEASE_EXTENSION_MS: i32 = 300_000;

    #[must_use]
    pub const fn new(ports: P) -> Self {
        Self { ports }
    }

    /// Advance exactly one deterministic orchestration tick.
    ///
    /// # Errors
    /// Returns any infrastructure/port failure without mutating service decision state.
    pub async fn tick(&self, agent_id: &RuntimeAgentId) -> Result<OrchestratorTickOutcome> {
        self.ports.recover_stale_claims().await?;
        let maybe_state = self.ports.get_agent_state(agent_id).await?;

        match maybe_state {
            None => Ok(OrchestratorTickOutcome::AgentMissing),
            Some(state) => match state.status() {
                RuntimeAgentStatus::Idle => {
                    let maybe_bead = self.ports.claim_next_bead(agent_id).await?;
                    if let Some(bead_id) = maybe_bead {
                        self.ports.create_workspace(agent_id, &bead_id).await?;
                        Ok(OrchestratorTickOutcome::Progressed)
                    } else {
                        Ok(OrchestratorTickOutcome::Idle)
                    }
                }
                RuntimeAgentStatus::Done => Ok(OrchestratorTickOutcome::Completed),
                RuntimeAgentStatus::Working | RuntimeAgentStatus::Waiting => {
                    if let Some(bead_id) = state.bead_id() {
                        let heartbeat_ok = self
                            .ports
                            .heartbeat_claim(agent_id, bead_id, Self::LEASE_EXTENSION_MS)
                            .await?;
                        if !heartbeat_ok {
                            return Ok(OrchestratorTickOutcome::Idle);
                        }
                    }

                    let execution = self
                        .ports
                        .execute_work(StageExecutionRequest::new(agent_id.clone(), state))
                        .await?;
                    if execution.is_progressed() {
                        Ok(OrchestratorTickOutcome::Progressed)
                    } else {
                        Ok(OrchestratorTickOutcome::Idle)
                    }
                }
                RuntimeAgentStatus::Error => Ok(OrchestratorTickOutcome::Idle),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactStore, ClaimRepository, EventSink, LandingGateway, LandingOutcome,
        OrchestratorEvent, OrchestratorPorts, OrchestratorService, OrchestratorTickOutcome,
        PortFuture, StageArtifactRecord, StageExecutionOutcome, StageExecutionRequest,
        StageExecutor,
    };
    use crate::{
        Error, Result, RuntimeAgentId, RuntimeAgentState, RuntimeAgentStatus, RuntimeBeadId,
        RuntimeRepoId, RuntimeStage,
    };
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone)]
    struct FakePorts {
        state: Arc<Mutex<Option<RuntimeAgentState>>>,
        claim_result: Arc<Mutex<Option<RuntimeBeadId>>>,
        progressed: Arc<Mutex<bool>>,
        fail_on_execute: Arc<Mutex<bool>>,
        recover_count: Arc<Mutex<u32>>,
        heartbeat_ok: Arc<Mutex<bool>>,
        heartbeat_calls: Arc<Mutex<Vec<(u32, String, i32)>>>,
        workspace_calls: Arc<Mutex<Vec<(u32, String)>>>,
    }

    impl FakePorts {
        fn new(state: Option<RuntimeAgentState>) -> Self {
            Self {
                state: Arc::new(Mutex::new(state)),
                claim_result: Arc::new(Mutex::new(None)),
                progressed: Arc::new(Mutex::new(false)),
                fail_on_execute: Arc::new(Mutex::new(false)),
                recover_count: Arc::new(Mutex::new(0)),
                heartbeat_ok: Arc::new(Mutex::new(true)),
                heartbeat_calls: Arc::new(Mutex::new(Vec::new())),
                workspace_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        async fn with_claim(self, bead_id: RuntimeBeadId) -> Self {
            let mut claim = self.claim_result.lock().await;
            *claim = Some(bead_id);
            drop(claim);
            self
        }

        async fn with_progressed(self, progressed: bool) -> Self {
            let mut current = self.progressed.lock().await;
            *current = progressed;
            drop(current);
            self
        }

        async fn with_execute_failure(self, fail: bool) -> Self {
            let mut current = self.fail_on_execute.lock().await;
            *current = fail;
            drop(current);
            self
        }

        async fn with_heartbeat_ok(self, heartbeat_ok: bool) -> Self {
            let mut current = self.heartbeat_ok.lock().await;
            *current = heartbeat_ok;
            drop(current);
            self
        }
    }

    impl ClaimRepository for FakePorts {
        fn recover_stale_claims(&self) -> PortFuture<'_, u32> {
            Box::pin(async move {
                let mut recovered = self.recover_count.lock().await;
                *recovered = recovered.saturating_add(1);
                Ok(0)
            })
        }

        fn get_agent_state<'a>(
            &'a self,
            _agent_id: &'a RuntimeAgentId,
        ) -> PortFuture<'a, Option<RuntimeAgentState>> {
            Box::pin(async move { Ok(self.state.lock().await.clone()) })
        }

        fn claim_next_bead<'a>(
            &'a self,
            _agent_id: &'a RuntimeAgentId,
        ) -> PortFuture<'a, Option<RuntimeBeadId>> {
            Box::pin(async move { Ok(self.claim_result.lock().await.clone()) })
        }

        fn create_workspace<'a>(
            &'a self,
            agent_id: &'a RuntimeAgentId,
            bead_id: &'a RuntimeBeadId,
        ) -> PortFuture<'a, ()> {
            Box::pin(async move {
                let mut calls = self.workspace_calls.lock().await;
                calls.push((agent_id.number(), bead_id.value().to_string()));
                Ok(())
            })
        }

        fn heartbeat_claim<'a>(
            &'a self,
            agent_id: &'a RuntimeAgentId,
            bead_id: &'a RuntimeBeadId,
            lease_extension_ms: i32,
        ) -> PortFuture<'a, bool> {
            Box::pin(async move {
                let mut calls = self.heartbeat_calls.lock().await;
                calls.push((
                    agent_id.number(),
                    bead_id.value().to_string(),
                    lease_extension_ms,
                ));
                Ok(*self.heartbeat_ok.lock().await)
            })
        }
    }

    impl StageExecutor for FakePorts {
        fn execute_work(
            &self,
            _request: StageExecutionRequest,
        ) -> PortFuture<'_, StageExecutionOutcome> {
            Box::pin(async move {
                if *self.fail_on_execute.lock().await {
                    return Err(Error::Internal("simulated execute failure".to_string()));
                }
                if *self.progressed.lock().await {
                    Ok(StageExecutionOutcome::Progressed)
                } else {
                    Ok(StageExecutionOutcome::Idle)
                }
            })
        }
    }

    impl ArtifactStore for FakePorts {
        fn store_artifact(&self, _record: StageArtifactRecord) -> PortFuture<'_, ()> {
            Box::pin(async move { Ok(()) })
        }
    }

    impl LandingGateway for FakePorts {
        fn execute_landing<'a>(
            &'a self,
            _bead_id: &'a RuntimeBeadId,
        ) -> PortFuture<'a, LandingOutcome> {
            Box::pin(async move { Ok(LandingOutcome::new(true, "noop")) })
        }
    }

    impl EventSink for FakePorts {
        fn append_event(&self, _event: OrchestratorEvent) -> PortFuture<'_, ()> {
            Box::pin(async move { Ok(()) })
        }
    }

    fn assert_ports_contract<T: OrchestratorPorts>() {}

    fn agent_id() -> RuntimeAgentId {
        assert_ports_contract::<FakePorts>();
        RuntimeAgentId::new(RuntimeRepoId::new("local"), 1)
    }

    fn working_state() -> RuntimeAgentState {
        RuntimeAgentState::new(
            agent_id(),
            Some(RuntimeBeadId::new("swm-2a2")),
            Some(RuntimeStage::Implement),
            RuntimeAgentStatus::Working,
            1,
        )
    }

    fn idle_state() -> RuntimeAgentState {
        RuntimeAgentState::new(agent_id(), None, None, RuntimeAgentStatus::Idle, 0)
    }

    #[tokio::test]
    async fn tick_returns_agent_missing_when_agent_not_registered() {
        let ports = FakePorts::new(None);
        let service = OrchestratorService::new(ports);

        let result = service.tick(&agent_id()).await;

        assert!(matches!(result, Ok(OrchestratorTickOutcome::AgentMissing)));
    }

    #[tokio::test]
    async fn tick_claims_and_creates_workspace_for_idle_agent() {
        let ports = FakePorts::new(Some(idle_state()))
            .with_claim(RuntimeBeadId::new("swm-2a2"))
            .await;
        let service = OrchestratorService::new(ports.clone());

        let result = service.tick(&agent_id()).await;
        let calls = ports.workspace_calls.lock().await.clone();

        assert!(matches!(result, Ok(OrchestratorTickOutcome::Progressed)));
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], (1, "swm-2a2".to_string()));
        assert_eq!(*ports.recover_count.lock().await, 1);
    }

    #[tokio::test]
    async fn tick_returns_idle_for_working_agent_when_no_progress() {
        let ports = FakePorts::new(Some(working_state()))
            .with_progressed(false)
            .await;
        let service = OrchestratorService::new(ports);

        let result = service.tick(&agent_id()).await;

        assert!(matches!(result, Ok(OrchestratorTickOutcome::Idle)));
    }

    #[tokio::test]
    async fn tick_returns_progressed_for_working_agent_when_stage_executes() {
        let ports = FakePorts::new(Some(working_state()))
            .with_progressed(true)
            .await;
        let service = OrchestratorService::new(ports.clone());

        let result = service.tick(&agent_id()).await;

        assert!(matches!(result, Ok(OrchestratorTickOutcome::Progressed)));
        let heartbeat_calls = ports.heartbeat_calls.lock().await.clone();
        assert_eq!(heartbeat_calls.len(), 1);
        assert_eq!(heartbeat_calls[0], (1, "swm-2a2".to_string(), 300_000));
    }

    #[tokio::test]
    async fn tick_returns_idle_without_execute_when_heartbeat_fails() {
        let ports = FakePorts::new(Some(working_state()))
            .with_progressed(true)
            .await
            .with_heartbeat_ok(false)
            .await;
        let service = OrchestratorService::new(ports.clone());

        let result = service.tick(&agent_id()).await;

        assert!(matches!(result, Ok(OrchestratorTickOutcome::Idle)));
        let heartbeat_calls = ports.heartbeat_calls.lock().await.clone();
        assert_eq!(heartbeat_calls.len(), 1);
    }

    #[tokio::test]
    async fn tick_propagates_port_failures_without_synthetic_transitions() {
        let ports = FakePorts::new(Some(working_state()))
            .with_progressed(true)
            .await
            .with_execute_failure(true)
            .await;
        let service = OrchestratorService::new(ports.clone());

        let result: Result<OrchestratorTickOutcome> = service.tick(&agent_id()).await;
        let calls = ports.workspace_calls.lock().await.clone();

        assert!(matches!(result, Err(Error::Internal(_))));
        assert!(calls.is_empty());
    }

    #[tokio::test]
    async fn tick_returns_completed_for_done_agent() {
        let done_state = RuntimeAgentState::new(
            agent_id(),
            None,
            Some(RuntimeStage::Done),
            RuntimeAgentStatus::Done,
            1,
        );
        let ports = FakePorts::new(Some(done_state));
        let service = OrchestratorService::new(ports);

        let result = service.tick(&agent_id()).await;

        assert!(matches!(result, Ok(OrchestratorTickOutcome::Completed)));
    }
}
