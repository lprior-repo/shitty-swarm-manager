use crate::{
    Result, RuntimeAgentId, RuntimeAgentState, RuntimeAgentStatus, RuntimeBeadId, RuntimeRepoId,
    RuntimeStage, RuntimeStageResult,
};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

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
    fn recover_stale_claims<'a>(&'a self, repo_id: &'a RuntimeRepoId) -> PortFuture<'a, u32>;

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
        self.ports.recover_stale_claims(agent_id.repo_id()).await?;
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

#[derive(Debug, Clone)]
pub struct ClaimNextResult {
    pub recommendation: Value,
    pub bead_id: String,
    pub claim: Value,
    pub bv_robot_next_ms: u64,
    pub br_update_ms: u64,
}

pub trait ClaimNextPorts {
    fn bv_robot_next(&self) -> PortFuture<'_, Value>;
    fn br_update_in_progress<'a>(&'a self, bead_id: &'a str) -> PortFuture<'a, Value>;
}

pub struct ClaimNextAppService<P> {
    ports: P,
}

impl<P> ClaimNextAppService<P>
where
    P: ClaimNextPorts + Sync,
{
    #[must_use]
    pub const fn new(ports: P) -> Self {
        Self { ports }
    }

    /// Execute one claim-next orchestration cycle through external ports.
    ///
    /// # Errors
    /// Returns an error when recommendation retrieval fails, the recommendation
    /// payload does not contain a bead id, or claim update fails.
    pub async fn execute<F>(&self, bead_id_from_recommendation: F) -> Result<ClaimNextResult>
    where
        F: Fn(&Value) -> Option<String>,
    {
        let recommendation_start = Instant::now();
        let recommendation_payload = self.ports.bv_robot_next().await?;
        let bv_robot_next_ms = elapsed_ms(recommendation_start);
        let recommendation = recommendation_payload
            .get("next")
            .cloned()
            .unwrap_or(recommendation_payload);
        let bead_id = bead_id_from_recommendation(&recommendation).ok_or_else(|| {
            crate::Error::ConfigError("missing bead id in recommendation".to_string())
        })?;

        let update_start = Instant::now();
        let claim = self.ports.br_update_in_progress(&bead_id).await?;
        let br_update_ms = elapsed_ms(update_start);

        Ok(ClaimNextResult {
            recommendation,
            bead_id,
            claim,
            bv_robot_next_ms,
            br_update_ms,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AssignCommand {
    pub repo_id: RuntimeRepoId,
    pub bead_id: String,
    pub agent_id: u32,
}

#[derive(Debug, Clone)]
pub struct AssignAgentSnapshot {
    pub valid_ids: Vec<u32>,
    pub status: RuntimeAgentStatus,
    pub current_bead: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AssignResult {
    pub bead_id: String,
    pub agent_id: u32,
    pub assignee: String,
    pub br_update: Value,
    pub bead_verify: Value,
    pub verified_status: Option<String>,
    pub verified_id: Option<String>,
}

pub trait AssignPorts {
    fn load_agent_snapshot<'a>(
        &'a self,
        repo_id: &'a RuntimeRepoId,
        agent_id: u32,
    ) -> PortFuture<'a, Option<AssignAgentSnapshot>>;

    fn br_show_bead<'a>(&'a self, bead_id: &'a str) -> PortFuture<'a, Value>;

    fn claim_bead<'a>(
        &'a self,
        repo_id: &'a RuntimeRepoId,
        agent_id: u32,
        bead_id: &'a str,
    ) -> PortFuture<'a, bool>;

    fn release_agent<'a>(&'a self, repo_id: &'a RuntimeRepoId, agent_id: u32)
        -> PortFuture<'a, ()>;

    fn br_assign_in_progress<'a>(
        &'a self,
        bead_id: &'a str,
        assignee: &'a str,
    ) -> PortFuture<'a, Value>;
}

pub struct AssignAppService<P> {
    ports: P,
}

impl<P> AssignAppService<P>
where
    P: AssignPorts + Sync,
{
    #[must_use]
    pub const fn new(ports: P) -> Self {
        Self { ports }
    }

    /// Execute one explicit assign command through repository and external ports.
    ///
    /// # Errors
    /// Returns an error when agent/bead preconditions are not met or when
    /// claim/sync side effects fail.
    pub async fn execute<S, I>(
        &self,
        command: AssignCommand,
        issue_status_from_payload: S,
        issue_id_from_payload: I,
    ) -> Result<AssignResult>
    where
        S: Fn(&Value) -> Option<String>,
        I: Fn(&Value) -> Option<String>,
    {
        let snapshot = self
            .ports
            .load_agent_snapshot(&command.repo_id, command.agent_id)
            .await?
            .ok_or_else(|| {
                crate::Error::BeadError(format!("Agent {} is not registered", command.agent_id))
            })?;

        if snapshot.status != RuntimeAgentStatus::Idle || snapshot.current_bead.is_some() {
            return Err(crate::Error::AgentError(format!(
                "Agent {} is not idle",
                command.agent_id
            )));
        }

        let bead_before = self.ports.br_show_bead(&command.bead_id).await?;
        let current_status = issue_status_from_payload(&bead_before).ok_or_else(|| {
            crate::Error::ConfigError("br show returned payload without status".to_string())
        })?;

        if current_status != "open" {
            return Err(crate::Error::StageError(format!(
                "Bead {} is not assignable: status={current_status}",
                command.bead_id
            )));
        }

        let claimed = self
            .ports
            .claim_bead(&command.repo_id, command.agent_id, &command.bead_id)
            .await?;
        if !claimed {
            return Err(crate::Error::StageError(format!(
                "Failed to claim bead {} for agent {}",
                command.bead_id, command.agent_id
            )));
        }

        let assignee = format!("swarm-agent-{}", command.agent_id);
        let update_result = self
            .ports
            .br_assign_in_progress(&command.bead_id, assignee.as_str())
            .await;

        let br_update = match update_result {
            Ok(value) => value,
            Err(err) => {
                let _ = self
                    .ports
                    .release_agent(&command.repo_id, command.agent_id)
                    .await;
                return Err(err);
            }
        };

        let bead_verify = self.ports.br_show_bead(&command.bead_id).await?;
        let verified_status = issue_status_from_payload(&bead_verify);
        let verified_id = issue_id_from_payload(&bead_verify);

        Ok(AssignResult {
            bead_id: command.bead_id,
            agent_id: command.agent_id,
            assignee,
            br_update,
            bead_verify,
            verified_status,
            verified_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RunOnceResult {
    pub agent_id: u32,
    pub doctor: Value,
    pub status_before: Value,
    pub claim_next: Value,
    pub agent: Value,
    pub progress: Value,
    pub doctor_ms: u64,
    pub status_before_ms: u64,
    pub claim_next_ms: u64,
    pub agent_ms: u64,
    pub progress_ms: u64,
}

pub trait RunOncePorts {
    fn doctor(&self) -> PortFuture<'_, Value>;
    fn status(&self) -> PortFuture<'_, Value>;
    fn claim_next(&self) -> PortFuture<'_, Value>;
    fn run_agent(&self, agent_id: u32) -> PortFuture<'_, Value>;
    fn monitor_progress(&self) -> PortFuture<'_, Value>;
}

pub struct RunOnceAppService<P> {
    ports: P,
}

impl<P> RunOnceAppService<P>
where
    P: RunOncePorts + Sync,
{
    #[must_use]
    pub const fn new(ports: P) -> Self {
        Self { ports }
    }

    /// Execute one compact orchestration run-once sequence.
    ///
    /// # Errors
    /// Returns an error when any constituent command port fails.
    pub async fn execute(&self, agent_id: u32) -> Result<RunOnceResult> {
        let doctor_start = Instant::now();
        let doctor = self.ports.doctor().await?;
        let doctor_ms = elapsed_ms(doctor_start);

        let status_before_start = Instant::now();
        let status_before = self.ports.status().await?;
        let status_before_ms = elapsed_ms(status_before_start);

        let claim_start = Instant::now();
        let claim_next = self.ports.claim_next().await?;
        let claim_next_ms = elapsed_ms(claim_start);

        let agent_start = Instant::now();
        let agent = self.ports.run_agent(agent_id).await?;
        let agent_ms = elapsed_ms(agent_start);

        let progress_start = Instant::now();
        let progress = self.ports.monitor_progress().await?;
        let progress_ms = elapsed_ms(progress_start);

        Ok(RunOnceResult {
            agent_id,
            doctor,
            status_before,
            claim_next,
            agent,
            progress,
            doctor_ms,
            status_before_ms,
            claim_next_ms,
            agent_ms,
            progress_ms,
        })
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    let duration = start.elapsed();
    let ms = duration.as_millis();
    u64::try_from(ms).map_or(u64::MAX, |value| value)
}

#[cfg(test)]
mod tests {
    use super::{
        ArtifactStore, AssignAgentSnapshot, AssignAppService, AssignCommand, AssignPorts,
        ClaimNextAppService, ClaimNextPorts, ClaimRepository, EventSink, LandingGateway,
        LandingOutcome, OrchestratorEvent, OrchestratorPorts, OrchestratorService,
        OrchestratorTickOutcome, PortFuture, RunOnceAppService, RunOncePorts, StageArtifactRecord,
        StageExecutionOutcome, StageExecutionRequest, StageExecutor,
    };
    use crate::{
        Error, Result, RuntimeAgentId, RuntimeAgentState, RuntimeAgentStatus, RuntimeBeadId,
        RuntimeRepoId, RuntimeStage, SwarmError,
    };
    use serde_json::{json, Value};
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
        fn recover_stale_claims<'a>(&'a self, _repo_id: &'a RuntimeRepoId) -> PortFuture<'a, u32> {
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

    #[derive(Clone)]
    struct ClaimNextFakePorts {
        recommendation: Value,
        claim: Value,
    }

    impl ClaimNextPorts for ClaimNextFakePorts {
        fn bv_robot_next(&self) -> PortFuture<'_, Value> {
            let payload = self.recommendation.clone();
            Box::pin(async move { Ok(payload) })
        }

        fn br_update_in_progress<'a>(&'a self, _bead_id: &'a str) -> PortFuture<'a, Value> {
            let payload = self.claim.clone();
            Box::pin(async move { Ok(payload) })
        }
    }

    #[tokio::test]
    async fn given_recommendation_with_id_when_claim_next_executes_then_returns_bead_and_claim() {
        let service = ClaimNextAppService::new(ClaimNextFakePorts {
            recommendation: json!({"id":"swm-100"}),
            claim: json!({"ok":true}),
        });

        let output = service
            .execute(|value| {
                value
                    .get("id")
                    .and_then(Value::as_str)
                    .map(std::string::ToString::to_string)
            })
            .await
            .expect("claim-next should succeed with recommendation id");

        assert_eq!(output.bead_id, "swm-100");
        assert_eq!(output.claim, json!({"ok":true}));
    }

    #[tokio::test]
    async fn given_recommendation_without_id_when_claim_next_executes_then_returns_error() {
        let service = ClaimNextAppService::new(ClaimNextFakePorts {
            recommendation: json!({"priority":1}),
            claim: json!({"ok":true}),
        });

        let result = service.execute(|value| {
            value
                .get("id")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string)
        });

        assert!(matches!(
            result.await,
            Err(SwarmError::ConfigError(message)) if message.contains("missing bead id")
        ));
    }

    #[derive(Clone)]
    struct AssignFakePorts {
        snapshot: Option<AssignAgentSnapshot>,
        bead_status: Option<String>,
        claim_ok: bool,
    }

    impl AssignPorts for AssignFakePorts {
        fn load_agent_snapshot<'a>(
            &'a self,
            _repo_id: &'a RuntimeRepoId,
            _agent_id: u32,
        ) -> PortFuture<'a, Option<AssignAgentSnapshot>> {
            let payload = self.snapshot.clone();
            Box::pin(async move { Ok(payload) })
        }

        fn br_show_bead<'a>(&'a self, bead_id: &'a str) -> PortFuture<'a, Value> {
            let status = self.bead_status.clone();
            Box::pin(async move {
                Ok(json!({
                    "id": bead_id,
                    "status": status,
                }))
            })
        }

        fn claim_bead<'a>(
            &'a self,
            _repo_id: &'a RuntimeRepoId,
            _agent_id: u32,
            _bead_id: &'a str,
        ) -> PortFuture<'a, bool> {
            let ok = self.claim_ok;
            Box::pin(async move { Ok(ok) })
        }

        fn release_agent<'a>(
            &'a self,
            _repo_id: &'a RuntimeRepoId,
            _agent_id: u32,
        ) -> PortFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }

        fn br_assign_in_progress<'a>(
            &'a self,
            bead_id: &'a str,
            assignee: &'a str,
        ) -> PortFuture<'a, Value> {
            Box::pin(async move {
                Ok(json!({
                    "bead_id": bead_id,
                    "assignee": assignee,
                }))
            })
        }
    }

    #[tokio::test]
    async fn given_idle_agent_and_open_bead_when_assign_executes_then_returns_assignee_result() {
        let service = AssignAppService::new(AssignFakePorts {
            snapshot: Some(AssignAgentSnapshot {
                valid_ids: vec![1],
                status: RuntimeAgentStatus::Idle,
                current_bead: None,
            }),
            bead_status: Some("open".to_string()),
            claim_ok: true,
        });

        let command = AssignCommand {
            repo_id: RuntimeRepoId::new("local"),
            bead_id: "swm-200".to_string(),
            agent_id: 1,
        };

        let result = service
            .execute(
                command,
                |payload| {
                    payload
                        .get("status")
                        .and_then(Value::as_str)
                        .map(std::string::ToString::to_string)
                },
                |payload| {
                    payload
                        .get("id")
                        .and_then(Value::as_str)
                        .map(std::string::ToString::to_string)
                },
            )
            .await;

        assert!(result.is_ok());
        let output = result.expect("assign should succeed with idle agent and open bead");
        assert_eq!(output.assignee, "swarm-agent-1");
        assert_eq!(output.verified_status, Some("open".to_string()));
        assert_eq!(output.verified_id, Some("swm-200".to_string()));
    }

    #[tokio::test]
    async fn given_non_idle_agent_when_assign_executes_then_returns_agent_error() {
        let service = AssignAppService::new(AssignFakePorts {
            snapshot: Some(AssignAgentSnapshot {
                valid_ids: vec![1],
                status: RuntimeAgentStatus::Working,
                current_bead: Some("swm-199".to_string()),
            }),
            bead_status: Some("open".to_string()),
            claim_ok: true,
        });

        let command = AssignCommand {
            repo_id: RuntimeRepoId::new("local"),
            bead_id: "swm-200".to_string(),
            agent_id: 1,
        };

        let result = service
            .execute(
                command,
                |_payload| Some("open".to_string()),
                |_payload| Some("swm-200".to_string()),
            )
            .await;

        assert!(matches!(
            result,
            Err(SwarmError::AgentError(message)) if message.contains("not idle")
        ));
    }

    #[derive(Clone)]
    struct RunOnceFakePorts;

    impl RunOncePorts for RunOnceFakePorts {
        fn doctor(&self) -> PortFuture<'_, Value> {
            Box::pin(async move { Ok(json!({"ok":true,"step":"doctor"})) })
        }

        fn status(&self) -> PortFuture<'_, Value> {
            Box::pin(async move { Ok(json!({"ok":true,"step":"status"})) })
        }

        fn claim_next(&self) -> PortFuture<'_, Value> {
            Box::pin(async move { Ok(json!({"ok":true,"step":"claim-next"})) })
        }

        fn run_agent(&self, agent_id: u32) -> PortFuture<'_, Value> {
            Box::pin(async move { Ok(json!({"ok":true,"step":"agent","id":agent_id})) })
        }

        fn monitor_progress(&self) -> PortFuture<'_, Value> {
            Box::pin(async move { Ok(json!({"ok":true,"step":"progress"})) })
        }
    }

    #[tokio::test]
    async fn given_run_once_ports_when_execute_then_returns_compact_step_payload() {
        let service = RunOnceAppService::new(RunOnceFakePorts);

        let result = service.execute(7).await;

        assert!(result.is_ok());
        let output = result.expect("run-once should succeed with fake ports");
        assert_eq!(output.agent_id, 7);
        assert_eq!(output.agent["id"], Value::from(7));
        assert_eq!(output.progress["step"], Value::from("progress"));
    }
}
