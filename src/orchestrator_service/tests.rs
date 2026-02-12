#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

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
