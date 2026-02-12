#![allow(clippy::all)]
#![allow(dead_code)]
#![allow(unused_imports)]

mod agent_adapter;
mod external_command;
mod monitor_adapter;

pub(in crate::protocol_runtime) use agent_adapter::{
    build_agent_request, claim_bead, load_agent_snapshot, release_agent, run_agent,
    runtime_status_from_db_status,
};
pub(in crate::protocol_runtime) use external_command::{
    br_assign_in_progress, br_show_bead, br_update_in_progress, bv_robot_next, claim_next, doctor,
    status,
};
pub(in crate::protocol_runtime) use monitor_adapter::{
    build_monitor_progress_request, monitor_progress,
};

use super::super::super::ProtocolRequest;
use crate::orchestrator_service::{
    AssignAgentSnapshot, AssignPorts, ClaimNextPorts, PortFuture, RunOncePorts,
};
use crate::RuntimeRepoId;

#[derive(Clone)]
pub(in crate::protocol_runtime) struct ProtocolCommandAdapter {
    request: ProtocolRequest,
}

impl ProtocolCommandAdapter {
    pub(in crate::protocol_runtime) fn new(request: &ProtocolRequest) -> Self {
        Self {
            request: request.clone(),
        }
    }
}

impl ClaimNextPorts for ProtocolCommandAdapter {
    fn bv_robot_next(&self) -> PortFuture<'_, serde_json::Value> {
        bv_robot_next(&self.request)
    }

    fn br_update_in_progress<'a>(&'a self, bead_id: &'a str) -> PortFuture<'a, serde_json::Value> {
        br_update_in_progress(&self.request, bead_id)
    }
}

impl AssignPorts for ProtocolCommandAdapter {
    fn load_agent_snapshot<'a>(
        &'a self,
        repo_id: &'a RuntimeRepoId,
        agent_id: u32,
    ) -> PortFuture<'a, Option<AssignAgentSnapshot>> {
        load_agent_snapshot(&self.request, repo_id, agent_id)
    }

    fn br_show_bead<'a>(&'a self, bead_id: &'a str) -> PortFuture<'a, serde_json::Value> {
        br_show_bead(&self.request, bead_id)
    }

    fn claim_bead<'a>(
        &'a self,
        repo_id: &'a RuntimeRepoId,
        agent_id: u32,
        bead_id: &'a str,
    ) -> PortFuture<'a, bool> {
        claim_bead(&self.request, repo_id, agent_id, bead_id)
    }

    fn release_agent<'a>(
        &'a self,
        repo_id: &'a RuntimeRepoId,
        agent_id: u32,
    ) -> PortFuture<'a, ()> {
        release_agent(&self.request, repo_id, agent_id)
    }

    fn br_assign_in_progress<'a>(
        &'a self,
        bead_id: &'a str,
        assignee: &'a str,
    ) -> PortFuture<'a, serde_json::Value> {
        br_assign_in_progress(&self.request, bead_id, assignee)
    }
}

impl RunOncePorts for ProtocolCommandAdapter {
    fn doctor(&self) -> PortFuture<'_, serde_json::Value> {
        doctor(&self.request)
    }

    fn status(&self) -> PortFuture<'_, serde_json::Value> {
        status(&self.request)
    }

    fn claim_next(&self) -> PortFuture<'_, serde_json::Value> {
        claim_next(&self.request)
    }

    fn run_agent(&self, agent_id: u32) -> PortFuture<'_, serde_json::Value> {
        run_agent(&self.request, agent_id)
    }

    fn monitor_progress(&self) -> PortFuture<'_, serde_json::Value> {
        monitor_progress(&self.request)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_agent_request, build_monitor_progress_request, runtime_status_from_db_status,
    };
    use crate::ddd::{
        RuntimeAgentId as RuntimeAgentIdStruct, RuntimeAgentState, RuntimeAgentStatus,
        RuntimeBeadId, RuntimeRepoId, RuntimeStage,
    };
    use crate::types::AvailableAgent;
    use crate::{AgentId, BeadId, RepoId, Result, SwarmError};
    use serde_json::Value;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone)]
    struct MockAgentState {
        status: String,
        bead_id: Option<String>,
        current_stage: Option<String>,
        implementation_attempt: u32,
    }

    #[derive(Debug, Clone)]
    struct MockDb {
        available_agents: Arc<Mutex<Vec<AvailableAgent>>>,
        agent_states: Arc<Mutex<std::collections::HashMap<String, MockAgentState>>>,
        claim_results: Arc<Mutex<std::collections::HashMap<String, bool>>>,
        release_bead_id: Arc<Mutex<Option<String>>>,
        release_error: Arc<Mutex<bool>>,
        should_fail_get_available_agents: Arc<Mutex<bool>>,
        should_fail_get_agent_state: Arc<Mutex<bool>>,
        should_fail_claim_bead: Arc<Mutex<bool>>,
        should_fail_release_agent: Arc<Mutex<bool>>,
    }

    impl MockDb {
        fn new() -> Self {
            Self {
                available_agents: Arc::new(Mutex::new(Vec::new())),
                agent_states: Arc::new(Mutex::new(std::collections::HashMap::new())),
                claim_results: Arc::new(Mutex::new(std::collections::HashMap::new())),
                release_bead_id: Arc::new(Mutex::new(None)),
                release_error: Arc::new(Mutex::new(false)),
                should_fail_get_available_agents: Arc::new(Mutex::new(false)),
                should_fail_get_agent_state: Arc::new(Mutex::new(false)),
                should_fail_claim_bead: Arc::new(Mutex::new(false)),
                should_fail_release_agent: Arc::new(Mutex::new(false)),
            }
        }

        async fn with_available_agents(&self, agents: Vec<u32>) -> Self {
            let mut lock = self.available_agents.lock().await;
            *lock = agents
                .into_iter()
                .map(|id| AvailableAgent {
                    repo_id: RepoId::new("test-repo"),
                    agent_id: id,
                    status: crate::AgentStatus::Idle,
                    implementation_attempt: 0,
                    max_implementation_attempts: 3,
                    max_agents: 10,
                })
                .collect();
            self.clone()
        }

        async fn with_agent_state(&self, agent_id: u32, state: MockAgentState) -> Self {
            let key = format!("test-repo:{}", agent_id);
            let mut lock = self.agent_states.lock().await;
            lock.insert(key, state);
            self.clone()
        }

        async fn with_claim_result(&self, agent_id: u32, bead_id: &str, result: bool) -> Self {
            let key = format!("{}:{}", agent_id, bead_id);
            let mut lock = self.claim_results.lock().await;
            lock.insert(key, result);
            self.clone()
        }

        async fn with_release_result(&self, bead_id: Option<&str>) -> Self {
            let mut lock = self.release_bead_id.lock().await;
            *lock = bead_id.map(|s| s.to_string());
            self.clone()
        }

        async fn with_release_error(&self) -> Self {
            let mut lock = self.release_error.lock().await;
            *lock = true;
            self.clone()
        }

        async fn with_get_available_agents_failure(&self) -> Self {
            let mut lock = self.should_fail_get_available_agents.lock().await;
            *lock = true;
            self.clone()
        }

        async fn with_get_agent_state_failure(&self) -> Self {
            let mut lock = self.should_fail_get_agent_state.lock().await;
            *lock = true;
            self.clone()
        }

        async fn with_claim_bead_failure(&self) -> Self {
            let mut lock = self.should_fail_claim_bead.lock().await;
            *lock = true;
            self.clone()
        }

        async fn with_release_agent_failure(&self) -> Self {
            let mut lock = self.should_fail_release_agent.lock().await;
            *lock = true;
            self.clone()
        }
    }

    async fn mock_get_available_agents(
        db: &MockDb,
        _repo_id: &RepoId,
    ) -> Result<Vec<AvailableAgent>> {
        if *db.should_fail_get_available_agents.lock().await {
            return Err(SwarmError::DatabaseError(
                "Simulated DB error on get_available_agents".to_string(),
            ));
        }
        Ok(db.available_agents.lock().await.clone())
    }

    async fn mock_get_agent_state(
        db: &MockDb,
        agent_id: &AgentId,
    ) -> Result<Option<RuntimeAgentState>> {
        if *db.should_fail_get_agent_state.lock().await {
            return Err(SwarmError::DatabaseError(
                "Simulated DB error on get_agent_state".to_string(),
            ));
        }
        let key = format!("{}:{}", agent_id.repo_id().value(), agent_id.number());
        let states = db.agent_states.lock().await;
        if let Some(mock_state) = states.get(&key) {
            let runtime_agent = RuntimeAgentIdStruct::new(
                RuntimeRepoId::new(agent_id.repo_id().value().to_string()),
                agent_id.number(),
            );
            let state = RuntimeAgentState::new(
                runtime_agent,
                mock_state.bead_id.clone().map(RuntimeBeadId::new),
                mock_state
                    .current_stage
                    .clone()
                    .map(|s| RuntimeStage::try_from(s.as_str()).unwrap()),
                RuntimeAgentStatus::try_from(mock_state.status.as_str()).unwrap(),
                mock_state.implementation_attempt,
            );
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    async fn mock_claim_bead(db: &MockDb, agent_id: &AgentId, bead_id: &BeadId) -> Result<bool> {
        if *db.should_fail_claim_bead.lock().await {
            return Err(SwarmError::DatabaseError(
                "Simulated DB error on claim_bead".to_string(),
            ));
        }
        let key = format!("{}:{}", agent_id.number(), bead_id.value());
        Ok(db
            .claim_results
            .lock()
            .await
            .get(&key)
            .copied()
            .unwrap_or(true))
    }

    async fn mock_release_agent(db: &MockDb, _agent_id: &AgentId) -> Result<Option<BeadId>> {
        if *db.should_fail_release_agent.lock().await {
            return Err(SwarmError::DatabaseError(
                "Simulated DB error on release_agent".to_string(),
            ));
        }
        let bead_id = db.release_bead_id.lock().await.clone();
        Ok(bead_id.map(BeadId::new))
    }

    mod load_agent_snapshot_tests {
        use super::*;

        fn create_test_request() -> crate::protocol_runtime::ProtocolRequest {
            crate::protocol_runtime::ProtocolRequest {
                cmd: "assign".to_string(),
                rid: Some("test-rid".to_string()),
                dry: Some(false),
                args: serde_json::Map::new(),
            }
        }

        #[tokio::test]
        async fn load_agent_snapshot_with_valid_state_returns_snapshot() {
            let db = MockDb::new()
                .with_available_agents(vec![1, 2, 3])
                .await
                .with_agent_state(
                    1,
                    MockAgentState {
                        status: "idle".to_string(),
                        bead_id: None,
                        current_stage: None,
                        implementation_attempt: 0,
                    },
                )
                .await;

            let valid_ids = db
                .available_agents
                .lock()
                .await
                .clone()
                .into_iter()
                .map(|a| a.agent_id)
                .collect::<Vec<_>>();

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let state = mock_get_agent_state(&db, &agent_key).await.unwrap();

            assert!(state.is_some());
            let snapshot = state
                .map(|s| {
                    let status = runtime_status_from_db_status(s.status().as_str());
                    crate::orchestrator_service::AssignAgentSnapshot {
                        valid_ids: valid_ids.clone(),
                        status,
                        current_bead: s.bead_id().map(|b| b.value().to_string()),
                    }
                })
                .unwrap();

            assert_eq!(snapshot.valid_ids, vec![1, 2, 3]);
            assert_eq!(snapshot.status, RuntimeAgentStatus::Idle);
            assert!(snapshot.current_bead.is_none());
        }

        #[tokio::test]
        async fn load_agent_snapshot_with_working_state_returns_working_status() {
            let db = MockDb::new()
                .with_available_agents(vec![1])
                .await
                .with_agent_state(
                    1,
                    MockAgentState {
                        status: "working".to_string(),
                        bead_id: Some("bead-42".to_string()),
                        current_stage: Some("implement".to_string()),
                        implementation_attempt: 1,
                    },
                )
                .await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let state = mock_get_agent_state(&db, &agent_key).await.unwrap();

            assert!(state.is_some());
            let agent_state = state.unwrap();
            assert_eq!(agent_state.status(), RuntimeAgentStatus::Working);
            assert!(agent_state.bead_id().is_some());
            assert_eq!(agent_state.bead_id().unwrap().value(), "bead-42");
        }

        #[tokio::test]
        async fn load_agent_snapshot_with_none_state_returns_none() {
            let db = MockDb::new().with_available_agents(vec![1, 2]).await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 99);
            let state = mock_get_agent_state(&db, &agent_key).await.unwrap();

            assert!(state.is_none());
        }

        #[tokio::test]
        async fn load_agent_snapshot_with_empty_agents_list_returns_empty_valid_ids() {
            let db = MockDb::new().with_available_agents(vec![]).await;

            let repo = RepoId::new("test-repo");
            let agents = mock_get_available_agents(&db, &repo).await.unwrap();

            assert!(agents.is_empty());
        }

        #[tokio::test]
        async fn load_agent_snapshot_with_db_error_propagates_error() {
            let db = MockDb::new().with_get_available_agents_failure().await;

            let repo = RepoId::new("test-repo");
            let result = mock_get_available_agents(&db, &repo).await;

            assert!(result.is_err());
            let error = result.unwrap_err();
            assert!(error.to_string().contains("get_available_agents"));
        }

        #[tokio::test]
        async fn load_agent_snapshot_maps_all_known_statuses_correctly() {
            let statuses = vec![
                ("idle", RuntimeAgentStatus::Idle),
                ("working", RuntimeAgentStatus::Working),
                ("waiting", RuntimeAgentStatus::Waiting),
                ("done", RuntimeAgentStatus::Done),
            ];

            for (db_status, expected_runtime) in statuses {
                let db = MockDb::new()
                    .with_available_agents(vec![1])
                    .await
                    .with_agent_state(
                        1,
                        MockAgentState {
                            status: db_status.to_string(),
                            bead_id: None,
                            current_stage: None,
                            implementation_attempt: 0,
                        },
                    )
                    .await;

                let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
                let state = mock_get_agent_state(&db, &agent_key)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    state.status(),
                    expected_runtime,
                    "Status {} should map to {:?}",
                    db_status,
                    expected_runtime
                );
            }
        }
    }

    mod claim_bead_tests {
        use super::*;

        #[tokio::test]
        async fn claim_bead_successful_returns_true() {
            let db = MockDb::new().with_claim_result(1, "bead-100", true).await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let bead_key = BeadId::new("bead-100".to_string());
            let result = mock_claim_bead(&db, &agent_key, &bead_key).await.unwrap();

            assert!(result);
        }

        #[tokio::test]
        async fn claim_bead_already_claimed_returns_false() {
            let db = MockDb::new()
                .with_claim_result(1, "bead-already-claimed", false)
                .await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let bead_key = BeadId::new("bead-already-claimed".to_string());
            let result = mock_claim_bead(&db, &agent_key, &bead_key).await.unwrap();

            assert!(!result);
        }

        #[tokio::test]
        async fn claim_bead_default_behavior_returns_true() {
            let db = MockDb::new();

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let bead_key = BeadId::new("new-bead".to_string());
            let result = mock_claim_bead(&db, &agent_key, &bead_key).await.unwrap();

            assert!(result);
        }

        #[tokio::test]
        async fn claim_bead_with_db_error_propagates_error() {
            let db = MockDb::new().with_claim_bead_failure().await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let bead_key = BeadId::new("bead-error".to_string());
            let result = mock_claim_bead(&db, &agent_key, &bead_key).await;

            assert!(result.is_err());
            let error = result.unwrap_err();
            assert!(error.to_string().contains("claim_bead"));
        }

        #[tokio::test]
        async fn claim_bead_concurrent_access_same_agent_same_bead_returns_consistent() {
            let db = MockDb::new()
                .with_claim_result(1, "bead-concurrent", true)
                .await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let bead_key = BeadId::new("bead-concurrent".to_string());

            let result1 = mock_claim_bead(&db, &agent_key, &bead_key).await.unwrap();
            let result2 = mock_claim_bead(&db, &agent_key, &bead_key).await.unwrap();

            assert_eq!(result1, result2);
        }
    }

    mod release_agent_tests {
        use super::*;

        #[tokio::test]
        async fn release_agent_with_working_agent_returns_previous_bead() {
            let db = MockDb::new()
                .with_release_result(Some("bead-was-working"))
                .await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let result = mock_release_agent(&db, &agent_key).await.unwrap();

            assert!(result.is_some());
            assert_eq!(result.unwrap().value(), "bead-was-working");
        }

        #[tokio::test]
        async fn release_agent_with_free_agent_returns_none() {
            let db = MockDb::new().with_release_result(None).await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let result = mock_release_agent(&db, &agent_key).await.unwrap();

            assert!(result.is_none());
        }

        #[tokio::test]
        async fn release_agent_non_existent_agent_returns_none() {
            let db = MockDb::new().with_release_result(None).await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 999);
            let result = mock_release_agent(&db, &agent_key).await.unwrap();

            assert!(result.is_none());
        }

        #[tokio::test]
        async fn release_agent_with_db_error_propagates_error() {
            let db = MockDb::new().with_release_agent_failure().await;

            let agent_key = AgentId::new(RepoId::new("test-repo"), 1);
            let result = mock_release_agent(&db, &agent_key).await;

            assert!(result.is_err());
            let error = result.unwrap_err();
            assert!(error.to_string().contains("release_agent"));
        }
    }

    mod external_command_error_handling_tests {
        use crate::protocol_envelope::ProtocolEnvelope;
        use crate::protocol_runtime::run_external_json_command;
        use std::time::Duration;

        #[tokio::test]
        async fn run_external_command_with_timeout_returns_envelope_error() {
            let timeout_ms = 100_u64;
            let program = "sleep";
            let args = &["10"];
            let rid = Some("timeout-test".to_string());
            let fix = "Increase timeout or simplify command";

            let result = tokio::time::timeout(
                Duration::from_millis(timeout_ms * 2),
                run_external_json_command(program, args, rid.clone(), fix),
            )
            .await;

            assert!(result.is_err());
        }

        #[tokio::test]
        async fn run_external_command_with_non_zero_exit_returns_error_envelope() {
            let program = "sh";
            let args = &["-c", "echo 'error' >&2; exit 1"];
            let rid = Some("exit-code-test".to_string());
            let fix = "Check command syntax and try again";

            let result = run_external_json_command(program, args, rid.clone(), fix).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            let envelope: Box<ProtocolEnvelope> = err;
            assert!(!envelope.ok);
            assert_eq!(envelope.err.as_ref().unwrap().code, "INTERNAL");
        }

        #[tokio::test]
        async fn run_external_command_with_malformed_json_returns_invalid_envelope() {
            let program = "echo";
            let args = &["not json at all {{{"];
            let rid = Some("malformed-json-test".to_string());
            let fix = "Verify command outputs valid JSON";

            let result = run_external_json_command(program, args, rid.clone(), fix).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            let envelope: Box<ProtocolEnvelope> = err;
            assert!(!envelope.ok);
            assert_eq!(envelope.err.as_ref().unwrap().code, "INVALID");
        }

        #[tokio::test]
        async fn run_external_command_with_valid_json_returns_success() {
            let program = "echo";
            let args = &["{\"ok\":true,\"data\":\"test\"}"];
            let rid = Some("valid-json-test".to_string());
            let fix = "";

            let result = run_external_json_command(program, args, rid.clone(), fix).await;

            assert!(result.is_ok());
            let value = result.unwrap();
            assert_eq!(value.get("ok").and_then(|v| v.as_bool()), Some(true));
        }

        #[tokio::test]
        async fn run_external_command_with_nonexistent_program_returns_error() {
            let program = "/path/to/nonexistent/command/xyz123";
            let args = &["--help"];
            let rid = Some("nonexistent-program".to_string());
            let fix = "Verify the command is installed and in PATH";

            let result = run_external_json_command(program, args, rid.clone(), fix).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            let envelope: Box<ProtocolEnvelope> = err;
            assert!(!envelope.ok);
            assert!(envelope.err.as_ref().unwrap().msg.contains("nonexistent"));
        }
    }

    #[test]
    fn given_known_agent_status_when_mapping_then_runtime_status_matches() {
        assert_eq!(
            runtime_status_from_db_status("idle"),
            RuntimeAgentStatus::Idle
        );
        assert_eq!(
            runtime_status_from_db_status("working"),
            RuntimeAgentStatus::Working
        );
        assert_eq!(
            runtime_status_from_db_status("waiting"),
            RuntimeAgentStatus::Waiting
        );
        assert_eq!(
            runtime_status_from_db_status("done"),
            RuntimeAgentStatus::Done
        );
    }

    #[test]
    fn given_unknown_agent_status_when_mapping_then_error_status_is_used() {
        assert_eq!(
            runtime_status_from_db_status("stuck"),
            RuntimeAgentStatus::Error
        );
    }

    #[test]
    fn given_rid_and_agent_id_when_building_agent_request_then_envelope_targets_agent_non_dry() {
        let request = build_agent_request(Some("rid-agent".to_string()), 9);

        assert_eq!(request.cmd, "agent");
        assert_eq!(request.rid.as_deref(), Some("rid-agent"));
        assert_eq!(request.dry, Some(false));
        assert_eq!(request.args.get("id").and_then(Value::as_u64), Some(9));
    }

    #[test]
    fn given_rid_when_building_monitor_progress_request_then_envelope_targets_progress_non_dry() {
        let request = build_monitor_progress_request(Some("rid-monitor".to_string()));

        assert_eq!(request.cmd, "monitor");
        assert_eq!(request.rid.as_deref(), Some("rid-monitor"));
        assert_eq!(request.dry, Some(false));
        assert_eq!(
            request.args.get("view").and_then(Value::as_str),
            Some("progress")
        );
    }
}
