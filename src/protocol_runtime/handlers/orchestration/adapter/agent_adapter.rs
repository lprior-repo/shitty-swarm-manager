#![allow(clippy::all)]
#![allow(dead_code)]
#![allow(unused_imports)]

use std::iter::FromIterator;

use super::super::super::super::{db_from_request, handle_agent, ProtocolRequest};
use super::super::helpers::protocol_failure_to_swarm_error;
use crate::orchestrator_service::{AssignAgentSnapshot, AssignPorts, PortFuture};
use crate::{AgentId, BeadId, RepoId, RuntimeAgentStatus, RuntimeRepoId};
use serde_json::{Map, Value};

pub(super) fn runtime_status_from_db_status(status: &str) -> RuntimeAgentStatus {
    match status {
        "idle" => RuntimeAgentStatus::Idle,
        "working" => RuntimeAgentStatus::Working,
        "waiting" => RuntimeAgentStatus::Waiting,
        "done" => RuntimeAgentStatus::Done,
        _ => RuntimeAgentStatus::Error,
    }
}

pub(super) fn build_agent_request(rid: Option<String>, agent_id: u32) -> ProtocolRequest {
    ProtocolRequest {
        cmd: "agent".to_string(),
        rid,
        dry: Some(false),
        args: Map::from_iter(vec![("id".to_string(), Value::from(agent_id))]),
    }
}

pub(super) fn run_agent(request: &ProtocolRequest, agent_id: u32) -> PortFuture<'_, Value> {
    let request = request.clone();
    Box::pin(async move {
        let req = build_agent_request(request.rid.clone(), agent_id);
        handle_agent(&req)
            .await
            .map(|success| success.data)
            .map_err(|failure| protocol_failure_to_swarm_error(*failure))
    })
}

pub(super) fn load_agent_snapshot<'a>(
    request: &'a ProtocolRequest,
    repo_id: &'a RuntimeRepoId,
    agent_id: u32,
) -> PortFuture<'a, Option<AssignAgentSnapshot>> {
    Box::pin(async move {
        let db = db_from_request(request)
            .await
            .map_err(|failure| protocol_failure_to_swarm_error(*failure))?;
        let repo = RepoId::new(repo_id.value());
        let valid_ids = db
            .get_available_agents(&repo)
            .await
            .map(|agents| {
                agents
                    .into_iter()
                    .map(|agent| agent.agent_id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let agent_key = AgentId::new(repo.clone(), agent_id);
        let state = db.get_agent_state(&agent_key).await?;

        Ok(state.map(|agent_state| {
            let status = runtime_status_from_db_status(agent_state.status().as_str());
            AssignAgentSnapshot {
                valid_ids,
                status,
                current_bead: agent_state.bead_id().map(|bead| bead.value().to_string()),
            }
        }))
    })
}

pub(super) fn claim_bead<'a>(
    request: &'a ProtocolRequest,
    repo_id: &'a RuntimeRepoId,
    agent_id: u32,
    bead_id: &'a str,
) -> PortFuture<'a, bool> {
    Box::pin(async move {
        let db = db_from_request(request)
            .await
            .map_err(|failure| protocol_failure_to_swarm_error(*failure))?;
        let repo = RepoId::new(repo_id.value());
        let agent_key = AgentId::new(repo, agent_id);
        db.claim_bead(&agent_key, &BeadId::new(bead_id.to_string()))
            .await
    })
}

pub(super) fn release_agent<'a>(
    request: &'a ProtocolRequest,
    repo_id: &'a RuntimeRepoId,
    agent_id: u32,
) -> PortFuture<'a, ()> {
    Box::pin(async move {
        let db = db_from_request(request)
            .await
            .map_err(|failure| protocol_failure_to_swarm_error(*failure))?;
        let repo = RepoId::new(repo_id.value());
        let agent_key = AgentId::new(repo, agent_id);
        db.release_agent(&agent_key).await.map(|_| ())
    })
}
