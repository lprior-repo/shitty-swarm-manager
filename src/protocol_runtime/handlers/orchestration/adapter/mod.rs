#![allow(clippy::all)]
#![allow(dead_code)]
#![allow(unused_imports)]

mod agent_adapter;
mod external_command;
mod monitor_adapter;
#[cfg(test)]
mod tests;

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
