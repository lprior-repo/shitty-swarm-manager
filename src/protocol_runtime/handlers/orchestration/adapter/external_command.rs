#![allow(clippy::all)]
#![allow(dead_code)]
#![allow(unused_imports)]

use super::super::super::super::{
    handle_claim_next, handle_doctor, handle_status, project_next_recommendation,
    run_external_json_command, ProtocolRequest,
};
use super::super::helpers::protocol_failure_to_swarm_error;
use crate::orchestrator_service::{ClaimNextPorts, PortFuture, RunOncePorts};
use serde_json::Value;

pub(in crate::protocol_runtime) fn bv_robot_next(
    request: &ProtocolRequest,
) -> PortFuture<'_, Value> {
    let request = request.clone();
    Box::pin(async move {
        run_external_json_command(
            "bv",
            &["--robot-next"],
            request.rid.clone(),
            "Run `bv --robot-next` manually and verify beads index is available",
        )
        .await
        .map(|payload| project_next_recommendation(&payload))
        .map_err(|failure| protocol_failure_to_swarm_error(*failure))
    })
}

pub(in crate::protocol_runtime) fn br_update_in_progress<'a>(
    request: &'a ProtocolRequest,
    bead_id: &'a str,
) -> PortFuture<'a, Value> {
    Box::pin(async move {
        run_external_json_command(
            "br",
            &["update", bead_id, "--status", "in_progress", "--json"],
            request.rid.clone(),
            "Run `br update <bead-id> --status in_progress --json` manually",
        )
        .await
        .map_err(|failure| protocol_failure_to_swarm_error(*failure))
    })
}

pub(in crate::protocol_runtime) fn br_show_bead<'a>(
    request: &'a ProtocolRequest,
    bead_id: &'a str,
) -> PortFuture<'a, Value> {
    Box::pin(async move {
        run_external_json_command(
            "br",
            &["show", bead_id, "--json"],
            request.rid.clone(),
            "Run `br show <bead-id> --json` and verify bead exists",
        )
        .await
        .map_err(|failure| protocol_failure_to_swarm_error(*failure))
    })
}

pub(in crate::protocol_runtime) fn br_assign_in_progress<'a>(
    request: &'a ProtocolRequest,
    bead_id: &'a str,
    assignee: &'a str,
) -> PortFuture<'a, Value> {
    Box::pin(async move {
        run_external_json_command(
            "br",
            &[
                "update",
                bead_id,
                "--status",
                "in_progress",
                "--assignee",
                assignee,
                "--json",
            ],
            request.rid.clone(),
            "Run `br update <bead-id> --status in_progress --assignee swarm-agent-<id> --json` manually",
        )
        .await
        .map_err(|failure| protocol_failure_to_swarm_error(*failure))
    })
}

pub(in crate::protocol_runtime) fn doctor(request: &ProtocolRequest) -> PortFuture<'_, Value> {
    let request = request.clone();
    Box::pin(async move {
        handle_doctor(&request)
            .await
            .map(|success| success.data)
            .map_err(|failure| protocol_failure_to_swarm_error(*failure))
    })
}

pub(in crate::protocol_runtime) fn status(request: &ProtocolRequest) -> PortFuture<'_, Value> {
    let request = request.clone();
    Box::pin(async move {
        handle_status(&request)
            .await
            .map(|success| success.data)
            .map_err(|failure| protocol_failure_to_swarm_error(*failure))
    })
}

pub(in crate::protocol_runtime) fn claim_next(request: &ProtocolRequest) -> PortFuture<'_, Value> {
    let request = request.clone();
    Box::pin(async move {
        handle_claim_next(&request)
            .await
            .map(|success| success.data)
            .map_err(|failure| protocol_failure_to_swarm_error(*failure))
    })
}
