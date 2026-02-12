#![allow(clippy::all)]
#![allow(dead_code)]
#![allow(unused_imports)]

use std::iter::FromIterator;

use super::super::super::super::{handle_monitor, ProtocolRequest};
use super::super::helpers::protocol_failure_to_swarm_error;
use crate::orchestrator_service::PortFuture;
use serde_json::{Map, Value};

pub(super) fn build_monitor_progress_request(rid: Option<String>) -> ProtocolRequest {
    ProtocolRequest {
        cmd: "monitor".to_string(),
        rid,
        dry: Some(false),
        args: Map::from_iter(vec![(
            "view".to_string(),
            Value::String("progress".to_string()),
        )]),
    }
}

pub(super) fn monitor_progress(request: &ProtocolRequest) -> PortFuture<'_, Value> {
    let request = request.clone();
    Box::pin(async move {
        let req = build_monitor_progress_request(request.rid.clone());
        handle_monitor(&req)
            .await
            .map(|success| success.data)
            .map_err(|failure| protocol_failure_to_swarm_error(*failure))
    })
}
