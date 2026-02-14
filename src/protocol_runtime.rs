#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::option_if_let_else)]
#![allow(clippy::literal_string_with_formatting_args)]
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::unnecessary_box_returns)]
#![allow(clippy::branches_sharing_code)]
#![allow(clippy::too_many_lines)]

use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, SwarmError};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::time::Instant;
use tokio::io::AsyncWriteExt;

mod audit;
pub mod constants;
mod db_resolution;
mod dispatcher;
mod doctor_checks;
mod external_commands;
mod handler_delegates;
pub mod handlers;
mod helpers;
pub mod input_parsing;
mod loop_executor;
mod parsing;
mod schema_loader;
mod validation;

pub use audit::{compose_database_url_candidates, mask_passwords_in_args};
pub use constants::*;
pub use db_resolution::mask_database_url_public as mask_database_url;
pub use dispatcher::{
    bead_id_from_recommendation, dispatch_no_batch, dry_run_success, execute_request,
    execute_request_no_batch, project_next_recommendation, CommandSuccess,
};
pub use doctor_checks::{check_command, check_database_connectivity};
pub use external_commands::{
    capture_stream_limited, run_external_json_command, run_external_json_command_with_ms,
    run_external_json_command_with_timeout, StreamCapture, MAX_EXTERNAL_OUTPUT_CAPTURE_BYTES,
};
pub use handler_delegates::{
    handle_agent, handle_artifacts, handle_assign, handle_claim_next, handle_doctor,
    handle_load_profile, handle_monitor, handle_prompt, handle_register, handle_release,
    handle_resume, handle_resume_context, handle_run_once, handle_smoke, handle_spawn_prompts,
    handle_status,
};
pub use input_parsing::{ParseError, ParseInput};
pub use loop_executor::run_protocol_loop;
pub use schema_loader::{
    current_repo_root, load_schema_sql, EMBEDDED_COORDINATOR_SCHEMA_REF,
    EMBEDDED_COORDINATOR_SCHEMA_SQL,
};

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct ProtocolRequest {
    pub cmd: String,
    pub rid: Option<String>,
    pub dry: Option<bool>,
    #[serde(flatten)]
    pub args: Map<String, Value>,
}

fn bounded_history_limit(limit: Option<i64>) -> i64 {
    parsing::bounded_history_limit(limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT)
}

pub async fn process_protocol_line(line: &str) -> std::result::Result<(), SwarmError> {
    let mut stdout = tokio::io::stdout();
    let started = Instant::now();
    let maybe_rid = parsing::parse_rid(line);
    let parsed = serde_json::from_str::<ProtocolRequest>(line).map_err(|err| {
        ProtocolEnvelope::error(
            maybe_rid.clone(),
            code::INVALID.to_string(),
            format!("Invalid request JSON: {err}"),
        )
        .with_fix("Ensure request is valid JSON with a 'cmd' field. Example: echo '{\"cmd\":\"doctor\"}' | swarm".to_string())
        .with_ctx(json!({"line": line}))
    });

    let (envelope, audit_cmd, audit_args) = match parsed {
        Ok(request) => {
            let command_name = request.cmd.clone();
            let command_args = Value::Object(request.args.clone());
            let rid = request.rid.clone();
            let result = dispatcher::execute_request(request).await;
            let env = match result {
                Ok(success) => ProtocolEnvelope::success(rid, success.data)
                    .with_next(success.next)
                    .with_state(success.state),
                Err(failure) => *failure,
            };
            (
                env.with_ms(i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)),
                command_name,
                command_args,
            )
        }
        Err(env) => (
            env.with_ms(i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)),
            "invalid".to_string(),
            json!({"raw": line}),
        ),
    };

    let response_text = serde_json::to_string(&envelope).map_err(SwarmError::SerializationError)?;
    stdout
        .write_all(response_text.as_bytes())
        .await
        .map_err(SwarmError::IoError)?;
    stdout.write_all(b"\n").await.map_err(SwarmError::IoError)?;

    let mut audit_args = audit_args;
    audit::mask_passwords_in_args(&mut audit_args);

    let candidates = crate::config::database_url_candidates_for_cli();
    let audit_result = audit::audit_request(
        &audit_cmd,
        maybe_rid.as_deref(),
        audit_args,
        envelope.ok,
        started.elapsed().as_millis() as u64,
        envelope.err.as_ref().map(|e| e.code.as_str()),
        &candidates,
        database_connect_timeout_ms(),
    )
    .await;

    if let Err(e) = audit_result {
        eprintln!("WARN: Audit trail recording failed: {e}");
    }

    if !envelope.ok {
        return Err(SwarmError::Internal(envelope.err.as_ref().map_or_else(
            || "Unknown protocol error".to_string(),
            |e| e.msg.clone(),
        )));
    }

    Ok(())
}

fn database_connect_timeout_ms() -> u64 {
    parsing::parse_database_connect_timeout_ms(
        std::env::var("SWARM_DB_CONNECT_TIMEOUT_MS").ok().as_deref(),
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
}

pub(in crate::protocol_runtime) async fn db_from_request(
    request: &ProtocolRequest,
) -> std::result::Result<crate::SwarmDb, Box<ProtocolEnvelope>> {
    db_resolution::db_from_request(
        request,
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
    .await
}

pub(in crate::protocol_runtime) async fn resolve_database_url_for_init(
    request: &ProtocolRequest,
) -> std::result::Result<String, Box<ProtocolEnvelope>> {
    db_resolution::resolve_database_url_for_init(
        request,
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
    .await
}

pub(in crate::protocol_runtime) async fn minimal_state_for_request(
    request: &ProtocolRequest,
) -> Value {
    helpers::minimal_state_for_request(
        request,
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
    .await
}

pub(in crate::protocol_runtime) fn minimal_state_from_progress(
    progress: &crate::ProgressSummary,
) -> Value {
    helpers::minimal_state_from_progress(progress)
}

pub(in crate::protocol_runtime) fn required_string_arg(
    request: &ProtocolRequest,
    key: &str,
) -> std::result::Result<String, Box<ProtocolEnvelope>> {
    helpers::required_string_arg(request, key)
}

pub(in crate::protocol_runtime) fn to_protocol_failure(
    error: SwarmError,
    rid: Option<String>,
) -> Box<ProtocolEnvelope> {
    helpers::to_protocol_failure(error, rid)
}

pub(in crate::protocol_runtime) fn dry_flag(request: &ProtocolRequest) -> bool {
    helpers::dry_flag(request)
}

pub(in crate::protocol_runtime) fn repo_id_from_request(
    request: &ProtocolRequest,
) -> crate::RepoId {
    db_resolution::repo_id_from_request(request)
}

pub(in crate::protocol_runtime) fn elapsed_ms(start: Instant) -> u64 {
    loop_executor::elapsed_ms(start)
}

#[cfg(test)]
fn parse_database_connect_timeout_ms(raw: Option<&str>) -> u64 {
    parsing::parse_database_connect_timeout_ms(
        raw,
        DEFAULT_DB_CONNECT_TIMEOUT_MS,
        MIN_DB_CONNECT_TIMEOUT_MS,
        MAX_DB_CONNECT_TIMEOUT_MS,
    )
}

#[cfg(test)]
mod tests;
