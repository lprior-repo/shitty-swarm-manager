use super::ProtocolRequest;
use crate::code;
use crate::protocol_envelope::ProtocolEnvelope;
use crate::protocol_runtime::handlers;
use serde_json::json;

pub struct CommandSuccess {
    pub data: serde_json::Value,
    pub next: String,
    pub state: serde_json::Value,
}

#[must_use]
pub fn project_next_recommendation(payload: &serde_json::Value) -> serde_json::Value {
    if payload.get("id").is_some() {
        return payload.clone();
    }

    if let Some(next) = payload.get("next") {
        return next.clone();
    }

    if let Some(recommendation) = payload.get("recommendation") {
        return recommendation.clone();
    }

    payload
        .pointer("/triage/quick_ref/top_picks/0")
        .cloned()
        .unwrap_or_else(|| payload.clone())
}

pub fn bead_id_from_recommendation(recommendation: &serde_json::Value) -> Option<String> {
    recommendation
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(std::string::ToString::to_string)
}

/// # Errors
/// Returns an error if request validation fails or command execution fails.
pub async fn execute_request(
    request: ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    super::validation::validate_request_null_bytes(&request)?;

    match request.cmd.as_str() {
        "batch" => handlers::batch_ops::handle_batch(&request).await,
        _ => execute_request_no_batch(request).await,
    }
}

/// # Errors
/// Returns an error if request validation fails or command execution fails.
pub async fn execute_request_no_batch(
    request: ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    super::validation::validate_request_args(&request)?;

    dispatch_request(&request).await
}

pub async fn dispatch_request(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let cmd = request.cmd.as_str();

    match cmd {
        "batch" => handlers::batch_ops::handle_batch(request).await,
        other => dispatch_no_batch(request, other).await,
    }
}

#[allow(clippy::large_stack_frames)]
/// # Errors
/// Returns an error if command execution fails.
pub async fn dispatch_no_batch(
    request: &ProtocolRequest,
    cmd: &str,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    match cmd {
        "?" | "help" => handlers::batch_ops::handle_help(request).await,
        "state" => handlers::state_ops::handle_state(request).await,
        "history" => handlers::state_ops::handle_history(request).await,
        "lock" => handlers::lock_ops::handle_lock(request).await,
        "unlock" => handlers::lock_ops::handle_unlock(request).await,
        "agents" => handlers::state_ops::handle_agents(request).await,
        "broadcast" => handlers::messaging_ops::handle_broadcast(request).await,
        "monitor" => super::handle_monitor(request).await,
        "register" => super::handle_register(request).await,
        "agent" => super::handle_agent(request).await,
        "status" => super::handle_status(request).await,
        "next" => handlers::qa_ops::handle_next(request).await,
        "claim-next" => super::handle_claim_next(request).await,
        "assign" => super::handle_assign(request).await,
        "run-once" => super::handle_run_once(request).await,
        "qa" => handlers::qa_ops::handle_qa(request).await,
        "resume" => super::handle_resume(request).await,
        "resume-context" => super::handle_resume_context(request).await,
        "artifacts" => super::handle_artifacts(request).await,
        "release" => super::handle_release(request).await,
        "init-db" => handlers::swarm_ops::handle_init_db(request).await,
        "init-local-db" => handlers::swarm_ops::handle_init_local_db(request).await,
        "spawn-prompts" => super::handle_spawn_prompts(request).await,
        "smoke" => super::handle_smoke(request).await,
        "prompt" => super::handle_prompt(request).await,
        "doctor" => super::handle_doctor(request).await,
        "load-profile" => super::handle_load_profile(request).await,
        "bootstrap" => handlers::swarm_ops::handle_bootstrap(request).await,
        "init" => handlers::swarm_ops::handle_init(request).await,
        other => Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("Unknown command: {other}"),
            )
            .with_fix(
                "Use a valid command: init, doctor, status, next, claim-next, assign, run-ononce, qa, resume, artifacts, resume-context, agent, smoke, prompt, register, release, monitor, init-db, init-local-db, spawn-prompts, batch, bootstrap, state, or ?/help for help".to_string()
            )
            .with_ctx(json!({"cmd": other})),
        )),
    }
}

#[must_use]
pub fn dry_run_success(
    _request: &ProtocolRequest,
    steps: Vec<serde_json::Value>,
    next: &str,
) -> CommandSuccess {
    CommandSuccess {
        data: serde_json::json!({
            "dry": true,
            "would_do": steps,
            "estimated_ms": 250,
            "reversible": true,
            "side_effects": [],
        }),
        next: next.to_string(),
        state: serde_json::json!({"total": 0, "active": 0}),
    }
}
