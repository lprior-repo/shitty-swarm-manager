use super::dispatcher::CommandSuccess;
use super::ProtocolRequest;
use crate::protocol_envelope::ProtocolEnvelope;
use crate::protocol_runtime::handlers;

pub async fn handle_claim_next(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::orchestration::handle_claim_next(request).await
}

pub async fn handle_assign(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::orchestration::handle_assign(request).await
}

pub async fn handle_run_once(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::orchestration::handle_run_once(request).await
}

#[allow(clippy::too_many_lines)]
pub async fn handle_monitor(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::monitoring::handle_monitor(request).await
}

pub async fn handle_register(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::agent_lifecycle::handle_register(request).await
}

pub async fn handle_agent(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::agent_lifecycle::handle_agent(request).await
}

pub async fn handle_status(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::monitoring::handle_status(request).await
}

pub async fn handle_resume(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::resume::handle_resume(request).await
}

pub async fn handle_resume_context(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::resume::handle_resume_context(request).await
}

pub async fn handle_artifacts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::artifacts::handle_artifacts(request).await
}

pub async fn handle_release(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::agent_lifecycle::handle_release(request).await
}

pub async fn handle_spawn_prompts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::prompts::handle_spawn_prompts(request).await
}

pub async fn handle_prompt(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::prompts::handle_prompt(request).await
}

pub async fn handle_smoke(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::prompts::handle_smoke(request).await
}

pub async fn handle_doctor(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::doctor::handle_doctor(request).await
}

pub async fn handle_load_profile(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    handlers::load_profile::handle_load_profile(request).await
}
