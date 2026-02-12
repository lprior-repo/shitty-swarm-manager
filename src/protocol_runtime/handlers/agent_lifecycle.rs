use super::super::{
    db_from_request, dry_flag, dry_run_success, minimal_state_for_request, repo_id_from_request,
    to_protocol_failure, CommandSuccess, ParseInput, ProtocolRequest, MAX_REGISTER_COUNT,
};
use crate::agent_runtime::run_agent;
use crate::config::load_config;
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, AgentId, RepoId, SwarmDb};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;

pub(in crate::protocol_runtime) async fn handle_register(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::RegisterInput::parse_input(request).map_err(|error| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                error.to_string(),
            )
            .with_fix("echo '{\"cmd\":\"register\",\"count\":3}' | swarm".to_string())
            .with_ctx(json!({"error": error.to_string()})),
        )
    })?;

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id_from_context = repo_id_from_request(request);
    let config = db.get_config(&repo_id_from_context).await.ok();

    let count = input
        .count
        .or_else(|| config.as_ref().map(|c| c.max_agents))
        .map_or(10, |value| value);

    if count == 0 {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "count must be greater than 0".to_string(),
            )
            .with_fix(
                "Provide a positive count. Example: {\"cmd\":\"register\",\"count\":3}".to_string(),
            )
            .with_ctx(json!({"count": count})),
        ));
    }

    if count > MAX_REGISTER_COUNT {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("count must be less than or equal to {MAX_REGISTER_COUNT}"),
            )
            .with_fix(format!(
                "Provide count <= {MAX_REGISTER_COUNT}. Example: {{\"cmd\":\"register\",\"count\":10}}"
            ))
            .with_ctx(json!({"count": count, "max_count": MAX_REGISTER_COUNT})),
        ));
    }

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "register_repo", "target": "current_repo"}),
                json!({"step": 2, "action": "register_agents", "target": count}),
            ],
            "swarm status",
        ));
    }

    let repo_id = RepoId::from_current_dir().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "Not in a git repository".to_string(),
            )
            .with_fix("Run command from a git repository root".to_string()),
        )
    })?;
    db.register_repo(&repo_id, repo_id.value(), ".")
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    if let Some(explicit_count) = input.count {
        let _ = db.update_config(explicit_count).await;
    }

    register_agents_recursive(&db, &repo_id, 1, count, request.rid.clone()).await?;

    Ok(CommandSuccess {
        data: json!({"repo": repo_id.value(), "count": count}),
        next: "swarm status".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn register_agents_recursive<'a>(
    db: &'a SwarmDb,
    repo_id: &'a RepoId,
    next: u32,
    count: u32,
    rid: Option<String>,
) -> Pin<Box<dyn Future<Output = std::result::Result<(), Box<ProtocolEnvelope>>> + Send + 'a>> {
    Box::pin(async move {
        if next > count {
            Ok(())
        } else {
            db.register_agent(&AgentId::new(repo_id.clone(), next))
                .await
                .map_err(|e| to_protocol_failure(e, rid.clone()))?;

            register_agents_recursive(db, repo_id, next.saturating_add(1), count, rid).await
        }
    })
}

pub(in crate::protocol_runtime) async fn handle_agent(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::AgentInput::parse_input(request).map_err(|e| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                e.to_string(),
            )
            .with_fix("echo '{\"cmd\":\"agent\",\"id\":1}' | swarm".to_string())
            .with_ctx(json!({"error": e.to_string()})),
        )
    })?;

    if dry_flag(request) || input.dry.unwrap_or(false) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "run_agent", "target": input.id})],
            "swarm status",
        ));
    }

    let config = load_config();
    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = RepoId::from_current_dir().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "Not in git repository".to_string(),
            )
            .with_fix("Run from repo root".to_string()),
        )
    })?;
    run_agent(
        &db,
        &AgentId::new(repo_id, input.id),
        &config.stage_commands,
    )
    .await
    .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"agent_id": input.id, "status": "completed"}),
        next: "swarm monitor --view progress".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

pub(in crate::protocol_runtime) async fn handle_release(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let agent_id = request
        .args
        .get("agent_id")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing agent_id".to_string(),
                )
                .with_fix("swarm release --agent-id 1".to_string())
                .with_ctx(json!({"agent_id": "required"})),
            )
        })? as u32;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "release_agent", "target": agent_id})],
            "swarm status",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let released = db
        .release_agent(&AgentId::new(repo_id, agent_id))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"agent_id": agent_id, "released_bead": released.map(|b| b.value().to_string())}),
        next: "swarm status".to_string(),
        state: minimal_state_for_request(request).await,
    })
}
