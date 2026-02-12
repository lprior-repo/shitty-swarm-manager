use super::super::{
    current_repo_root, db_from_request, dry_flag, dry_run_success, minimal_state_for_request,
    repo_id_from_request, to_protocol_failure, CommandSuccess, ParseInput, ProtocolRequest,
};
use crate::agent_runtime::run_smoke_once;
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, AgentId, SwarmDb, SwarmError};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use tokio::fs;

pub(in crate::protocol_runtime) async fn handle_spawn_prompts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let (template_text, template_name) =
        if let Some(path) = request.args.get("template").and_then(Value::as_str) {
            let text = fs::read_to_string(path).await.map_err(|err| {
                Box::new(
                    ProtocolEnvelope::error(
                        request.rid.clone(),
                        code::NOTFOUND.to_string(),
                        format!("Template file not found: {err}"),
                    )
                    .with_fix(format!("Ensure {path} exists"))
                    .with_ctx(json!({"template": path})),
                )
            })?;
            (text, path.to_string())
        } else {
            let repo_root = current_repo_root().await?;
            let template_path = crate::prompts::canonical_agent_prompt_path(&repo_root);
            let text = crate::prompts::load_agent_prompt_template(&repo_root)
                .await
                .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
            (text, template_path.to_string_lossy().to_string())
        };

    let out_dir = request
        .args
        .get("out_dir")
        .and_then(Value::as_str)
        .map_or(".agents/generated", |value| value);

    let count = request
        .args
        .get("count")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(10);

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "read_template", "target": template_name}),
                json!({"step": 2, "action": "write_prompts", "target": count, "dir": out_dir}),
            ],
            "swarm monitor --view progress",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let configured_count = db
        .get_config(&repo_id)
        .await
        .ok()
        .map_or(count, |cfg| cfg.max_agents);

    fs::create_dir_all(out_dir)
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    spawn_prompts_recursive(
        out_dir,
        &template_text,
        1,
        configured_count,
        request.rid.clone(),
    )
    .await?;

    Ok(CommandSuccess {
        data: json!({"count": configured_count, "out_dir": out_dir, "template": template_name}),
        next: "swarm monitor --view active".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn spawn_prompts_recursive<'a>(
    out_dir: &'a str,
    template_text: &'a str,
    next: u32,
    count: u32,
    rid: Option<String>,
) -> Pin<Box<dyn Future<Output = std::result::Result<(), Box<ProtocolEnvelope>>> + Send + 'a>> {
    Box::pin(async move {
        if next > count {
            Ok(())
        } else {
            let file = format!("{out_dir}/agent_{next:02}.md");
            fs::write(file, template_text.replace("{N}", &next.to_string()))
                .await
                .map_err(SwarmError::IoError)
                .map_err(|e| to_protocol_failure(e, rid.clone()))?;

            spawn_prompts_recursive(out_dir, template_text, next.saturating_add(1), count, rid)
                .await
        }
    })
}

pub(in crate::protocol_runtime) async fn handle_prompt(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::PromptInput::parse_input(request).map_err(|error| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                error.to_string(),
            )
            .with_fix("echo '{\"cmd\":\"prompt\",\"id\":1}' | swarm".to_string())
            .with_ctx(json!({"error": error.to_string()})),
        )
    })?;

    if let Some(skill_name) = input.skill.as_deref() {
        if let Some(prompt) = crate::skill_prompts::get_skill_prompt(skill_name) {
            return Ok(CommandSuccess {
                data: json!({"skill": skill_name, "prompt": prompt}),
                next: "swarm monitor --view progress".to_string(),
                state: minimal_state_for_request(request).await,
            });
        }
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::NOTFOUND.to_string(),
                format!("Skill prompt not found: {skill_name}"),
            )
            .with_fix(
                "Use a valid skill: rust-contract, implement, qa-enforcer, red-queen".to_string(),
            )
            .with_ctx(json!({"skill": skill_name})),
        ));
    }

    let id = input.id;

    let repo_root = current_repo_root().await?;
    let prompt = crate::prompts::get_agent_prompt(&repo_root, id)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"agent_id": id, "prompt": prompt}),
        next: format!("swarm agent --id {id}"),
        state: minimal_state_for_request(request).await,
    })
}

pub(in crate::protocol_runtime) async fn handle_smoke(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let id = request
        .args
        .get("id")
        .and_then(Value::as_u64)
        .map_or(1, |v| v) as u32;
    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "run_smoke", "target": id})],
            "swarm monitor --view progress",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    run_smoke_once(&db, &AgentId::new(repo_id, id))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({"agent_id": id, "status": "completed"}),
        next: "swarm monitor --view progress".to_string(),
        state: minimal_state_for_request(request).await,
    })
}
