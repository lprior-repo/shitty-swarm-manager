use super::super::{
    db_from_request, minimal_state_for_request, repo_id_from_request, to_protocol_failure,
    CommandSuccess, ProtocolRequest,
};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, ResumeContextContract, SwarmDb};
use serde_json::json;

pub(in crate::protocol_runtime) async fn handle_resume(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let contexts = db
        .get_resume_context_projections(&repo_id)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    let contracts = contexts
        .iter()
        .map(ResumeContextContract::from_projection)
        .collect::<Vec<_>>();

    Ok(CommandSuccess {
        data: json!({
            "contexts": contracts,
        }),
        next: "swarm monitor --view failures".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

pub(in crate::protocol_runtime) async fn handle_resume_context(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let bead_filter = parse_resume_context_bead_filter(request)?;

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    let contexts = db
        .get_deep_resume_contexts(&repo_id)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    let selected = if let Some(ref bead_id) = bead_filter {
        let filtered = contexts
            .into_iter()
            .filter(|context| context.bead_id == *bead_id)
            .collect::<Vec<_>>();
        if filtered.is_empty() {
            return Err(Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::NOTFOUND.to_string(),
                    format!("Bead {bead_id} not found or not resumable"),
                )
                .with_fix("swarm resume-context --bead-id <bead-id>".to_string())
                .with_ctx(json!({"bead_id": bead_id})),
            ));
        }
        filtered
    } else {
        contexts
    };

    Ok(CommandSuccess {
        data: json!({"contexts": selected}),
        next: "swarm monitor --view failures".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn parse_resume_context_bead_filter(
    request: &ProtocolRequest,
) -> std::result::Result<Option<String>, Box<ProtocolEnvelope>> {
    let Some(raw) = request.args.get("bead_id") else {
        return Ok(None);
    };

    let bead_id = raw.as_str().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "bead_id must be a string".to_string(),
            )
            .with_fix("Use --bead-id <bead-id> with a non-empty string value".to_string())
            .with_ctx(json!({"bead_id": raw})),
        )
    })?;

    if bead_id.trim().is_empty() {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "bead_id cannot be empty".to_string(),
            )
            .with_fix("Use --bead-id <bead-id> with a non-empty value".to_string())
            .with_ctx(json!({"bead_id": bead_id})),
        ));
    }

    Ok(Some(bead_id.to_string()))
}
