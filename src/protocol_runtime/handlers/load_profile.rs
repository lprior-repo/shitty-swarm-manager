use super::super::{
    db_from_request, dry_flag, dry_run_success, minimal_state_for_request, repo_id_from_request,
    to_protocol_failure, CommandSuccess, ProtocolRequest,
};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{AgentId, RepoId, SwarmDb};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

pub(in crate::protocol_runtime) async fn handle_load_profile(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let agents = request
        .args
        .get("agents")
        .and_then(Value::as_u64)
        .map_or(90, |v| v) as u32;
    let rounds = request
        .args
        .get("rounds")
        .and_then(Value::as_u64)
        .map_or(5, |v| v) as u32;
    let timeout_ms = request
        .args
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .map_or(1500, |v| v);

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "load_profile", "target": format!("{}x{}", agents, rounds)}),
            ],
            "swarm status",
        ));
    }

    let db: SwarmDb = db_from_request(request).await?;
    let repo_id = repo_id_from_request(request);
    db.seed_idle_agents(agents)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    db.enqueue_backlog_batch(&repo_id, "load", agents.saturating_mul(rounds))
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    let stats = load_profile_recursive(
        &db,
        &repo_id,
        0,
        rounds,
        agents,
        timeout_ms,
        LoadStats::default(),
    )
    .await?;

    Ok(CommandSuccess {
        data: json!({
            "agents": agents,
            "rounds": rounds,
            "timeouts": stats.timeout,
            "errors": stats.error,
            "successful_claims": stats.success,
            "empty_claims": stats.empty,
        }),
        next: "swarm status".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn load_profile_recursive<'a>(
    db: &'a SwarmDb,
    repo_id: &'a RepoId,
    current_round: u32,
    total_rounds: u32,
    agents_per_round: u32,
    timeout_ms: u64,
    stats: LoadStats,
) -> Pin<Box<dyn Future<Output = std::result::Result<LoadStats, Box<ProtocolEnvelope>>> + Send + 'a>>
{
    Box::pin(async move {
        if current_round >= total_rounds {
            Ok(stats)
        } else {
            let round_stats = load_profile_round_recursive(
                db,
                repo_id,
                1,
                agents_per_round,
                timeout_ms,
                LoadStats::default(),
            )
            .await?;

            let next_stats = LoadStats {
                success: stats.success.saturating_add(round_stats.success),
                empty: stats.empty.saturating_add(round_stats.empty),
                timeout: stats.timeout.saturating_add(round_stats.timeout),
                error: stats.error.saturating_add(round_stats.error),
            };

            load_profile_recursive(
                db,
                repo_id,
                current_round.saturating_add(1),
                total_rounds,
                agents_per_round,
                timeout_ms,
                next_stats,
            )
            .await
        }
    })
}

fn load_profile_round_recursive<'a>(
    db: &'a SwarmDb,
    repo_id: &'a RepoId,
    agent_num: u32,
    total_agents: u32,
    timeout_ms: u64,
    mut stats: LoadStats,
) -> Pin<Box<dyn Future<Output = std::result::Result<LoadStats, Box<ProtocolEnvelope>>> + Send + 'a>>
{
    Box::pin(async move {
        if agent_num > total_agents {
            Ok(stats)
        } else {
            let timeout_dur = tokio::time::Duration::from_millis(timeout_ms);
            let claim = tokio::time::timeout(
                timeout_dur,
                db.claim_next_bead(&AgentId::new(repo_id.clone(), agent_num)),
            )
            .await;

            match claim {
                Ok(Ok(Some(_))) => stats.success = stats.success.saturating_add(1),
                Ok(Ok(None)) => stats.empty = stats.empty.saturating_add(1),
                Ok(Err(_)) => stats.error = stats.error.saturating_add(1),
                Err(_) => stats.timeout = stats.timeout.saturating_add(1),
            }

            load_profile_round_recursive(
                db,
                repo_id,
                agent_num.saturating_add(1),
                total_agents,
                timeout_ms,
                stats,
            )
            .await
        }
    })
}

#[derive(Default, Clone, Copy)]
struct LoadStats {
    success: u64,
    empty: u64,
    timeout: u64,
    error: u64,
}
