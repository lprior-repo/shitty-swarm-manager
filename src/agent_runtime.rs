#![allow(clippy::branches_sharing_code)]

use crate::agent_runtime_support::{
    build_full_message_body, execute_stage, stage_failure_message_type, stage_primary_artifact,
    stage_success_message_type,
};
use crate::config::StageCommands;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;
use tracing::{error, info, warn};

use serde_json::json;
use swarm::{
    runtime_determine_transition, stage_executors::execute_stage_rust, AgentId, ArtifactType,
    BeadId, MessageType, RepoId, Result, RuntimeAgentId as AgentKey,
    RuntimeAgentState as AgentStateKey, RuntimeAgentStatus as AgentStatusKey,
    RuntimeBeadId as BeadKey, RuntimeRepoId as RepoKey, RuntimeStage as StageKey,
    RuntimeStageResult as StageResultKey, RuntimeStageTransition as StageTransitionKey, Stage,
    StageResult, SwarmDb,
};

const MIN_POLL_BACKOFF: Duration = Duration::from_millis(250);
const MAX_POLL_BACKOFF: Duration = Duration::from_secs(5);

pub async fn run_agent(
    db: &SwarmDb,
    agent_id: &AgentId,
    stage_commands: &StageCommands,
) -> Result<()> {
    let agent_key = to_agent_key(agent_id);
    run_agent_loop_recursive(db, &agent_key, stage_commands, MIN_POLL_BACKOFF).await
}

fn run_agent_loop_recursive<'a>(
    db: &'a SwarmDb,
    agent_id: &'a AgentKey,
    stage_commands: &'a StageCommands,
    poll_backoff: Duration,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match get_agent_state_key(db, agent_id).await? {
            None => {
                error!("Agent {} not registered", agent_id);
                Ok(())
            }
            Some(state) => match state.status {
                AgentStatusKey::Idle => {
                    if let Some(bead_id) = claim_next_bead_key(db, agent_id).await? {
                        info!("Agent {} claimed bead {}", agent_id, bead_id.value());
                        crate::agent_runtime_support::create_workspace(
                            agent_id.number(),
                            bead_id.value(),
                        )
                        .await?;
                        run_agent_loop_recursive(db, agent_id, stage_commands, MIN_POLL_BACKOFF)
                            .await
                    } else {
                        info!("Agent {} found no available beads", agent_id);
                        tokio::time::sleep(poll_backoff).await;
                        run_agent_loop_recursive(
                            db,
                            agent_id,
                            stage_commands,
                            next_poll_backoff(poll_backoff),
                        )
                        .await
                    }
                }
                AgentStatusKey::Done => {
                    info!("Agent {} completed work", agent_id);
                    Ok(())
                }
                AgentStatusKey::Working | AgentStatusKey::Waiting => {
                    let progressed =
                        process_work_state(db, agent_id, stage_commands, state).await?;
                    if progressed {
                        run_agent_loop_recursive(db, agent_id, stage_commands, MIN_POLL_BACKOFF)
                            .await
                    } else {
                        tokio::time::sleep(poll_backoff).await;
                        run_agent_loop_recursive(
                            db,
                            agent_id,
                            stage_commands,
                            next_poll_backoff(poll_backoff),
                        )
                        .await
                    }
                }
                AgentStatusKey::Error => {
                    tokio::time::sleep(poll_backoff).await;
                    run_agent_loop_recursive(
                        db,
                        agent_id,
                        stage_commands,
                        next_poll_backoff(poll_backoff),
                    )
                    .await
                }
            },
        }
    })
}

pub async fn run_smoke_once(db: &SwarmDb, agent_id: &AgentId) -> Result<()> {
    let agent_key = to_agent_key(agent_id);
    let maybe_bead = claim_next_bead_key(db, &agent_key).await?;
    if let Some(bead_id) = maybe_bead {
        println!("Running smoke pipeline for bead {}", bead_id.value());
        let stages = [
            StageKey::RustContract,
            StageKey::Implement,
            StageKey::QaEnforcer,
            StageKey::RedQueen,
        ];
        run_smoke_stages_recursive(db, &agent_key, &bead_id, &stages, 0).await?;
        println!("Smoke pipeline completed for bead {}", bead_id.value());
        Ok(())
    } else {
        println!("No pending p0 beads available for smoke run.");
        Ok(())
    }
}

fn next_poll_backoff(current: Duration) -> Duration {
    let doubled_ms = current.as_millis().saturating_mul(2);
    let bounded_ms = doubled_ms.min(MAX_POLL_BACKOFF.as_millis());
    Duration::from_millis(u64::try_from(bounded_ms).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::{next_poll_backoff, MAX_POLL_BACKOFF, MIN_POLL_BACKOFF};
    use std::time::Duration;

    #[test]
    fn when_idle_polling_backoff_doubles_until_cap() {
        let after_first_idle = next_poll_backoff(MIN_POLL_BACKOFF);
        let after_second_idle = next_poll_backoff(after_first_idle);

        assert_eq!(after_first_idle, Duration::from_millis(500));
        assert_eq!(after_second_idle, Duration::from_secs(1));
    }

    #[test]
    fn when_backoff_reaches_cap_it_stays_bounded() {
        let at_cap = next_poll_backoff(MAX_POLL_BACKOFF);
        let above_cap_input = next_poll_backoff(Duration::from_secs(7));

        assert_eq!(at_cap, MAX_POLL_BACKOFF);
        assert_eq!(above_cap_input, MAX_POLL_BACKOFF);
    }
}

#[allow(clippy::too_many_lines)]
async fn process_work_state(
    db: &SwarmDb,
    agent_id: &AgentKey,
    stage_commands: &StageCommands,
    state: AgentStateKey,
) -> Result<bool> {
    if state.implementation_attempt >= 3 {
        return match state.bead_id {
            Some(bead_id) => {
                let reason = "Max implementation attempts (3) exceeded";
                mark_bead_blocked_key(db, agent_id, &bead_id, reason).await?;
                warn!(
                    "Agent {} blocked bead {}: {}",
                    agent_id,
                    bead_id.value(),
                    reason
                );
                crate::agent_runtime_support::finalize_workspace(bead_id.value()).await?;
                Ok(true)
            }
            None => Ok(false),
        };
    }

    if let (Some(stage), Some(bead_id)) = (state.current_stage, state.bead_id) {
        let swarm_agent_id = to_swarm_agent_id(agent_id);
        let swarm_bead_id = to_swarm_bead_id(&bead_id);
        let swarm_stage = to_swarm_stage(stage);

        let unread_messages = db
            .get_unread_messages(&swarm_agent_id, Some(&swarm_bead_id))
            .await?;
        let feedback_messages: Vec<String> = unread_messages
            .iter()
            .filter(|m| {
                matches!(
                    m.message_type,
                    MessageType::QaFailed
                        | MessageType::RedQueenFailed
                        | MessageType::ImplementationRetry
                )
            })
            .map(|m| format!("[{}] {}", m.message_type.as_str(), m.body))
            .collect();

        let attempt = state.implementation_attempt.saturating_add(1);
        let started = Instant::now();
        let stage_history_id =
            record_stage_started_key(db, agent_id, &bead_id, stage, attempt).await?;

        if !feedback_messages.is_empty() {
            let feedback_payload = feedback_messages.join("\n\n");
            db.store_stage_artifact(
                stage_history_id,
                ArtifactType::Feedback,
                &feedback_payload,
                Some(json!({"source": "agent_messages"})),
            )
            .await?;
        }

        let rust_result = execute_stage_rust(
            db,
            swarm_stage,
            &swarm_bead_id,
            &swarm_agent_id,
            stage_history_id,
            None,
        )
        .await;
        let (result, used_fallback) = match &rust_result {
            StageResult::Error(err) => {
                warn!(
                    "Rust stage executor errored for bead {} stage {}: {}; falling back to configured command",
                    bead_id.value(),
                    to_swarm_stage(stage).as_str(),
                    err
                );
                (
                    execute_stage(swarm_stage, &swarm_bead_id, &swarm_agent_id, stage_commands)
                        .await,
                    true,
                )
            }
            _ => (rust_result, false),
        };
        let status = result.as_str();
        let is_success = result.is_success();

        let result_message = result
            .message()
            .map_or_else(String::new, ToString::to_string);

        if used_fallback {
            db.store_stage_artifact(
                stage_history_id,
                ArtifactType::StageLog,
                &result_message,
                Some(json!({
                    "stage": swarm_stage.as_str(),
                    "status": result.as_str(),
                    "attempt": attempt,
                })),
            )
            .await?;

            db.store_stage_artifact(
                stage_history_id,
                stage_primary_artifact(swarm_stage, &result),
                &result_message,
                Some(json!({
                    "stage": swarm_stage.as_str(),
                    "status": result.as_str(),
                    "source": "fallback_shell_executor",
                })),
            )
            .await?;
        }

        let stage_artifacts_result = db.get_stage_artifacts(stage_history_id).await;
        let stage_artifacts = stage_artifacts_result
            .into_iter()
            .flat_map(IntoIterator::into_iter)
            .collect::<Vec<_>>();
        let artifact_types: Vec<String> = stage_artifacts
            .iter()
            .map(|artifact| artifact.artifact_type.as_str().to_string())
            .collect();
        let message_body = build_full_message_body(
            swarm_stage,
            &status,
            &swarm_bead_id,
            &result_message,
            &stage_artifacts,
            is_success,
        );

        let stage_outcome = to_stage_result_key(&result);
        let transition = runtime_determine_transition(stage, &stage_outcome, attempt, 3);

        record_stage_complete_key(
            db,
            agent_id,
            &bead_id,
            stage,
            attempt,
            stage_outcome,
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
        .await?;

        let maybe_message_type = if is_success {
            stage_success_message_type(swarm_stage)
        } else {
            stage_failure_message_type(swarm_stage)
        };

        if let Some(message_type) = maybe_message_type {
            let subject = format!("{} {} {}", swarm_stage.as_str(), status, bead_id.value());
            let body = message_body;

            db.send_agent_message(
                &swarm_agent_id,
                Some(&swarm_agent_id),
                Some(&swarm_bead_id),
                message_type,
                (&subject, &body),
                Some(json!({
                    "stage": swarm_stage.as_str(),
                    "status": status,
                    "attempt": attempt,
                    "stage_history_id": stage_history_id,
                    "artifact_count": stage_artifacts.len(),
                    "artifact_types": artifact_types,
                })),
            )
            .await?;
        }

        let unread_message_ids = unread_messages.iter().map(|m| m.id).collect::<Vec<_>>();
        db.mark_messages_read(&swarm_agent_id, &unread_message_ids)
            .await?;

        match transition {
            StageTransitionKey::Complete => {
                info!(
                    "Agent {} completed bead {} - Landing the plane...",
                    agent_id,
                    bead_id.value()
                );
                crate::agent_runtime_support::finalize_workspace(bead_id.value()).await?;
            }
            StageTransitionKey::Block => {
                let reason = if result_message.is_empty() {
                    "Stage failed at max implementation attempts"
                } else {
                    result_message.as_str()
                };
                mark_bead_blocked_key(db, agent_id, &bead_id, reason).await?;
                warn!(
                    "Agent {} blocked bead {}: {}",
                    agent_id,
                    bead_id.value(),
                    reason
                );
                crate::agent_runtime_support::finalize_workspace(bead_id.value()).await?;
            }
            StageTransitionKey::Advance(_)
            | StageTransitionKey::Retry
            | StageTransitionKey::NoOp => {}
        }

        return Ok(true);
    }

    Ok(false)
}

fn run_smoke_stages_recursive<'a>(
    db: &'a SwarmDb,
    agent_id: &'a AgentKey,
    bead_id: &'a BeadKey,
    stages: &'a [StageKey],
    idx: usize,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        stages
            .get(idx)
            .copied()
            .map_or_else(
                || Box::pin(async { Ok(()) }) as Pin<Box<dyn Future<Output = Result<()>> + Send>>,
                |stage| {
                    Box::pin(async move {
                        let started = Instant::now();
                        record_stage_started_key(db, agent_id, bead_id, stage, 1).await?;
                        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
                        record_stage_complete_key(
                            db,
                            agent_id,
                            bead_id,
                            stage,
                            1,
                            StageResultKey::Passed,
                            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                        )
                        .await?;
                        run_smoke_stages_recursive(db, agent_id, bead_id, stages, idx + 1).await
                    })
                },
            )
            .await
    })
}

fn to_agent_key(agent_id: &AgentId) -> AgentKey {
    AgentKey::new(RepoKey::new(agent_id.repo_id().value()), agent_id.number())
}

fn to_swarm_agent_id(agent_id: &AgentKey) -> AgentId {
    AgentId::new(RepoId::new(agent_id.repo_id.value()), agent_id.number())
}

fn to_swarm_bead_id(bead_id: &BeadKey) -> BeadId {
    BeadId::new(bead_id.value())
}

const fn to_swarm_stage(stage: StageKey) -> Stage {
    match stage {
        StageKey::RustContract => Stage::RustContract,
        StageKey::Implement => Stage::Implement,
        StageKey::QaEnforcer => Stage::QaEnforcer,
        StageKey::RedQueen => Stage::RedQueen,
        StageKey::Done => Stage::Done,
    }
}

fn to_stage_result_key(result: &StageResult) -> StageResultKey {
    match result {
        StageResult::Started => StageResultKey::Started,
        StageResult::Passed => StageResultKey::Passed,
        StageResult::Failed(msg) => StageResultKey::Failed(msg.clone()),
        StageResult::Error(msg) => StageResultKey::Error(msg.clone()),
    }
}

async fn get_agent_state_key(db: &SwarmDb, agent_id: &AgentKey) -> Result<Option<AgentStateKey>> {
    db.get_agent_state(&to_swarm_agent_id(agent_id))
        .await
        .map(|state| {
            state.map(|s| AgentStateKey {
                agent_id: agent_id.clone(),
                bead_id: s.bead_id.map(|bead| BeadKey::new(bead.value())),
                current_stage: s
                    .current_stage
                    .and_then(|stage| stage.as_str().try_into().ok()),
                status: s
                    .status
                    .as_str()
                    .try_into()
                    .unwrap_or(AgentStatusKey::Error),
                implementation_attempt: s.implementation_attempt,
            })
        })
}

async fn claim_next_bead_key(db: &SwarmDb, agent_id: &AgentKey) -> Result<Option<BeadKey>> {
    db.claim_next_bead(&to_swarm_agent_id(agent_id))
        .await
        .map(|bead| bead.map(|b| BeadKey::new(b.value())))
}

async fn mark_bead_blocked_key(
    db: &SwarmDb,
    agent_id: &AgentKey,
    bead_id: &BeadKey,
    reason: &str,
) -> Result<()> {
    db.mark_bead_blocked(
        &to_swarm_agent_id(agent_id),
        &to_swarm_bead_id(bead_id),
        reason,
    )
    .await
}

async fn record_stage_started_key(
    db: &SwarmDb,
    agent_id: &AgentKey,
    bead_id: &BeadKey,
    stage: StageKey,
    attempt: u32,
) -> Result<i64> {
    db.record_stage_started(
        &to_swarm_agent_id(agent_id),
        &to_swarm_bead_id(bead_id),
        to_swarm_stage(stage),
        attempt,
    )
    .await
}

async fn record_stage_complete_key(
    db: &SwarmDb,
    agent_id: &AgentKey,
    bead_id: &BeadKey,
    stage: StageKey,
    attempt: u32,
    result: StageResultKey,
    duration_ms: u64,
) -> Result<()> {
    let swarm_result = match result {
        StageResultKey::Started => StageResult::Started,
        StageResultKey::Passed => StageResult::Passed,
        StageResultKey::Failed(msg) => StageResult::Failed(msg),
        StageResultKey::Error(msg) => StageResult::Error(msg),
    };

    db.record_stage_complete(
        &to_swarm_agent_id(agent_id),
        &to_swarm_bead_id(bead_id),
        to_swarm_stage(stage),
        attempt,
        swarm_result,
        duration_ms,
    )
    .await
}
