use crate::agent_runtime_support::{
    build_full_message_body, execute_stage, stage_failure_message_type, stage_primary_artifact,
    stage_success_message_type,
};
use crate::config::StageCommands;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tracing::{error, info, warn};

use serde_json::json;
use swarm::{
    stage_executors::execute_stage_rust, AgentId, ArtifactType, BeadId, MessageType, Result, Stage,
    StageResult, SwarmDb, SwarmError,
};

pub async fn run_agent(
    db: &SwarmDb,
    agent_id: &AgentId,
    stage_commands: &StageCommands,
) -> Result<()> {
    run_agent_recursive(db, agent_id, stage_commands).await
}

pub async fn run_smoke_once(db: &SwarmDb, agent_id: &AgentId) -> Result<()> {
    let maybe_bead = db.claim_next_bead(agent_id).await?;
    match maybe_bead {
        Some(bead_id) => {
            println!("Running smoke pipeline for bead {}", bead_id);
            let stages = [
                Stage::RustContract,
                Stage::Implement,
                Stage::QaEnforcer,
                Stage::RedQueen,
            ];
            run_smoke_stages_recursive(db, agent_id, &bead_id, &stages, 0).await?;
            println!("Smoke pipeline completed for bead {}", bead_id);
            Ok(())
        }
        None => {
            println!("No pending p0 beads available for smoke run.");
            Ok(())
        }
    }
}

fn run_agent_recursive<'a>(
    db: &'a SwarmDb,
    agent_id: &'a AgentId,
    stage_commands: &'a StageCommands,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match db.get_agent_state(agent_id).await? {
            None => {
                error!("Agent {} not registered", agent_id);
                Ok(())
            }
            Some(state) => match state.status {
                swarm::AgentStatus::Idle => match db.claim_next_bead(agent_id).await? {
                    Some(bead_id) => {
                        info!("Agent {} claimed bead {}", agent_id, bead_id);
                        run_agent_recursive(db, agent_id, stage_commands).await
                    }
                    None => {
                        info!("Agent {} found no available beads", agent_id);
                        Ok(())
                    }
                },
                swarm::AgentStatus::Done => {
                    info!("Agent {} completed work", agent_id);
                    Ok(())
                }
                swarm::AgentStatus::Working | swarm::AgentStatus::Waiting => {
                    process_work_state(db, agent_id, stage_commands, state).await
                }
                _ => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    run_agent_recursive(db, agent_id, stage_commands).await
                }
            },
        }
    })
}

async fn process_work_state(
    db: &SwarmDb,
    agent_id: &AgentId,
    stage_commands: &StageCommands,
    state: swarm::AgentState,
) -> Result<()> {
    if state.implementation_attempt >= 3 {
        return match state.bead_id {
            Some(bead_id) => {
                let reason = "Max implementation attempts (3) exceeded";
                db.mark_bead_blocked(agent_id, &bead_id, reason).await?;
                warn!("Agent {} blocked bead {}: {}", agent_id, bead_id, reason);
                Ok(())
            }
            None => Ok(()),
        };
    }

    if let (Some(stage), Some(bead_id)) = (state.current_stage, state.bead_id) {
        let unread_messages = db.get_unread_messages(agent_id, Some(&bead_id)).await?;
        let feedback_messages = unread_messages
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
            .collect::<Vec<_>>();

        let attempt = state.implementation_attempt.saturating_add(1);
        let started = Instant::now();
        db.record_stage_started(agent_id, &bead_id, stage, attempt)
            .await?;

        let stage_history_id = db
            .get_stage_history_id(agent_id, &bead_id, stage, attempt)
            .await?
            .ok_or_else(|| {
                SwarmError::DatabaseError(
                    "Missing started stage_history row for artifact storage".to_string(),
                )
            })?;

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

        let rust_result = execute_stage_rust(db, stage, &bead_id, agent_id, stage_history_id).await;
        let (result, used_fallback) = match &rust_result {
            StageResult::Error(err) => {
                warn!(
                    "Rust stage executor errored for bead {} stage {}: {}; falling back to configured command",
                    bead_id,
                    stage,
                    err
                );
                (
                    execute_stage(stage, &bead_id, agent_id, stage_commands).await,
                    true,
                )
            }
            _ => (rust_result, false),
        };
        let status = result.as_str();
        let is_success = result.is_success();

        let result_message = result
            .message()
            .map_or_else(String::new, std::string::ToString::to_string);

        if used_fallback {
            db.store_stage_artifact(
                stage_history_id,
                ArtifactType::StageLog,
                &result_message,
                Some(json!({
                    "stage": stage.as_str(),
                    "status": result.as_str(),
                    "attempt": attempt,
                })),
            )
            .await?;

            db.store_stage_artifact(
                stage_history_id,
                stage_primary_artifact(stage, &result),
                &result_message,
                Some(json!({
                    "stage": stage.as_str(),
                    "status": result.as_str(),
                    "source": "fallback_shell_executor",
                })),
            )
            .await?;
        }

        let stage_artifacts = db
            .get_stage_artifacts(stage_history_id)
            .await
            .unwrap_or_else(|_| Vec::new());
        let artifact_types = stage_artifacts
            .iter()
            .map(|artifact| artifact.artifact_type.as_str().to_string())
            .collect::<Vec<_>>();
        let message_body = build_full_message_body(
            stage,
            &status,
            &bead_id,
            &result_message,
            &stage_artifacts,
            is_success,
        );

        db.record_stage_complete(
            agent_id,
            &bead_id,
            stage,
            attempt,
            result.clone(),
            started.elapsed().as_millis() as u64,
        )
        .await?;

        let maybe_message_type = if is_success {
            stage_success_message_type(stage)
        } else {
            stage_failure_message_type(stage)
        };

        if let Some(message_type) = maybe_message_type {
            let subject = format!("{} {} {}", stage.as_str(), status, bead_id.value());
            let body = message_body;

            db.send_agent_message(
                agent_id,
                Some(agent_id),
                Some(&bead_id),
                message_type,
                &subject,
                &body,
                Some(json!({
                    "stage": stage.as_str(),
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
        db.mark_messages_read(agent_id, &unread_message_ids).await?;
    }

    run_agent_recursive(db, agent_id, stage_commands).await
}

fn run_smoke_stages_recursive<'a>(
    db: &'a SwarmDb,
    agent_id: &'a AgentId,
    bead_id: &'a BeadId,
    stages: &'a [Stage],
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
                        db.record_stage_started(agent_id, bead_id, stage, 1).await?;
                        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
                        db.record_stage_complete(
                            agent_id,
                            bead_id,
                            stage,
                            1,
                            StageResult::Passed,
                            started.elapsed().as_millis() as u64,
                        )
                        .await?;
                        run_smoke_stages_recursive(db, agent_id, bead_id, stages, idx + 1).await
                    })
                },
            )
            .await
    })
}
