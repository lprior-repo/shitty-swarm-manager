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

use serde::Serialize;
use serde_json::json;
use swarm::{
    diagnostics::{classify_failure_category, redact_sensitive},
    map_terminal_sync_state, runtime_determine_transition,
    stage_executors::execute_stage_rust,
    AgentId, ArtifactStore, ArtifactType, BeadId, BrSyncAction, BrSyncStatus, ClaimRepository,
    CoordinatorSyncTerminal, Error, EventSink, LandingGateway, LandingOutcome, MessageType,
    OrchestratorEvent, OrchestratorService, OrchestratorTickOutcome, RepoId, Result,
    RuntimeAgentId as AgentKey, RuntimeAgentState as AgentStateKey,
    RuntimeAgentStatus as AgentStatusKey, RuntimeBeadId as BeadKey, RuntimeRepoId as RepoKey,
    RuntimeStage as StageKey, RuntimeStageResult as StageResultKey,
    RuntimeStageTransition as StageTransitionKey, Stage, StageArtifact, StageArtifactRecord,
    StageExecutionOutcome, StageExecutionRequest, StageExecutor, StageResult, SwarmDb,
};

const MIN_POLL_BACKOFF: Duration = Duration::from_millis(250);
const MAX_POLL_BACKOFF: Duration = Duration::from_secs(5);
const MAX_IMPLEMENTATION_ATTEMPTS: u32 = 3;

struct StageCompletionInput {
    stage: StageKey,
    attempt: u32,
    result: StageResultKey,
    duration_ms: u64,
    apply_transition: bool,
}

struct RuntimeOrchestratorPorts<'a> {
    db: &'a SwarmDb,
    stage_commands: &'a StageCommands,
}

impl ClaimRepository for RuntimeOrchestratorPorts<'_> {
    fn recover_stale_claims(&self) -> swarm::orchestrator_service::PortFuture<'_, u32> {
        Box::pin(async move { self.db.recover_expired_claims().await })
    }

    fn get_agent_state<'a>(
        &'a self,
        agent_id: &'a AgentKey,
    ) -> swarm::orchestrator_service::PortFuture<'a, Option<AgentStateKey>> {
        Box::pin(async move { get_agent_state_key(self.db, agent_id).await })
    }

    fn claim_next_bead<'a>(
        &'a self,
        agent_id: &'a AgentKey,
    ) -> swarm::orchestrator_service::PortFuture<'a, Option<BeadKey>> {
        Box::pin(async move { claim_next_bead_key(self.db, agent_id).await })
    }

    fn create_workspace<'a>(
        &'a self,
        agent_id: &'a AgentKey,
        bead_id: &'a BeadKey,
    ) -> swarm::orchestrator_service::PortFuture<'a, ()> {
        Box::pin(async move {
            info!("Agent {} claimed bead {}", agent_id, bead_id.value());
            crate::agent_runtime_support::create_workspace(agent_id.number(), bead_id.value()).await
        })
    }

    fn heartbeat_claim<'a>(
        &'a self,
        agent_id: &'a AgentKey,
        bead_id: &'a BeadKey,
        lease_extension_ms: i32,
    ) -> swarm::orchestrator_service::PortFuture<'a, bool> {
        Box::pin(async move {
            self.db
                .heartbeat_claim(
                    &to_swarm_agent_id(agent_id),
                    &to_swarm_bead_id(bead_id),
                    lease_extension_ms,
                )
                .await
        })
    }
}

impl StageExecutor for RuntimeOrchestratorPorts<'_> {
    fn execute_work(
        &self,
        request: StageExecutionRequest,
    ) -> swarm::orchestrator_service::PortFuture<'_, StageExecutionOutcome> {
        Box::pin(async move {
            process_work_state(
                self.db,
                request.agent_id(),
                self.stage_commands,
                request.state().clone(),
            )
            .await
            .map(|progressed| {
                if progressed {
                    StageExecutionOutcome::Progressed
                } else {
                    StageExecutionOutcome::Idle
                }
            })
        })
    }
}

impl ArtifactStore for RuntimeOrchestratorPorts<'_> {
    fn store_artifact(
        &self,
        _record: StageArtifactRecord,
    ) -> swarm::orchestrator_service::PortFuture<'_, ()> {
        Box::pin(async move { Ok(()) })
    }
}

impl LandingGateway for RuntimeOrchestratorPorts<'_> {
    fn execute_landing<'a>(
        &'a self,
        _bead_id: &'a BeadKey,
    ) -> swarm::orchestrator_service::PortFuture<'a, LandingOutcome> {
        Box::pin(async move { Ok(LandingOutcome::new(true, "not-invoked-by-tick")) })
    }
}

impl EventSink for RuntimeOrchestratorPorts<'_> {
    fn append_event(
        &self,
        _event: OrchestratorEvent,
    ) -> swarm::orchestrator_service::PortFuture<'_, ()> {
        Box::pin(async move { Ok(()) })
    }
}

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
        let service = OrchestratorService::new(RuntimeOrchestratorPorts { db, stage_commands });
        match service.tick(agent_id).await? {
            OrchestratorTickOutcome::AgentMissing => {
                error!("Agent {} not registered", agent_id);
                Ok(())
            }
            OrchestratorTickOutcome::Completed => {
                info!("Agent {} completed work", agent_id);
                Ok(())
            }
            OrchestratorTickOutcome::Progressed => {
                run_agent_loop_recursive(db, agent_id, stage_commands, MIN_POLL_BACKOFF).await
            }
            OrchestratorTickOutcome::Idle => {
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
    Duration::from_millis(saturating_millis_to_u64(bounded_ms))
}

fn saturating_millis_to_u64(milliseconds: u128) -> u64 {
    u64::try_from(milliseconds).map_or(u64::MAX, |value| value)
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
    if state.implementation_attempt() >= MAX_IMPLEMENTATION_ATTEMPTS {
        return match state.bead_id() {
            Some(bead_id) => {
                let reason =
                    format!("Max implementation attempts ({MAX_IMPLEMENTATION_ATTEMPTS}) exceeded");
                mark_bead_blocked_key(db, agent_id, bead_id, reason).await?;
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

    if let (Some(stage), Some(bead_id)) = (state.current_stage(), state.bead_id()) {
        let swarm_agent_id = to_swarm_agent_id(agent_id);
        let swarm_bead_id = to_swarm_bead_id(bead_id);
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

        let attempt = state.implementation_attempt().saturating_add(1);
        let started = Instant::now();
        let stage_history_id =
            record_stage_started_key(db, agent_id, bead_id, stage, attempt).await?;

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
        let transition = runtime_determine_transition(
            stage,
            &stage_outcome,
            attempt,
            MAX_IMPLEMENTATION_ATTEMPTS,
        );

        if should_persist_retry_packet(transition, swarm_stage) {
            persist_retry_packet(
                db,
                stage_history_id,
                swarm_stage,
                attempt,
                result.message(),
                &stage_artifacts,
            )
            .await?;
        }

        let apply_transition = !matches!(transition, StageTransitionKey::Complete);
        record_stage_complete_key(
            db,
            agent_id,
            bead_id,
            StageCompletionInput {
                stage,
                attempt,
                result: stage_outcome,
                duration_ms: saturating_millis_to_u64(started.elapsed().as_millis()),
                apply_transition,
            },
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
                    "Agent {} entering landing saga for bead {}",
                    agent_id,
                    bead_id.value()
                );
                let landing_outcome =
                    crate::agent_runtime_support::execute_landing_saga(bead_id.value()).await;
                match landing_outcome {
                    Ok(outcome) => {
                        let report = serde_json::to_string_pretty(&outcome)
                            .map_err(swarm::Error::SerializationError)?;

                        db.store_stage_artifact(
                            stage_history_id,
                            ArtifactType::StageLog,
                            &report,
                            Some(json!({
                                "stage": swarm_stage.as_str(),
                                "status": status,
                                "landing": outcome.persistence_payload(),
                            })),
                        )
                        .await?;

                        let sync_decision = map_terminal_sync_state(if outcome.push_confirmed {
                            CoordinatorSyncTerminal::PushConfirmed
                        } else {
                            CoordinatorSyncTerminal::PushUnconfirmed {
                                reason: outcome.failure_summary(),
                            }
                        });

                        if sync_decision.status() == BrSyncStatus::Diverged {
                            warn!(
                                "Landing sync diverged for bead {}: {:?}",
                                bead_id.value(),
                                sync_decision.divergence()
                            );
                        }

                        match sync_decision.action() {
                            BrSyncAction::FinalizeTerminalClaim => {
                                db.finalize_after_push_confirmation(
                                    &to_swarm_agent_id(agent_id),
                                    &to_swarm_bead_id(bead_id),
                                    outcome.push_confirmed,
                                )
                                .await?;
                                info!(
                                    "Agent {} completed bead {} after push confirmation",
                                    agent_id,
                                    bead_id.value()
                                );
                            }
                            BrSyncAction::RecordRetryableFailure { reason } => {
                                db.mark_landing_retryable(&to_swarm_agent_id(agent_id), reason)
                                    .await?;
                                warn!(
                                    "Landing saga incomplete for bead {}: {}",
                                    bead_id.value(),
                                    reason
                                );
                            }
                        }
                    }
                    Err(err) => {
                        let sync_decision =
                            map_terminal_sync_state(CoordinatorSyncTerminal::LandingErrored {
                                reason: format!("Landing saga execution error: {err}"),
                            });

                        if sync_decision.status() == BrSyncStatus::Diverged {
                            warn!(
                                "Landing sync diverged for bead {}: {:?}",
                                bead_id.value(),
                                sync_decision.divergence()
                            );
                        }

                        if let BrSyncAction::RecordRetryableFailure { reason } =
                            sync_decision.action()
                        {
                            db.mark_landing_retryable(&to_swarm_agent_id(agent_id), reason)
                                .await?;
                            warn!(
                                "Landing saga errored for bead {}: {}",
                                bead_id.value(),
                                reason
                            );
                        }
                    }
                }
            }
            StageTransitionKey::Block => {
                let reason = if result_message.is_empty() {
                    "Stage failed at max implementation attempts"
                } else {
                    result_message.as_str()
                };
                mark_bead_blocked_key(db, agent_id, bead_id, reason).await?;
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
                            StageCompletionInput {
                                stage,
                                attempt: 1,
                                result: StageResultKey::Passed,
                                duration_ms: saturating_millis_to_u64(
                                    started.elapsed().as_millis(),
                                ),
                                apply_transition: true,
                            },
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
    AgentId::new(RepoId::new(agent_id.repo_id().value()), agent_id.number())
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
            state.map(|s| {
                let runtime_status = s
                    .status
                    .as_str()
                    .try_into()
                    .map_or(AgentStatusKey::Error, |status| status);

                AgentStateKey::new(
                    agent_id.clone(),
                    s.bead_id.as_ref().map(|bead| BeadKey::new(bead.value())),
                    s.current_stage
                        .and_then(|stage| stage.as_str().try_into().ok()),
                    runtime_status,
                    s.implementation_attempt,
                )
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
    completion: StageCompletionInput,
) -> Result<()> {
    let swarm_result = match completion.result {
        StageResultKey::Started => StageResult::Started,
        StageResultKey::Passed => StageResult::Passed,
        StageResultKey::Failed(msg) => StageResult::Failed(msg),
        StageResultKey::Error(msg) => StageResult::Error(msg),
    };

    if completion.apply_transition {
        db.record_stage_complete(
            &to_swarm_agent_id(agent_id),
            &to_swarm_bead_id(bead_id),
            to_swarm_stage(completion.stage),
            completion.attempt,
            swarm_result,
            completion.duration_ms,
        )
        .await
    } else {
        db.record_stage_complete_without_transition(
            &to_swarm_agent_id(agent_id),
            &to_swarm_bead_id(bead_id),
            to_swarm_stage(completion.stage),
            completion.attempt,
            &swarm_result,
            completion.duration_ms,
        )
        .await
    }
}

fn should_persist_retry_packet(transition: StageTransitionKey, stage: Stage) -> bool {
    matches!(transition, StageTransitionKey::Retry)
        && matches!(stage, Stage::QaEnforcer | Stage::RedQueen)
}

async fn persist_retry_packet(
    db: &SwarmDb,
    stage_history_id: i64,
    stage: Stage,
    attempt: u32,
    failure_message: Option<&str>,
    stage_artifacts: &[StageArtifact],
) -> Result<()> {
    let packet = RetryPacket::new(stage, attempt, failure_message, stage_artifacts);
    let payload = serde_json::to_string(&packet).map_err(Error::SerializationError)?;

    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::RetryPacket,
        &payload,
        Some(json!({
            "stage": stage.as_str(),
            "attempt": attempt,
        })),
    )
    .await
    .map(|_| ())
}

#[derive(Serialize)]
struct RetryPacket {
    stage: String,
    attempt: u32,
    remaining_attempts: u32,
    failure_category: String,
    failure_message: Option<String>,
    artifact_references: Vec<RetryArtifactReference>,
}

impl RetryPacket {
    fn new(
        stage: Stage,
        attempt: u32,
        failure_message: Option<&str>,
        stage_artifacts: &[StageArtifact],
    ) -> Self {
        let remaining_attempts = MAX_IMPLEMENTATION_ATTEMPTS.saturating_sub(attempt);
        let failure_category = failure_message
            .map(|value| classify_failure_category(value).to_string())
            .unwrap_or_else(|| "stage_failure".to_string());
        let failure_message = failure_message.map(redact_sensitive);
        let artifact_references = stage_artifacts
            .iter()
            .map(|artifact| RetryArtifactReference {
                artifact_type: artifact.artifact_type.as_str().to_string(),
                artifact_id: artifact.id,
                stage_history_id: artifact.stage_history_id,
                content_hash: artifact.content_hash.clone(),
                created_at: artifact.created_at.to_rfc3339(),
            })
            .collect();

        Self {
            stage: stage.as_str().to_string(),
            attempt,
            remaining_attempts,
            failure_category,
            failure_message,
            artifact_references,
        }
    }
}

#[derive(Serialize)]
struct RetryArtifactReference {
    artifact_type: String,
    artifact_id: i64,
    stage_history_id: i64,
    content_hash: Option<String>,
    created_at: String,
}

#[cfg(test)]
mod retry_packet_tests {
    use super::{should_persist_retry_packet, RetryPacket, MAX_IMPLEMENTATION_ATTEMPTS};
    use chrono::Utc;
    use swarm::{ArtifactType, RuntimeStageTransition as StageTransitionKey, Stage, StageArtifact};

    fn sample_stage_artifacts() -> Vec<StageArtifact> {
        let now = Utc::now();
        vec![
            StageArtifact {
                id: 1,
                stage_history_id: 10,
                artifact_type: ArtifactType::FailureDetails,
                content: "failure".to_string(),
                metadata: None,
                created_at: now,
                content_hash: Some("hash-1".to_string()),
            },
            StageArtifact {
                id: 2,
                stage_history_id: 10,
                artifact_type: ArtifactType::TestResults,
                content: "tests".to_string(),
                metadata: None,
                created_at: now,
                content_hash: None,
            },
        ]
    }

    #[test]
    fn retry_packet_is_only_persisted_for_retryable_stages() {
        assert!(should_persist_retry_packet(
            StageTransitionKey::Retry,
            Stage::QaEnforcer
        ));
        assert!(should_persist_retry_packet(
            StageTransitionKey::Retry,
            Stage::RedQueen
        ));
        assert!(!should_persist_retry_packet(
            StageTransitionKey::Retry,
            Stage::Implement
        ));
        assert!(!should_persist_retry_packet(
            StageTransitionKey::Retry,
            Stage::Done
        ));
    }

    #[test]
    fn retry_packet_captures_metadata_and_sanitizes_message() {
        let artifacts = sample_stage_artifacts();
        let packet = RetryPacket::new(
            Stage::QaEnforcer,
            2,
            Some("syntax failure password=secret"),
            &artifacts,
        );

        assert_eq!(packet.remaining_attempts, MAX_IMPLEMENTATION_ATTEMPTS - 2);
        assert_eq!(packet.failure_category, "compile_error");
        assert_eq!(
            packet.failure_message.unwrap(),
            "syntax failure password=<redacted>"
        );
        assert_eq!(packet.artifact_references.len(), artifacts.len());
        let reference = &packet.artifact_references[0];
        assert_eq!(
            reference.artifact_type,
            ArtifactType::FailureDetails.as_str()
        );
        assert_eq!(reference.stage_history_id, artifacts[0].stage_history_id);
        assert_eq!(reference.artifact_id, artifacts[0].id);
        assert_eq!(
            reference.content_hash.as_deref(),
            artifacts[0].content_hash.as_deref()
        );
    }

    #[test]
    fn retry_packet_defaults_missing_message_to_stage_failure() {
        let artifacts = sample_stage_artifacts();
        let packet = RetryPacket::new(Stage::RedQueen, 1, None, &artifacts);

        assert_eq!(packet.failure_category, "stage_failure");
        assert!(packet.failure_message.is_none());
    }
}
