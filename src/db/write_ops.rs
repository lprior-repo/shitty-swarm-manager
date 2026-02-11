use crate::db::SwarmDb;
use crate::ddd::{
    runtime_determine_transition_decision, validate_completion_implies_push_confirmed,
    RuntimeStage, RuntimeStageResult, RuntimeStageTransition,
};
use crate::error::{Result, SwarmError};
use crate::types::{
    AgentId, ArtifactType, BeadId, EventSchemaVersion, MessageType, RepoId, Stage, StageResult,
    SwarmStatus,
};
use crate::BrSyncStatus;
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::Acquire;
use std::collections::HashSet;
use std::convert::TryFrom;
use tracing::{debug, info};

const CONTEXT_ARTIFACT_TYPES: [ArtifactType; 3] = [
    ArtifactType::ImplementationCode,
    ArtifactType::TestResults,
    ArtifactType::TestOutput,
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum StageTransition {
    Finalize,
    Advance(Stage),
    RetryImplement,
    NoOp,
}

struct StageTransitionInput<'a> {
    transition: &'a StageTransition,
    agent_id: &'a AgentId,
    bead_id: &'a BeadId,
    stage: Stage,
    stage_history_id: Option<i64>,
    attempt: u32,
    message: Option<&'a str>,
}

impl SwarmDb {
    /// Records a command execution audit entry.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn record_command_audit(
        &self,
        cmd: &str,
        rid: Option<&str>,
        args: serde_json::Value,
        ok: bool,
        ms: u64,
        error_code: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO command_audit (cmd, rid, args, ok, ms, error_code)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(cmd)
        .bind(rid)
        .bind(args)
        .bind(ok)
        .bind(ms.cast_signed())
        .bind(error_code)
        .execute(self.pool())
        .await
        .map(|_result| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to write command audit: {e}")))
    }

    /// Acquires a resource lock for an agent.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn acquire_resource_lock(
        &self,
        resource: &str,
        agent: &str,
        ttl_ms: i64,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}")))?;

        sqlx::query("DELETE FROM resource_locks WHERE until_at <= NOW()")
            .execute(&mut *conn)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to cleanup locks: {e}")))?;

        let acquired = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "INSERT INTO resource_locks (resource, agent, until_at)
             VALUES ($1, $2, NOW() + ($3 * INTERVAL '1 millisecond'))
             ON CONFLICT (resource) DO NOTHING
             RETURNING until_at",
        )
        .bind(resource)
        .bind(agent)
        .bind(ttl_ms)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire lock: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))
            .map(|()| acquired)
    }

    /// Releases a resource lock held by an agent.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn unlock_resource(&self, resource: &str, agent: &str) -> Result<bool> {
        sqlx::query("DELETE FROM resource_locks WHERE resource = $1 AND agent = $2")
            .bind(resource)
            .bind(agent)
            .execute(self.pool())
            .await
            .map(|result| result.rows_affected() > 0)
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to unlock resource: {e}")))
    }

    /// Writes a broadcast message from an agent.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn write_broadcast(&self, from_agent: &str, msg: &str) -> Result<i64> {
        sqlx::query("DELETE FROM resource_locks WHERE until_at <= NOW()")
            .execute(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to cleanup locks: {e}")))?;

        sqlx::query("INSERT INTO broadcast_log (from_agent, msg) VALUES ($1, $2)")
            .bind(from_agent)
            .bind(msg)
            .execute(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to write broadcast: {e}")))?;

        sqlx::query_scalar::<_, i64>("SELECT COUNT(DISTINCT agent) FROM resource_locks")
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to count agents: {e}")))
    }

    /// Registers a repository with the swarm.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn register_repo(&self, repo_id: &RepoId, name: &str, path: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO repos (repo_id, name, path) VALUES ($1, $2, $3)
             ON CONFLICT (repo_id) DO NOTHING",
        )
        .bind(repo_id.value())
        .bind(name)
        .bind(path)
        .execute(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to register repo: {e}")))
        .map(|_result| ())
    }

    /// Registers an agent with the swarm.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn register_agent(&self, agent_id: &AgentId) -> Result<bool> {
        let repo_scoped = self.table_has_column("agent_state", "repo_id").await?;

        if repo_scoped {
            self.register_repo(
                agent_id.repo_id(),
                agent_id.repo_id().value(),
                agent_id.repo_id().value(),
            )
            .await?;

            sqlx::query(
                "INSERT INTO agent_state (repo_id, agent_id, status) VALUES ($1, $2, 'idle')
                 ON CONFLICT (repo_id, agent_id) DO NOTHING",
            )
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .execute(self.pool())
            .await
            .map(|rows| rows.rows_affected() > 0)
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to register agent: {e}")))
        } else {
            sqlx::query(
                "INSERT INTO agent_state (agent_id, status) VALUES ($1, 'idle')
                 ON CONFLICT (agent_id) DO NOTHING",
            )
            .bind(agent_id.number().cast_signed())
            .execute(self.pool())
            .await
            .map(|rows| rows.rows_affected() > 0)
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to register agent: {e}")))
        }
    }

    /// Attempts to claim a specific bead for an agent.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn claim_bead(&self, agent_id: &AgentId, bead_id: &BeadId) -> Result<bool> {
        self.claim_next_bead(agent_id)
            .await
            .map(|claimed| claimed.as_ref().map(BeadId::value) == Some(bead_id.value()))
    }

    /// Refreshes lease ownership for an active bead claim.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn heartbeat_claim(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        lease_extension_ms: i32,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>("SELECT heartbeat_bead_claim($1, $2, $3, $4)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .bind(bead_id.value())
            .bind(lease_extension_ms)
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to heartbeat bead claim: {e}")))
    }

    /// Reclaims any expired in-progress claims back into pending backlog.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn recover_expired_claims(&self) -> Result<u32> {
        sqlx::query_scalar::<_, i32>("SELECT recover_expired_bead_claims()")
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to recover expired claims: {e}"))
            })
            .map(i32::cast_unsigned)
    }

    /// Records the start of a stage execution.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn record_stage_started(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
    ) -> Result<i64> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}")))?;

        let stage_history_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO stage_history (agent_id, bead_id, stage, attempt_number, status)
             VALUES ($1, $2, $3, $4, 'started')
             RETURNING id",
        )
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt.cast_signed())
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to record stage start: {e}")))?;

        sqlx::query(
            "UPDATE agent_state
             SET current_stage = $3, stage_started_at = NOW(), status = 'working'
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(stage.as_str())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update stage start: {e}")))?;

        sqlx::query(
            "INSERT INTO execution_events (
                schema_version,
                event_type,
                entity_id,
                bead_id,
                agent_id,
                stage,
                causation_id,
                payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(EventSchemaVersion::V1.as_i32())
        .bind("stage_started")
        .bind(event_entity_id(bead_id, agent_id.repo_id()))
        .bind(bead_id.value())
        .bind(agent_id.number().cast_signed())
        .bind(stage.as_str())
        .bind(Some(format!("stage-history:{stage_history_id}")))
        .bind(json!({"attempt": attempt, "status": "started"}))
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to write stage start event: {e}"))
        })?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))
            .map(|()| stage_history_id)
    }

    /// Records the completion of a stage execution.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn record_stage_complete(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
        result: StageResult,
        duration_ms: u64,
    ) -> Result<()> {
        let message = result.message();
        let stage_history_id = self
            .record_stage_complete_without_transition(
                agent_id,
                bead_id,
                stage,
                attempt,
                &result,
                duration_ms,
            )
            .await?;

        self.apply_stage_transition(StageTransitionInput {
            transition: &determine_transition(stage, &result),
            agent_id,
            bead_id,
            stage,
            stage_history_id: Some(stage_history_id),
            attempt,
            message,
        })
        .await?;

        debug!(
            "Agent {} completed stage {} for bead {}: {:?}",
            agent_id, stage, bead_id, result
        );
        Ok(())
    }

    /// Records stage completion state without applying transition side effects.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn record_stage_complete_without_transition(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
        result: &StageResult,
        duration_ms: u64,
    ) -> Result<i64> {
        let status = result.as_str();
        let message = result.message();

        let duration_value = i32::try_from(duration_ms).map_err(|_| {
            SwarmError::DatabaseError("Duration overflow updating stage history".to_string())
        })?;

        let stage_history_row = sqlx::query!(
            "UPDATE stage_history
             SET status = $5, result = $6, feedback = $7, completed_at = NOW(), duration_ms = $8
             WHERE id = (
                SELECT id FROM stage_history
                WHERE agent_id = $1 AND bead_id = $2 AND stage = $3 AND attempt_number = $4 AND status = 'started'
                ORDER BY started_at DESC LIMIT 1
             )
             RETURNING id, completed_at",
            agent_id.number().cast_signed(),
            bead_id.value(),
            stage.as_str(),
            attempt.cast_signed(),
            &status,
            message,
            message,
            duration_value
        )
        .fetch_optional(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update stage history: {e}")))?
        .ok_or_else(|| {
            SwarmError::DatabaseError(
                "Failed to locate active stage history row for completion update".to_string(),
            )
        })?;

        let completed_at = stage_history_row.completed_at.ok_or_else(|| {
            SwarmError::DatabaseError("Failed to capture stage completion timestamp".to_string())
        })?;

        let stage_history_id = stage_history_row.id;

        self.persist_stage_transcript(
            agent_id,
            stage_history_id,
            stage,
            attempt,
            result,
            completed_at,
        )
        .await?;

        self.record_execution_event(
            bead_id,
            agent_id,
            ExecutionEventWriteInput {
                stage: Some(stage),
                event_type: "stage_completed",
                causation_id: Some(format!("stage-history:{stage_history_id}")),
                payload: json!({
                    "attempt": attempt,
                    "status": status,
                    "result": message,
                    "duration_ms": duration_ms,
                }),
                diagnostics: None,
            },
        )
        .await
        .map(|()| stage_history_id)
    }

    async fn persist_stage_transcript(
        &self,
        agent_id: &AgentId,
        stage_history_id: i64,
        stage: Stage,
        attempt: u32,
        result: &StageResult,
        completed_at: DateTime<Utc>,
    ) -> Result<()> {
        let artifacts = self
            .get_stage_artifacts(agent_id.repo_id(), stage_history_id)
            .await?;
        let mut sorted_artifacts = artifacts.clone();
        sorted_artifacts.sort_by(|a, b| {
            a.artifact_type
                .as_str()
                .cmp(b.artifact_type.as_str())
                .then_with(|| a.id.cmp(&b.id))
        });

        let artifact_types = sorted_artifacts
            .iter()
            .map(|artifact| artifact.artifact_type.as_str().to_string())
            .collect::<Vec<_>>();

        let artifact_refs = sorted_artifacts
            .iter()
            .map(|artifact| {
                json!({
                    "id": artifact.id,
                    "artifact_type": artifact.artifact_type.as_str(),
                    "content_hash": artifact.content_hash,
                    "created_at": artifact.created_at.to_rfc3339(),
                })
            })
            .collect::<Vec<_>>();

        let message = result
            .message()
            .map_or_else(String::new, ToString::to_string);

        let metadata = json!({
            "stage_history_id": stage_history_id,
            "stage": stage.as_str(),
            "attempt": attempt,
            "status": result.as_str(),
            "artifact_count": sorted_artifacts.len(),
            "artifact_types": artifact_types,
            "completed_at": completed_at.to_rfc3339(),
        });

        let transcript_body = json!({
            "stage": stage.as_str(),
            "attempt": attempt,
            "status": result.as_str(),
            "message": message,
            "artifacts": artifact_refs,
            "metadata": metadata.clone(),
        });

        let transcript_text = serde_json::to_string(&transcript_body).map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to serialize stage transcript: {e}"))
        })?;

        sqlx::query(
            "UPDATE stage_history\n             SET transcript = $1\n             WHERE id = $2 AND transcript IS DISTINCT FROM $1",
        )
        .bind(&transcript_text)
        .bind(stage_history_id)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update stage transcript: {e}")))?;

        self.store_stage_artifact(
            stage_history_id,
            ArtifactType::StageLog,
            &transcript_text,
            Some(metadata),
        )
        .await
        .map(|_| ())?;

        Ok(())
    }

    /// Stores a stage artifact to the database.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn store_stage_artifact(
        &self,
        stage_history_id: i64,
        artifact_type: ArtifactType,
        content: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<i64> {
        sqlx::query_scalar::<_, i64>("SELECT store_stage_artifact($1, $2, $3, $4)")
            .bind(stage_history_id)
            .bind(artifact_type.as_str())
            .bind(content)
            .bind(metadata)
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to store stage artifact: {e}")))
    }

    /// Sends a message from one agent to another.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn send_agent_message(
        &self,
        from_agent: &AgentId,
        to_agent: Option<&AgentId>,
        bead_id: Option<&BeadId>,
        message_type: MessageType,
        content: (&str, &str),
        metadata: Option<serde_json::Value>,
    ) -> Result<i64> {
        let (subject, body) = content;
        let to_repo_id = to_agent.map(|agent| agent.repo_id().value().to_string());
        let to_agent_id = to_agent.map(|agent| agent.number().cast_signed());

        sqlx::query_scalar::<_, i64>(
            "SELECT send_agent_message($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(from_agent.repo_id().value())
        .bind(from_agent.number().cast_signed())
        .bind(to_repo_id)
        .bind(to_agent_id)
        .bind(bead_id.map(BeadId::value))
        .bind(message_type.as_str())
        .bind(subject)
        .bind(body)
        .bind(metadata)
        .fetch_one(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to send agent message: {e}")))
    }

    /// Marks messages as read for an agent.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn mark_messages_read(&self, agent_id: &AgentId, message_ids: &[i64]) -> Result<()> {
        if message_ids.is_empty() {
            return Ok(());
        }

        sqlx::query("SELECT mark_messages_read($1, $2, $3)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .bind(message_ids)
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to mark messages read: {e}")))
    }

    async fn apply_stage_transition(&self, input: StageTransitionInput<'_>) -> Result<()> {
        match input.transition {
            StageTransition::Finalize => {
                self.finalize_agent_and_bead(input.agent_id, input.bead_id)
                    .await?;
                self.record_execution_event(
                    input.bead_id,
                    input.agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(input.stage),
                        event_type: "transition_finalize",
                        causation_id: input
                            .stage_history_id
                            .map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "finalize"}),
                        diagnostics: None,
                    },
                )
                .await
            }
            StageTransition::Advance(next_stage) => {
                self.advance_to_stage(input.agent_id, *next_stage).await?;
                self.record_execution_event(
                    input.bead_id,
                    input.agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(input.stage),
                        event_type: "transition_advance",
                        causation_id: input
                            .stage_history_id
                            .map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "advance", "next_stage": next_stage.as_str()}),
                        diagnostics: None,
                    },
                )
                .await
            }
            StageTransition::RetryImplement => {
                self.persist_retry_packet(
                    input.stage_history_id,
                    input.stage,
                    input.attempt,
                    input.bead_id,
                    input.agent_id,
                    input.message,
                )
                .await?;
                self.apply_failure_transition(input.agent_id, input.message)
                    .await?;
                self.record_execution_event(
                    input.bead_id,
                    input.agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(input.stage),
                        event_type: "transition_retry",
                        causation_id: input
                            .stage_history_id
                            .map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "retry", "next_stage": Stage::Implement.as_str()}),
                        diagnostics: Some(build_failure_diagnostics(input.message)),
                    },
                )
                .await
            }
            StageTransition::NoOp => {
                self.record_execution_event(
                    input.bead_id,
                    input.agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(input.stage),
                        event_type: "transition_noop",
                        causation_id: input
                            .stage_history_id
                            .map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "noop"}),
                        diagnostics: None,
                    },
                )
                .await
            }
        }
    }

    /// Persists retry diagnostics and artifact references after a retry transition.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` when reading or storing retry packet artifacts fails.
    pub async fn persist_retry_packet(
        &self,
        stage_history_id: Option<i64>,
        stage: Stage,
        attempt: u32,
        bead_id: &BeadId,
        agent_id: &AgentId,
        message: Option<&str>,
    ) -> Result<()> {
        let Some(stage_history_id) = stage_history_id else {
            return Ok(());
        };

        let config = self.get_config(agent_id.repo_id()).await?;
        let max_attempts = config.max_implementation_attempts;
        let remaining_attempts = max_attempts.saturating_sub(attempt);

        let FailureDiagnosticsPayload {
            category: failure_category,
            retryable,
            next_command,
            detail: failure_detail,
        } = build_failure_diagnostics(message);

        let mut artifact_refs = Vec::new();
        let mut seen_ids = HashSet::new();
        let mut seen_types = HashSet::new();

        let stage_artifacts = self
            .get_stage_artifacts(agent_id.repo_id(), stage_history_id)
            .await?;
        for artifact in stage_artifacts {
            let artifact_type_name = artifact.artifact_type.as_str().to_string();
            if seen_ids.insert(artifact.id) {
                seen_types.insert(artifact_type_name.clone());
                artifact_refs.push(json!({
                    "artifact_id": artifact.id,
                    "artifact_type": artifact.artifact_type.as_str(),
                    "content_hash": artifact.content_hash,
                    "metadata": artifact.metadata,
                    "created_at": artifact.created_at.to_rfc3339(),
                    "stage_history_id": artifact.stage_history_id,
                    "context": "current_stage",
                }));
            }
        }

        for artifact_type in CONTEXT_ARTIFACT_TYPES {
            let artifact_type_name = artifact_type.as_str().to_string();
            if seen_types.contains(&artifact_type_name) {
                continue;
            }

            let artifacts = self
                .get_bead_artifacts_by_type(agent_id.repo_id(), bead_id, artifact_type)
                .await?;
            if let Some(artifact) = artifacts.last() {
                if seen_ids.insert(artifact.id) {
                    seen_types.insert(artifact_type_name.clone());
                    artifact_refs.push(json!({
                        "artifact_id": artifact.id,
                        "artifact_type": artifact.artifact_type.as_str(),
                        "content_hash": artifact.content_hash,
                        "metadata": artifact.metadata,
                        "created_at": artifact.created_at.to_rfc3339(),
                        "stage_history_id": artifact.stage_history_id,
                        "context": "latest_per_type",
                    }));
                    continue;
                }
            }

            seen_types.insert(artifact_type_name.clone());
            artifact_refs.push(json!({
                "artifact_type": artifact_type.as_str(),
                "missing": true,
                "context": "latest_per_type",
            }));
        }

        let retry_packet = json!({
            "bead_id": bead_id.value(),
            "agent_id": agent_id.number(),
            "stage": stage.as_str(),
            "stage_history_id": stage_history_id,
            "attempt": attempt,
            "max_attempts": max_attempts,
            "remaining_attempts": remaining_attempts,
            "failure_category": failure_category,
            "failure_detail": failure_detail,
            "failure_message": message.map(redact_sensitive),
            "retryable": retryable,
            "next_command": next_command,
            "artifact_refs": artifact_refs,
            "created_at": Utc::now().to_rfc3339(),
        });

        self.store_stage_artifact(
            stage_history_id,
            ArtifactType::RetryPacket,
            &retry_packet.to_string(),
            Some(json!({
                "stage": stage.as_str(),
                "attempt": attempt,
                "failure_category": failure_category,
            })),
        )
        .await
        .map(|_| ())
    }

    /// Sets the swarm status.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn set_swarm_status(&self, repo_id: &RepoId, status: SwarmStatus) -> Result<()> {
        let repo_scoped = self.table_has_column("swarm_config", "repo_id").await?;

        if repo_scoped {
            sqlx::query(
                "INSERT INTO swarm_config (repo_id, swarm_status)
                 VALUES ($1, $2)
                 ON CONFLICT (repo_id) DO UPDATE
                 SET swarm_status = EXCLUDED.swarm_status",
            )
            .bind(repo_id.value())
            .bind(status.as_str())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to update status: {e}")))
        } else {
            sqlx::query("UPDATE swarm_config SET swarm_status = $1 WHERE id = TRUE")
                .bind(status.as_str())
                .execute(self.pool())
                .await
                .map(|_result| ())
                .map_err(|e| SwarmError::DatabaseError(format!("Failed to update status: {e}")))
        }
    }

    /// Updates the swarm configuration.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn update_config(&self, max_agents: u32) -> Result<()> {
        sqlx::query("UPDATE swarm_config SET max_agents = $1")
            .bind(max_agents.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to update config: {e}")))
    }

    /// Starts the swarm.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn start_swarm(&self, repo_id: &RepoId) -> Result<()> {
        let repo_scoped = self.table_has_column("swarm_config", "repo_id").await?;

        if repo_scoped {
            sqlx::query(
                "INSERT INTO swarm_config (repo_id, swarm_status, swarm_started_at)
                 VALUES ($1, 'running', NOW())
                 ON CONFLICT (repo_id) DO UPDATE
                 SET swarm_status = 'running',
                     swarm_started_at = NOW()",
            )
            .bind(repo_id.value())
            .execute(self.pool())
            .await
            .map(|_| info!("Started swarm"))
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to start swarm: {e}")))
        } else {
            sqlx::query(
                "UPDATE swarm_config SET swarm_status = 'running', swarm_started_at = NOW() WHERE id = TRUE",
            )
            .execute(self.pool())
            .await
            .map(|_| info!("Started swarm"))
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to start swarm: {e}")))
        }
    }

    /// Initializes the database schema from SQL.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn initialize_schema_from_sql(&self, schema_sql: &str) -> Result<()> {
        sqlx::raw_sql(schema_sql)
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to initialize schema: {e}")))
    }

    /// Seeds idle agents into the database.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn seed_idle_agents(&self, count: u32) -> Result<()> {
        let repo_scoped = self.table_has_column("agent_state", "repo_id").await?;
        let default_repo = RepoId::new("local");

        if repo_scoped {
            self.register_repo(&default_repo, default_repo.value(), default_repo.value())
                .await?;
        }
        self.prune_idle_unassigned_agents(repo_scoped, &default_repo, count)
            .await?;

        let existing_agent_ids = self
            .load_existing_agent_ids(repo_scoped, &default_repo)
            .await?;

        let mut occupied_ids = existing_agent_ids
            .into_iter()
            .map(i32::cast_unsigned)
            .collect::<HashSet<_>>();

        let idle_unassigned_count = self
            .count_idle_unassigned_agents(repo_scoped, &default_repo)
            .await?;

        let target_count = i64::from(count);
        if idle_unassigned_count >= target_count {
            return Ok(());
        }

        let mut next_candidate = 1_u32;
        let agents_to_add = target_count - idle_unassigned_count;
        for _ in 0..agents_to_add {
            while occupied_ids.contains(&next_candidate) {
                next_candidate = next_candidate.saturating_add(1);
            }

            self.insert_idle_agent(repo_scoped, &default_repo, next_candidate)
                .await?;

            occupied_ids.insert(next_candidate);
            next_candidate = next_candidate.saturating_add(1);
        }

        Ok(())
    }

    async fn prune_idle_unassigned_agents(
        &self,
        repo_scoped: bool,
        default_repo: &RepoId,
        count: u32,
    ) -> Result<()> {
        if repo_scoped {
            sqlx::query(
                "DELETE FROM agent_state
                 WHERE repo_id = $1
                   AND status = 'idle'
                   AND bead_id IS NULL
                   AND agent_id IN (
                     SELECT agent_id
                     FROM agent_state
                     WHERE repo_id = $1 AND status = 'idle' AND bead_id IS NULL
                     ORDER BY agent_id DESC
                     OFFSET $2
                   )",
            )
            .bind(default_repo.value())
            .bind(count.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to prune idle agents: {e}")))
        } else {
            sqlx::query(
                "DELETE FROM agent_state
                 WHERE status = 'idle'
                   AND bead_id IS NULL
                   AND agent_id IN (
                     SELECT agent_id
                     FROM agent_state
                     WHERE status = 'idle' AND bead_id IS NULL
                     ORDER BY agent_id DESC
                     OFFSET $1
                   )",
            )
            .bind(count.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to prune idle agents: {e}")))
        }
    }

    async fn load_existing_agent_ids(
        &self,
        repo_scoped: bool,
        default_repo: &RepoId,
    ) -> Result<Vec<i32>> {
        if repo_scoped {
            sqlx::query_scalar::<_, i32>(
                "SELECT agent_id
                 FROM agent_state
                 WHERE repo_id = $1
                 ORDER BY agent_id ASC",
            )
            .bind(default_repo.value())
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to load existing seeded agents: {e}"))
            })
        } else {
            sqlx::query_scalar::<_, i32>("SELECT agent_id FROM agent_state ORDER BY agent_id ASC")
                .fetch_all(self.pool())
                .await
                .map_err(|e| {
                    SwarmError::DatabaseError(format!("Failed to load existing seeded agents: {e}"))
                })
        }
    }

    async fn count_idle_unassigned_agents(
        &self,
        repo_scoped: bool,
        default_repo: &RepoId,
    ) -> Result<i64> {
        if repo_scoped {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)
                 FROM agent_state
                 WHERE repo_id = $1 AND status = 'idle' AND bead_id IS NULL",
            )
            .bind(default_repo.value())
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to count idle unassigned agents: {e}"))
            })
        } else {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_state WHERE status = 'idle' AND bead_id IS NULL",
            )
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to count idle unassigned agents: {e}"))
            })
        }
    }

    async fn insert_idle_agent(
        &self,
        repo_scoped: bool,
        default_repo: &RepoId,
        agent_number: u32,
    ) -> Result<()> {
        if repo_scoped {
            sqlx::query(
                "INSERT INTO agent_state (repo_id, agent_id, status)
                 VALUES ($1, $2, 'idle')
                 ON CONFLICT (agent_id) DO NOTHING",
            )
            .bind(default_repo.value())
            .bind(agent_number.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to seed agent {agent_number}: {e}"))
            })
        } else {
            sqlx::query(
                "INSERT INTO agent_state (agent_id, status)
                 VALUES ($1, 'idle')
                 ON CONFLICT (agent_id) DO NOTHING",
            )
            .bind(agent_number.cast_signed())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to seed agent {agent_number}: {e}"))
            })
        }
    }

    /// Enqueues a batch of beads into the backlog.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn enqueue_backlog_batch(&self, prefix: &str, count: u32) -> Result<()> {
        sqlx::query(
            "INSERT INTO bead_backlog (bead_id, priority, status)
             SELECT format('%s-%s', $1, g), 'p0', 'pending'
             FROM generate_series(1, $2) AS g",
        )
        .bind(prefix)
        .bind(count.cast_signed())
        .execute(self.pool())
        .await
        .map(|_result| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to enqueue backlog batch: {e}")))
    }

    /// Marks a bead as blocked.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn mark_bead_blocked(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        reason: &str,
    ) -> Result<()> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}")))?;

        let claim_update = sqlx::query(
            "UPDATE bead_claims
             SET status = 'blocked'
             WHERE repo_id = $1
               AND bead_id = $2
               AND claimed_by = $3
               AND status = 'in_progress'",
        )
        .bind(agent_id.repo_id().value())
        .bind(bead_id.value())
        .bind(agent_id.number().cast_signed())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to block claim: {e}")))?;

        if claim_update.rows_affected() != 1 {
            return Err(SwarmError::AgentError(format!(
                "Agent {} does not own active claim for bead {}",
                agent_id.number(),
                bead_id.value()
            )));
        }

        sqlx::query(
            "UPDATE bead_backlog SET status = 'blocked' WHERE repo_id = $1 AND bead_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(bead_id.value())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to block backlog bead: {e}")))?;

        sqlx::query(
            "UPDATE agent_state
             SET status = 'error', feedback = $3
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(reason)
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to mark agent error: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))?;

        self.record_execution_event(
            bead_id,
            agent_id,
            ExecutionEventWriteInput {
                stage: None,
                event_type: "transition_blocked",
                causation_id: None,
                payload: json!({"transition": "blocked"}),
                diagnostics: Some(FailureDiagnosticsPayload {
                    category: "max_attempts_exhausted".to_string(),
                    retryable: false,
                    next_command: "swarm monitor --view failures".to_string(),
                    detail: Some(redact_sensitive(reason)),
                }),
            },
        )
        .await
    }

    /// Releases an agent from its current bead.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn release_agent(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}")))?;

        let bead = sqlx::query_scalar::<_, Option<String>>(
            "SELECT bead_id
             FROM agent_state
             WHERE repo_id = $1 AND agent_id = $2
             FOR UPDATE",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to read agent state: {e}")))?
        .flatten();

        if let Some(bead_id) = bead.as_deref() {
            sqlx::query(
                "UPDATE agent_state
                 SET bead_id = NULL,
                     current_stage = NULL,
                     stage_started_at = NULL,
                     status = 'idle',
                     feedback = NULL,
                     implementation_attempt = 0
                 WHERE repo_id = $1 AND agent_id = $2",
            )
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .execute(&mut *conn)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to reset agent state: {e}")))?;

            sqlx::query("DELETE FROM agent_messages WHERE bead_id = $1")
                .bind(bead_id)
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    SwarmError::DatabaseError(format!(
                        "Failed to clear bead messages on release: {e}"
                    ))
                })?;

            sqlx::query("DELETE FROM bead_claims WHERE repo_id = $1 AND bead_id = $2")
                .bind(agent_id.repo_id().value())
                .bind(bead_id)
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    SwarmError::DatabaseError(format!("Failed to clear bead claim on release: {e}"))
                })?;

            sqlx::query(
                "UPDATE bead_backlog
                 SET status = 'pending'
                 WHERE repo_id = $1
                   AND bead_id = $2
                   AND status <> 'completed'",
            )
            .bind(agent_id.repo_id().value())
            .bind(bead_id)
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to reset backlog status on release: {e}"))
            })?;
        }

        if bead.is_none() {
            sqlx::query(
                "UPDATE agent_state
                 SET bead_id = NULL,
                     current_stage = NULL,
                     stage_started_at = NULL,
                     status = 'idle',
                     feedback = NULL,
                     implementation_attempt = 0
                 WHERE repo_id = $1 AND agent_id = $2",
            )
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .execute(&mut *conn)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to reset agent state: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))?;

        Ok(bead.map(BeadId::new))
    }

    /// Finalizes agent and bead only after landing push confirmation.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn finalize_after_push_confirmation(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        push_confirmed: bool,
    ) -> Result<()> {
        validate_completion_implies_push_confirmed(
            RuntimeStageTransition::Complete,
            push_confirmed,
        )
        .map_err(|err| SwarmError::AgentError(err.to_string()))?;
        self.finalize_agent_and_bead(agent_id, bead_id).await?;
        self.record_landing_sync_outcome_if_absent(
            bead_id,
            agent_id,
            BrSyncStatus::Synchronized,
            None,
        )
        .await
    }

    /// Marks landing as retryable without completing the bead claim.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn mark_landing_retryable(&self, agent_id: &AgentId, reason: &str) -> Result<()> {
        sqlx::query(
            "UPDATE agent_state
             SET status = 'waiting', feedback = $3, current_stage = 'red-queen'
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(reason)
        .execute(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to mark landing retryable: {e}")))
        .map(|_| ())?;

        let bead_id = self.lookup_agent_bead(agent_id).await?;
        if let Some(bead_id) = bead_id {
            let causation_id = Some(landing_retry_causation_id(reason));
            self.record_execution_event_if_absent(
                &bead_id,
                agent_id,
                ExecutionEventWriteInput {
                    stage: Some(Stage::RedQueen),
                    event_type: "transition_retry",
                    causation_id,
                    payload: json!({"transition": "retry", "next_stage": Stage::RedQueen.as_str()}),
                    diagnostics: Some(FailureDiagnosticsPayload {
                        category: "landing_failure".to_string(),
                        retryable: true,
                        next_command: "swarm monitor --view failures".to_string(),
                        detail: Some(redact_sensitive(reason)),
                    }),
                },
            )
            .await?;
            self.record_landing_sync_outcome_if_absent(
                &bead_id,
                agent_id,
                BrSyncStatus::RetryScheduled,
                Some(reason),
            )
            .await?;
        }

        Ok(())
    }

    async fn finalize_agent_and_bead(&self, agent_id: &AgentId, bead_id: &BeadId) -> Result<()> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {e}")))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {e}")))?;

        let claim_update = sqlx::query(
            "UPDATE bead_claims
             SET status = 'completed'
             WHERE bead_id = $1
               AND claimed_by = $2
               AND status = 'in_progress'",
        )
        .bind(bead_id.value())
        .bind(agent_id.number().cast_signed())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to finalize bead: {e}")))?;

        if claim_update.rows_affected() != 1 {
            let existing_status = sqlx::query_scalar::<_, String>(
                "SELECT status
                 FROM bead_claims
                 WHERE bead_id = $1 AND claimed_by = $2
                 ORDER BY claimed_at DESC
                 LIMIT 1",
            )
            .bind(bead_id.value())
            .bind(agent_id.number().cast_signed())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!(
                    "Failed to read existing claim while finalizing bead: {e}"
                ))
            })?;

            if existing_status.as_deref() == Some("completed") {
                tx.commit()
                    .await
                    .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))?;
                return Ok(());
            }

            return Err(SwarmError::AgentError(format!(
                "Agent {} does not own active claim for bead {}",
                agent_id.number(),
                bead_id.value()
            )));
        }

        sqlx::query(
            "UPDATE agent_state
             SET status = 'done', current_stage = 'done'
             WHERE repo_id = $1 AND agent_id = $2 AND bead_id = $3",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.value())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to finalize agent: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {e}")))
    }

    async fn advance_to_stage(&self, agent_id: &AgentId, next_stage: Stage) -> Result<()> {
        sqlx::query(
            "UPDATE agent_state
             SET current_stage = $3, stage_started_at = NOW(), status = 'working'
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(next_stage.as_str())
        .execute(self.pool())
        .await
        .map(|_result| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to advance stage: {e}")))
    }

    async fn lookup_agent_bead(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        let repo_scoped = self.table_has_column("agent_state", "repo_id").await?;

        if repo_scoped {
            sqlx::query_scalar::<_, String>(
                "SELECT bead_id FROM agent_state WHERE repo_id = $1 AND agent_id = $2",
            )
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .fetch_optional(self.pool())
            .await
            .map(|bead| bead.map(BeadId::new))
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to lookup agent bead: {e}")))
        } else {
            sqlx::query_scalar::<_, String>("SELECT bead_id FROM agent_state WHERE agent_id = $1")
                .bind(agent_id.number().cast_signed())
                .fetch_optional(self.pool())
                .await
                .map(|bead| bead.map(BeadId::new))
                .map_err(|e| SwarmError::DatabaseError(format!("Failed to lookup agent bead: {e}")))
        }
    }

    async fn record_execution_event(
        &self,
        bead_id: &BeadId,
        agent_id: &AgentId,
        input: ExecutionEventWriteInput,
    ) -> Result<()> {
        let diagnostics_category = input
            .diagnostics
            .as_ref()
            .map(|value| value.category.as_str());
        let diagnostics_retryable = input.diagnostics.as_ref().map(|value| value.retryable);
        let diagnostics_next_command = input
            .diagnostics
            .as_ref()
            .map(|value| value.next_command.as_str());
        let diagnostics_detail = input
            .diagnostics
            .as_ref()
            .and_then(|value| value.detail.clone());

        sqlx::query(
            "INSERT INTO execution_events (
                schema_version,
                event_type,
                entity_id,
                bead_id,
                agent_id,
                stage,
                causation_id,
                diagnostics_category,
                diagnostics_retryable,
                diagnostics_next_command,
                diagnostics_detail,
                payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(EventSchemaVersion::V1.as_i32())
        .bind(input.event_type)
        .bind(event_entity_id(bead_id, agent_id.repo_id()))
        .bind(bead_id.value())
        .bind(agent_id.number().cast_signed())
        .bind(input.stage.map(|value| value.as_str()))
        .bind(input.causation_id)
        .bind(diagnostics_category)
        .bind(diagnostics_retryable)
        .bind(diagnostics_next_command)
        .bind(diagnostics_detail)
        .bind(input.payload)
        .execute(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to write execution event: {e}")))
        .map(|_| ())
    }

    async fn record_execution_event_if_absent(
        &self,
        bead_id: &BeadId,
        agent_id: &AgentId,
        input: ExecutionEventWriteInput,
    ) -> Result<()> {
        let should_insert = match input.causation_id.as_deref() {
            Some(causation_id) => {
                !self
                    .execution_event_exists(bead_id, agent_id, input.event_type, causation_id)
                    .await?
            }
            None => true,
        };

        if should_insert {
            self.record_execution_event(bead_id, agent_id, input).await
        } else {
            Ok(())
        }
    }

    async fn execution_event_exists(
        &self,
        bead_id: &BeadId,
        agent_id: &AgentId,
        event_type: &str,
        causation_id: &str,
    ) -> Result<bool> {
        let repo_entity_id = event_entity_id(bead_id, agent_id.repo_id());
        let legacy_entity_id = format!("bead:{}", bead_id.value());

        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM execution_events
                WHERE bead_id = $1
                  AND event_type = $2
                  AND causation_id = $3
                  AND (entity_id = $4 OR entity_id = $5)
            )",
        )
        .bind(bead_id.value())
        .bind(event_type)
        .bind(causation_id)
        .bind(repo_entity_id)
        .bind(legacy_entity_id)
        .fetch_one(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to check existing execution event: {e}"))
        })
    }

    async fn record_landing_sync_outcome_if_absent(
        &self,
        bead_id: &BeadId,
        agent_id: &AgentId,
        status: BrSyncStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        self.record_execution_event_if_absent(
            bead_id,
            agent_id,
            ExecutionEventWriteInput {
                stage: Some(Stage::RedQueen),
                event_type: "landing_sync",
                causation_id: Some(landing_sync_causation_id(status, reason)),
                payload: json!({
                    "status": landing_sync_status_key(status),
                    "reason": reason.map(redact_sensitive),
                }),
                diagnostics: None,
            },
        )
        .await
    }

    async fn apply_failure_transition(
        &self,
        agent_id: &AgentId,
        message: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE agent_state
             SET status = 'waiting', feedback = $3, implementation_attempt = implementation_attempt + 1, current_stage = 'implement'
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(message)
        .execute(self.pool())
        .await
        .map(|_result| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to record failed stage: {e}")))
    }

    async fn table_has_column(&self, table_name: &str, column_name: &str) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = $1
                  AND column_name = $2
            )",
        )
        .bind(table_name)
        .bind(column_name)
        .fetch_one(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!(
                "Failed to inspect schema for {table_name}.{column_name}: {e}"
            ))
        })
    }
}

#[derive(Debug, Clone)]
struct FailureDiagnosticsPayload {
    category: String,
    retryable: bool,
    next_command: String,
    detail: Option<String>,
}

#[derive(Debug, Clone)]
struct ExecutionEventWriteInput {
    stage: Option<Stage>,
    event_type: &'static str,
    causation_id: Option<String>,
    payload: serde_json::Value,
    diagnostics: Option<FailureDiagnosticsPayload>,
}

fn build_failure_diagnostics(message: Option<&str>) -> FailureDiagnosticsPayload {
    let detail = message
        .map(redact_sensitive)
        .filter(|value| !value.trim().is_empty());
    FailureDiagnosticsPayload {
        category: message.map_or_else(
            || "stage_failure".to_string(),
            |value| classify_failure_category(value).to_string(),
        ),
        retryable: true,
        next_command: "swarm stage --stage implement".to_string(),
        detail,
    }
}

fn classify_failure_category(message: &str) -> &'static str {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("timeout") {
        "timeout"
    } else if lowered.contains("syntax") || lowered.contains("compile") {
        "compile_error"
    } else if lowered.contains("test") || lowered.contains("assert") {
        "test_failure"
    } else {
        "stage_failure"
    }
}

fn redact_sensitive(message: &str) -> String {
    message
        .split_whitespace()
        .map(redact_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn landing_retry_causation_id(reason: &str) -> String {
    format!(
        "landing-sync:retry:{}",
        reason
            .trim()
            .to_ascii_lowercase()
            .replace(char::is_whitespace, "-")
    )
}

const fn landing_sync_status_key(status: BrSyncStatus) -> &'static str {
    match status {
        BrSyncStatus::Synchronized => "synchronized",
        BrSyncStatus::RetryScheduled => "retry_scheduled",
        BrSyncStatus::Diverged => "diverged",
    }
}

fn landing_sync_causation_id(status: BrSyncStatus, reason: Option<&str>) -> String {
    match reason {
        Some(reason)
            if matches!(
                status,
                BrSyncStatus::RetryScheduled | BrSyncStatus::Diverged
            ) =>
        {
            format!(
                "landing-sync:{}:{}",
                landing_sync_status_key(status),
                reason
                    .trim()
                    .to_ascii_lowercase()
                    .replace(char::is_whitespace, "-")
            )
        }
        _ => format!("landing-sync:{}", landing_sync_status_key(status)),
    }
}

#[cfg(test)]
mod causation_tests {
    use super::{landing_retry_causation_id, landing_sync_causation_id, BrSyncStatus};

    #[test]
    fn landing_retry_causation_id_is_stable() {
        assert_eq!(
            landing_retry_causation_id("  JJ push FAILED with timeout  "),
            "landing-sync:retry:jj-push-failed-with-timeout"
        );
    }

    #[test]
    fn landing_sync_causation_id_includes_reason_for_retryable_states() {
        assert_eq!(
            landing_sync_causation_id(BrSyncStatus::RetryScheduled, Some("transport timeout")),
            "landing-sync:retry_scheduled:transport-timeout"
        );
        assert_eq!(
            landing_sync_causation_id(BrSyncStatus::Synchronized, Some("ignored")),
            "landing-sync:synchronized"
        );
    }
}

fn redact_token(token: &str) -> String {
    token.split_once('=').map_or_else(
        || token.to_string(),
        |(key, _)| {
            let normalized = key.to_ascii_lowercase();
            if ["token", "password", "secret", "api_key", "database_url"]
                .iter()
                .any(|sensitive| normalized.contains(sensitive))
            {
                format!("{key}=<redacted>")
            } else {
                token.to_string()
            }
        },
    )
}

fn event_entity_id(bead_id: &BeadId, repo_id: &RepoId) -> String {
    format!("repo:{}:bead:{}", repo_id.value(), bead_id.value())
}

fn determine_transition(stage: Stage, result: &StageResult) -> StageTransition {
    let decision = runtime_determine_transition_decision(
        to_runtime_stage(stage),
        &to_runtime_stage_result(result),
        0,
        1,
    );

    match decision.transition() {
        crate::ddd::RuntimeStageTransition::Advance(next_stage) => {
            StageTransition::Advance(to_stage(next_stage))
        }
        crate::ddd::RuntimeStageTransition::Retry => StageTransition::RetryImplement,
        crate::ddd::RuntimeStageTransition::Complete => StageTransition::Finalize,
        crate::ddd::RuntimeStageTransition::Block | crate::ddd::RuntimeStageTransition::NoOp => {
            StageTransition::NoOp
        }
    }
}

const fn to_runtime_stage(stage: Stage) -> RuntimeStage {
    match stage {
        Stage::RustContract => RuntimeStage::RustContract,
        Stage::Implement => RuntimeStage::Implement,
        Stage::QaEnforcer => RuntimeStage::QaEnforcer,
        Stage::RedQueen => RuntimeStage::RedQueen,
        Stage::Done => RuntimeStage::Done,
    }
}

fn to_runtime_stage_result(result: &StageResult) -> RuntimeStageResult {
    match result {
        StageResult::Started => RuntimeStageResult::Started,
        StageResult::Passed => RuntimeStageResult::Passed,
        StageResult::Failed(message) => RuntimeStageResult::Failed(message.clone()),
        StageResult::Error(message) => RuntimeStageResult::Error(message.clone()),
    }
}

const fn to_stage(stage: RuntimeStage) -> Stage {
    match stage {
        RuntimeStage::RustContract => Stage::RustContract,
        RuntimeStage::Implement => Stage::Implement,
        RuntimeStage::QaEnforcer => Stage::QaEnforcer,
        RuntimeStage::RedQueen => Stage::RedQueen,
        RuntimeStage::Done => Stage::Done,
    }
}

#[cfg(test)]
#[path = "write_ops_tests.rs"]
mod tests;
