use chrono::{DateTime, Utc};
use crate::db::{mappers::to_u32_i32, SwarmDb};
use crate::ddd::{
    runtime_determine_transition_decision, validate_completion_implies_push_confirmed,
    RuntimeStage, RuntimeStageResult, RuntimeStageTransition,
};
use crate::error::{Result, SwarmError};
use crate::types::{
    AgentId, ArtifactType, BeadId, EventSchemaVersion, MessageType, RepoId, Stage, StageArtifact,
    StageResult, SwarmStatus,
};
use crate::BrSyncStatus;
use serde_json::json;
use sqlx::Acquire;
use std::future::Future;
use std::pin::Pin;
use tracing::{debug, info};

#[derive(Debug, Clone, PartialEq, Eq)]
enum StageTransition {
    Finalize,
    Advance(Stage),
    RetryImplement,
    NoOp,
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
        sqlx::query_scalar::<_, bool>("SELECT heartbeat_bead_claim($1, $2, $3)")
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
             SET current_stage = $2, stage_started_at = NOW(), status = 'working'
             WHERE agent_id = $1",
        )
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
        .bind(event_entity_id(bead_id))
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

        self.apply_stage_transition(
            &determine_transition(stage, &result),
            agent_id,
            bead_id,
            stage,
            Some(stage_history_id),
            attempt,
            message,
        )
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

        let stage_history_row = sqlx::query!(
            "UPDATE stage_history
             SET status = $5, result = $6, feedback = $7, completed_at = NOW(), duration_ms = $8
             WHERE id = (
                SELECT id FROM stage_history
                WHERE agent_id = $1 AND bead_id = $2 AND stage = $3 AND attempt_number = $4 AND status = 'started'
                ORDER BY started_at DESC LIMIT 1
             )
             RETURNING id, completed_at",
        )
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt.cast_signed())
        .bind(&status)
        .bind(message)
        .bind(message)
        .bind(duration_ms.cast_signed())
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
        .map(|_| stage_history_id)
    }

    async fn persist_stage_transcript(
        &self,
        stage_history_id: i64,
        stage: Stage,
        attempt: u32,
        result: &StageResult,
        completed_at: DateTime<Utc>,
    ) -> Result<()> {
        let artifacts = self.get_stage_artifacts(stage_history_id).await?;
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
            .map(ToString::to_string)
            .unwrap_or_else(String::new);

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

        let transcript_text = serde_json::to_string(&transcript_body)
            .map_err(|e| SwarmError::DatabaseError(format!(
                "Failed to serialize stage transcript: {e}"
            )))?;

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
        .map(|_| ())?
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

    async fn apply_stage_transition(
        &self,
        transition: &StageTransition,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        stage_history_id: Option<i64>,
        attempt: u32,
        message: Option<&str>,
    ) -> Result<()> {
        match transition {
            StageTransition::Finalize => {
                self.finalize_agent_and_bead(agent_id, bead_id).await?;
                self.record_execution_event(
                    bead_id,
                    agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(stage),
                        event_type: "transition_finalize",
                        causation_id: stage_history_id.map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "finalize"}),
                        diagnostics: None,
                    },
                )
                .await
            }
            StageTransition::Advance(next_stage) => {
                self.advance_to_stage(agent_id, *next_stage).await?;
                self.record_execution_event(
                    bead_id,
                    agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(stage),
                        event_type: "transition_advance",
                        causation_id: stage_history_id.map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "advance", "next_stage": next_stage.as_str()}),
                        diagnostics: None,
                    },
                )
                .await
            }
            StageTransition::RetryImplement => {
                self.apply_failure_transition(agent_id, message).await?;
                self.record_execution_event(
                    bead_id,
                    agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(stage),
                        event_type: "transition_retry",
                        causation_id: stage_history_id.map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "retry", "next_stage": Stage::Implement.as_str()}),
                        diagnostics: Some(build_failure_diagnostics(message)),
                    },
                )
                .await
            }
            StageTransition::NoOp => {
                self.record_execution_event(
                    bead_id,
                    agent_id,
                    ExecutionEventWriteInput {
                        stage: Some(stage),
                        event_type: "transition_noop",
                        causation_id: stage_history_id.map(|id| format!("stage-history:{id}")),
                        payload: json!({"transition": "noop"}),
                        diagnostics: None,
                    },
                )
                .await
            }
        }
    }

    /// Sets the swarm status.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn set_swarm_status(&self, _repo_id: &RepoId, status: SwarmStatus) -> Result<()> {
        sqlx::query("UPDATE swarm_config SET swarm_status = $1 WHERE id = TRUE")
            .bind(status.as_str())
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to update status: {e}")))
    }

    /// Updates the swarm configuration.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database operation fails.
    pub async fn update_config(&self, max_agents: u32) -> Result<()> {
        sqlx::query("UPDATE swarm_config SET max_agents = $1 WHERE id = TRUE")
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
    pub async fn start_swarm(&self, _repo_id: &RepoId) -> Result<()> {
        sqlx::query("UPDATE swarm_config SET swarm_status = 'running', swarm_started_at = NOW() WHERE id = TRUE")
            .execute(self.pool())
            .await
            .map(|_| info!("Started swarm"))
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to start swarm: {e}")))
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
        seed_idle_agents_recursive(self, 1, count).await
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
             WHERE bead_id = $1
               AND claimed_by = $2
               AND status = 'in_progress'",
        )
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

        sqlx::query("UPDATE bead_backlog SET status = 'blocked' WHERE bead_id = $1")
            .bind(bead_id.value())
            .execute(&mut *conn)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to block backlog bead: {e}")))?;

        sqlx::query("UPDATE agent_state SET status = 'error', feedback = $2 WHERE agent_id = $1")
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
            "SELECT bead_id FROM agent_state WHERE agent_id = $1 FOR UPDATE",
        )
        .bind(agent_id.number().cast_signed())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to read agent state: {e}")))?
        .flatten();

        if let Some(bead_id) = bead.as_deref() {
            sqlx::query("DELETE FROM bead_claims WHERE bead_id = $1")
                .bind(bead_id)
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    SwarmError::DatabaseError(format!("Failed to clear bead claim on release: {e}"))
                })?;

            sqlx::query(
                "UPDATE bead_backlog
                 SET status = 'pending'
                 WHERE bead_id = $1 AND status <> 'completed'",
            )
            .bind(bead_id)
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to reset backlog status on release: {e}"))
            })?;
        }

        sqlx::query(
            "UPDATE agent_state
             SET bead_id = NULL,
                 current_stage = NULL,
                 stage_started_at = NULL,
                 status = 'idle',
                 feedback = NULL,
                 implementation_attempt = 0
             WHERE agent_id = $1",
        )
        .bind(agent_id.number().cast_signed())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to reset agent state: {e}")))?;

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
             SET status = 'waiting', feedback = $2, current_stage = 'red-queen'
             WHERE agent_id = $1",
        )
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
             WHERE agent_id = $1 AND bead_id = $2",
        )
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
             SET current_stage = $2, stage_started_at = NOW(), status = 'working'
             WHERE agent_id = $1",
        )
        .bind(agent_id.number().cast_signed())
        .bind(next_stage.as_str())
        .execute(self.pool())
        .await
        .map(|_result| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to advance stage: {e}")))
    }

    async fn lookup_agent_bead(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        sqlx::query_scalar::<_, String>("SELECT bead_id FROM agent_state WHERE agent_id = $1")
            .bind(agent_id.number().cast_signed())
            .fetch_optional(self.pool())
            .await
            .map(|bead| bead.map(BeadId::new))
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to lookup agent bead: {e}")))
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
        .bind(event_entity_id(bead_id))
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
                    .execution_event_exists(bead_id, input.event_type, causation_id)
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
        event_type: &str,
        causation_id: &str,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM execution_events
                WHERE bead_id = $1
                  AND event_type = $2
                  AND causation_id = $3
            )",
        )
        .bind(bead_id.value())
        .bind(event_type)
        .bind(causation_id)
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
             SET status = 'waiting', feedback = $2, implementation_attempt = implementation_attempt + 1, current_stage = 'implement'
             WHERE agent_id = $1",
        )
        .bind(agent_id.number().cast_signed())
        .bind(message)
        .execute(self.pool())
        .await
        .map(|_result| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to record failed stage: {e}")))
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
    let detail = message.map(redact_sensitive);
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

fn event_entity_id(bead_id: &BeadId) -> String {
    format!("bead:{}", bead_id.value())
}

fn seed_idle_agents_recursive<'a>(
    db: &'a SwarmDb,
    next: u32,
    count: u32,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if next > count {
            Ok(())
        } else {
            sqlx::query(
                "INSERT INTO agent_state (agent_id, status)
                 VALUES ($1, 'idle')
                 ON CONFLICT (agent_id) DO UPDATE
                 SET status = 'idle', bead_id = NULL, current_stage = NULL, stage_started_at = NULL,
                     feedback = NULL, implementation_attempt = 0",
            )
            .bind(next.cast_signed())
            .execute(db.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to seed agent {next}: {e}")))?;

            seed_idle_agents_recursive(db, next.saturating_add(1), count).await
        }
    })
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
