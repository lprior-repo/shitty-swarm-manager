use crate::db::mappers::{parse_agent_state, parse_swarm_config, to_u32_i32, AgentStateFields};
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{
    AgentId, AgentMessage, AgentState, AgentStatus, ArtifactType, AvailableAgent, BeadId,
    DeepResumeContextContract, ExecutionEvent, FailureDiagnostics, MessageType, ProgressSummary,
    RepoId, ResumeArtifactDetailContract, ResumeArtifactSummary, ResumeContextProjection,
    ResumeStageAttempt, ResumeStageAttemptContract, Stage, StageArtifact, SwarmConfig,
};
use serde::Deserialize;
use sqlx::FromRow;
use std::collections::HashMap;

#[derive(FromRow)]
struct AgentStateRow {
    bead_id: Option<String>,
    current_stage: Option<String>,
    stage_started_at: Option<chrono::DateTime<chrono::Utc>>,
    status: String,
    last_update: chrono::DateTime<chrono::Utc>,
    implementation_attempt: i32,
    feedback: Option<String>,
}

#[derive(FromRow)]
struct AvailableAgentRow {
    agent_id: i32,
    status: String,
    implementation_attempt: i32,
    max_implementation_attempts: i32,
    max_agents: i32,
}

#[derive(FromRow)]
struct ProgressRow {
    done: i64,
    working: i64,
    waiting: i64,
    error: i64,
    idle: i64,
    total: i64,
}

#[derive(FromRow)]
struct SwarmConfigRow {
    max_agents: i32,
    max_implementation_attempts: i32,
    claim_label: String,
    swarm_started_at: Option<chrono::DateTime<chrono::Utc>>,
    swarm_status: String,
}

#[derive(FromRow)]
struct ActiveAgentRow {
    agent_id: i32,
    bead_id: Option<String>,
    status: String,
}

#[derive(FromRow)]
struct FeedbackRow {
    bead_id: String,
    agent_id: i32,
    stage: String,
    attempt_number: i32,
    feedback: Option<String>,
    completed_at: Option<String>,
}

#[derive(FromRow)]
struct ResumeContextAggregateRow {
    agent_id: i32,
    bead_id: String,
    current_stage: Option<String>,
    implementation_attempt: i32,
    feedback: Option<String>,
    status: String,
    attempts_json: serde_json::Value,
    artifacts_json: serde_json::Value,
}

#[derive(Deserialize)]
struct ResumeStageAttemptJson {
    stage: String,
    attempt_number: i32,
    status: String,
    feedback: Option<String>,
    started_at: chrono::DateTime<chrono::Utc>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Deserialize)]
struct ResumeArtifactSummaryJson {
    artifact_type: String,
    created_at: chrono::DateTime<chrono::Utc>,
    content_hash: Option<String>,
    byte_length: i64,
}

#[derive(FromRow)]
struct StageArtifactRow {
    id: i64,
    stage_history_id: i64,
    artifact_type: String,
    content: String,
    metadata: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
    content_hash: Option<String>,
}

#[derive(FromRow)]
struct ResumeArtifactDetailRow {
    bead_id: String,
    artifact_type: String,
    content: String,
    metadata: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
    content_hash: Option<String>,
}

#[derive(FromRow)]
struct AgentMessageRow {
    id: i64,
    from_repo_id: String,
    from_agent_id: i32,
    to_repo_id: Option<String>,
    to_agent_id: Option<i32>,
    bead_id: Option<String>,
    message_type: String,
    subject: String,
    body: String,
    metadata: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
    read_at: Option<chrono::DateTime<chrono::Utc>>,
    read: bool,
}

#[derive(FromRow)]
struct CommandAuditRow {
    seq: i64,
    t: chrono::DateTime<chrono::Utc>,
    cmd: String,
    args: serde_json::Value,
    ok: bool,
    ms: i32,
    error_code: Option<String>,
}

#[derive(FromRow)]
struct ResourceLockRow {
    resource: String,
    agent: String,
    since: chrono::DateTime<chrono::Utc>,
    until_at: chrono::DateTime<chrono::Utc>,
}

#[derive(FromRow)]
struct ExecutionEventRow {
    seq: i64,
    schema_version: i32,
    event_type: String,
    entity_id: String,
    bead_id: Option<String>,
    agent_id: Option<i32>,
    stage: Option<String>,
    causation_id: Option<String>,
    diagnostics_category: Option<String>,
    diagnostics_retryable: Option<bool>,
    diagnostics_next_command: Option<String>,
    diagnostics_detail: Option<String>,
    payload: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl SwarmDb {
    /// Retrieves command audit history.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_command_history(
        &self,
        limit: i64,
    ) -> Result<
        Vec<(
            i64,
            i64,
            String,
            serde_json::Value,
            bool,
            u64,
            Option<String>,
        )>,
    > {
        sqlx::query("DELETE FROM resource_locks WHERE until_at <= NOW()")
            .execute(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to cleanup locks: {e}")))?;

        sqlx::query_as::<_, CommandAuditRow>(
            "SELECT seq, t, cmd, args, ok, ms, error_code
             FROM command_audit
             ORDER BY seq DESC
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to load command history: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        row.seq,
                        row.t.timestamp_millis(),
                        row.cmd,
                        row.args,
                        row.ok,
                        u64::from(row.ms.cast_unsigned()),
                        row.error_code,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    /// Lists active resource locks in the database.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn list_active_resource_locks(&self) -> Result<Vec<(String, String, i64, i64)>> {
        sqlx::query("DELETE FROM resource_locks WHERE until_at <= NOW()")
            .execute(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to cleanup locks: {e}")))?;

        sqlx::query_as::<_, ResourceLockRow>(
            "SELECT resource, agent, since, until_at
             FROM resource_locks
             ORDER BY since ASC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to load resource locks: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        row.resource,
                        row.agent,
                        row.since.timestamp_millis(),
                        row.until_at.timestamp_millis(),
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    /// Retrieves the current state of a specific agent.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_agent_state(&self, agent_id: &AgentId) -> Result<Option<AgentState>> {
        let agent_id_number = agent_id.number();
        sqlx::query_as::<_, AgentStateRow>(
            "SELECT bead_id, current_stage, stage_started_at, status, last_update, implementation_attempt, feedback
             FROM agent_state WHERE agent_id = $1",
        )
            .bind(agent_id_number.cast_signed())
            .fetch_optional(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get agent state: {e}")))
            .and_then(|row_opt| {
                row_opt
                    .map(|row| {
                        parse_agent_state(
                            agent_id,
                            AgentStateFields {
                                bead_id: row.bead_id,
                                stage_str: row.current_stage,
                                stage_started_at: row.stage_started_at,
                                status_str: row.status,
                                last_update: row.last_update,
                                implementation_attempt: row.implementation_attempt,
                                feedback: row.feedback,
                            },
                        )
                    })
                    .transpose()
            })
    }

    /// Gets a list of available agents for a repository.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_available_agents(&self, repo_id: &RepoId) -> Result<Vec<AvailableAgent>> {
        let local_repo = repo_id.clone();
        sqlx::query_as::<_, AvailableAgentRow>(
            "SELECT agent_id, status, implementation_attempt, max_implementation_attempts, max_agents
             FROM v_available_agents",
        )
            .fetch_all(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get available agents: {e}")))
            .and_then(|rows| {
                rows.into_iter()
                    .map(|row| {
                        AgentStatus::try_from(row.status.as_str())
                            .map_err(SwarmError::DatabaseError)
                            .map(|status| AvailableAgent {
                                repo_id: local_repo.clone(),
                                agent_id: to_u32_i32(row.agent_id),
                                status,
                                implementation_attempt: to_u32_i32(row.implementation_attempt),
                                max_implementation_attempts: to_u32_i32(row.max_implementation_attempts),
                                max_agents: to_u32_i32(row.max_agents),
                            })
                    })
                    .collect::<Result<Vec<_>>>()
            })
    }

    /// Retrieves the current progress summary for the swarm.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_progress(&self, _repo_id: &RepoId) -> Result<ProgressSummary> {
        sqlx::query_as::<_, ProgressRow>(
            "SELECT
                done_agents AS done,
                working_agents AS working,
                waiting_agents AS waiting,
                error_agents AS error,
                idle_agents AS idle,
                total_agents AS total
             FROM v_swarm_progress",
        )
        .fetch_one(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get progress: {e}")))
        .map(|row| ProgressSummary {
            completed: row.done.cast_unsigned(),
            working: row.working.cast_unsigned(),
            waiting: row.waiting.cast_unsigned(),
            errors: row.error.cast_unsigned(),
            idle: row.idle.cast_unsigned(),
            total_agents: row.total.cast_unsigned(),
        })
    }

    /// Retrieves the swarm configuration for a repository.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_config(&self, _repo_id: &RepoId) -> Result<SwarmConfig> {
        sqlx::query_as::<_, SwarmConfigRow>(
            "SELECT max_agents, max_implementation_attempts, claim_label, swarm_started_at, swarm_status
             FROM swarm_config WHERE id = TRUE",
        )
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get config: {e}")))
            .and_then(|row| {
                parse_swarm_config(
                    row.max_agents,
                    row.max_implementation_attempts,
                    row.claim_label,
                    row.swarm_started_at,
                    &row.swarm_status,
                )
            })
    }

    /// Lists all registered repositories.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub fn list_repos(&self) -> Result<Vec<(RepoId, String)>> {
        Ok(vec![(RepoId::new("local"), "local".to_string())])
    }

    /// Gets all active agents across all repositories.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_all_active_agents(
        &self,
    ) -> Result<Vec<(RepoId, u32, Option<String>, String)>> {
        sqlx::query_as::<_, ActiveAgentRow>(
            "SELECT agent_id, bead_id, status FROM v_active_agents ORDER BY last_update DESC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get active agents: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        RepoId::new("local"),
                        to_u32_i32(row.agent_id),
                        row.bead_id,
                        row.status,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    /// Claims the next available bead for an agent.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn claim_next_bead(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        let claim_agent_id = agent_id.number();
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_p0_bead($1)")
            .bind(claim_agent_id.cast_signed())
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to claim next bead: {e}")))
            .map(|value| value.map(BeadId::new))
    }

    /// Retrieves beads that require feedback from agents.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_feedback_required(
        &self,
    ) -> Result<Vec<(String, u32, String, u32, Option<String>, Option<String>)>> {
        sqlx::query_as::<_, FeedbackRow>(
            "SELECT bead_id, agent_id, stage, attempt_number, feedback, completed_at::TEXT
             FROM v_feedback_required
             ORDER BY completed_at DESC NULLS LAST",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to query feedback: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    (
                        row.bead_id,
                        to_u32_i32(row.agent_id),
                        row.stage,
                        to_u32_i32(row.attempt_number),
                        row.feedback,
                        row.completed_at,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    /// Retrieves deterministic execution events, optionally filtered by bead.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_execution_events(
        &self,
        bead_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ExecutionEvent>> {
        sqlx::query_as::<_, ExecutionEventRow>(
            "SELECT
                seq,
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
                payload,
                created_at
             FROM execution_events
             WHERE ($1::TEXT IS NULL OR bead_id = $1)
             ORDER BY seq DESC
             LIMIT $2",
        )
        .bind(bead_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to load execution events: {e}")))
        .map(|rows| {
            rows.into_iter()
                .map(|row| {
                    let diagnostics = diagnostics_from_row(&row);
                    ExecutionEvent {
                        seq: row.seq,
                        schema_version: row.schema_version,
                        event_type: row.event_type,
                        entity_id: row.entity_id,
                        bead_id: row.bead_id,
                        agent_id: row.agent_id.map(to_u32_i32),
                        stage: row.stage,
                        causation_id: row.causation_id,
                        diagnostics,
                        payload: row.payload,
                        created_at: row.created_at,
                    }
                })
                .collect::<Vec<_>>()
        })
    }

    /// Retrieves resumable execution context for each active bead.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_resume_context_projections(&self) -> Result<Vec<ResumeContextProjection>> {
        let artifact_types = resume_artifact_type_names();
        let contexts = sqlx::query_as::<_, ResumeContextAggregateRow>(
            "SELECT
                a.agent_id,
                a.bead_id,
                a.current_stage,
                a.implementation_attempt,
                a.feedback,
                a.status,
                COALESCE(
                    (
                        SELECT json_agg(
                            json_build_object(
                                'stage', sh.stage,
                                'attempt_number', sh.attempt_number,
                                'status', sh.status,
                                'feedback', sh.feedback,
                                'started_at', sh.started_at,
                                'completed_at', sh.completed_at
                            )
                            ORDER BY sh.attempt_number ASC, sh.started_at ASC, sh.id ASC
                        )
                        FROM stage_history sh
                        WHERE sh.bead_id = a.bead_id
                    ),
                    '[]'::json
                ) AS attempts_json,
                COALESCE(
                    (
                        SELECT json_agg(
                            json_build_object(
                                'artifact_type', latest.artifact_type,
                                'created_at', latest.created_at,
                                'content_hash', latest.content_hash,
                                'byte_length', latest.byte_length
                            )
                            ORDER BY latest.created_at ASC, latest.artifact_type ASC
                        )
                        FROM (
                            SELECT DISTINCT ON (sa.artifact_type)
                                sa.artifact_type,
                                sa.created_at,
                                sa.content_hash,
                                OCTET_LENGTH(sa.content)::BIGINT AS byte_length
                            FROM stage_artifacts sa
                            JOIN stage_history sh ON sh.id = sa.stage_history_id
                            WHERE sh.bead_id = a.bead_id
                              AND sa.artifact_type = ANY($1::TEXT[])
                            ORDER BY sa.artifact_type, sa.created_at DESC, sa.id DESC
                        ) AS latest
                    ),
                    '[]'::json
                ) AS artifacts_json
             FROM agent_state a
             WHERE a.bead_id IS NOT NULL
               AND a.status IN ('working', 'waiting', 'error')
             ORDER BY a.bead_id ASC, a.agent_id ASC",
        )
        .bind(artifact_types)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to load resume context rows: {e}"))
        })?;

        let mut projections = Vec::with_capacity(contexts.len());
        for context in contexts {
            let attempts = parse_resume_attempts(context.attempts_json.clone())?;
            let artifacts = parse_resume_artifacts(context.artifacts_json.clone())?;

            let status = AgentStatus::try_from(context.status.as_str())
                .map_err(SwarmError::DatabaseError)?;

            let current_stage = context
                .current_stage
                .as_deref()
                .map(Stage::try_from)
                .transpose()
                .map_err(SwarmError::DatabaseError)?;

            projections.push(ResumeContextProjection {
                agent_id: to_u32_i32(context.agent_id),
                bead_id: BeadId::new(&context.bead_id),
                status,
                current_stage,
                implementation_attempt: to_u32_i32(context.implementation_attempt),
                feedback: context.feedback.clone(),
                attempts,
                artifacts,
            });
        }

        Ok(projections)
    }

    pub async fn get_deep_resume_contexts(&self) -> Result<Vec<DeepResumeContextContract>> {
        let projections = self.get_resume_context_projections().await?;
        if projections.is_empty() {
            return Ok(Vec::new());
        }

        let bead_ids = projections
            .iter()
            .map(|context| context.bead_id.value().to_string())
            .collect::<Vec<_>>();

        let diagnostics_map = self.get_latest_diagnostics_for_beads(&bead_ids).await?;
        let artifacts_map = self.get_latest_artifacts_for_beads(&bead_ids).await?;

        let contexts = projections
            .into_iter()
            .map(|projection| {
                let diagnostics = diagnostics_map.get(projection.bead_id.value()).cloned();
                let artifacts = artifacts_map
                    .get(projection.bead_id.value())
                    .cloned()
                    .unwrap_or_default();
                let attempts = projection
                    .attempts
                    .iter()
                    .map(|attempt| ResumeStageAttemptContract {
                        stage: attempt.stage.as_str().to_string(),
                        attempt_number: attempt.attempt_number,
                        status: attempt.status.clone(),
                        feedback: attempt.feedback.clone(),
                        started_at: attempt.started_at,
                        completed_at: attempt.completed_at,
                    })
                    .collect::<Vec<_>>();

                DeepResumeContextContract {
                    agent_id: projection.agent_id,
                    bead_id: projection.bead_id.value().to_string(),
                    status: projection.status.as_str().to_string(),
                    current_stage: projection
                        .current_stage
                        .map(|stage| stage.as_str().to_string()),
                    implementation_attempt: projection.implementation_attempt,
                    feedback: projection.feedback.clone(),
                    attempts,
                    diagnostics,
                    artifacts,
                }
            })
            .collect::<Vec<_>>();

        Ok(contexts)
    }

    async fn get_latest_diagnostics_for_beads(
        &self,
        bead_ids: &[String],
    ) -> Result<HashMap<String, FailureDiagnostics>> {
        if bead_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query_as::<_, ExecutionEventRow>(
            "SELECT DISTINCT ON (bead_id)
                seq,
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
                payload,
                created_at
             FROM execution_events
             WHERE bead_id = ANY($1::TEXT[])
               AND diagnostics_category IS NOT NULL
             ORDER BY bead_id, seq DESC",
        )
        .bind(bead_ids)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!(
                "Failed to load diagnostics for resume context: {e}"
            ))
        })?;

        let mut map = HashMap::new();
        for row in rows {
            if let Some(bead_id) = row.bead_id.clone() {
                if let Some(diagnostics) = diagnostics_from_row(&row) {
                    map.insert(bead_id, diagnostics);
                }
            }
        }

        Ok(map)
    }

    async fn get_latest_artifacts_for_beads(
        &self,
        bead_ids: &[String],
    ) -> Result<HashMap<String, Vec<ResumeArtifactDetailContract>>> {
        if bead_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let artifact_types = resume_artifact_type_names();
        let rows = sqlx::query_as::<_, ResumeArtifactDetailRow>(
            "SELECT DISTINCT ON (sh.bead_id, sa.artifact_type)
                sh.bead_id,
                sa.artifact_type,
                sa.content,
                sa.metadata,
                sa.created_at,
                sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sh.id = sa.stage_history_id
             WHERE sh.bead_id = ANY($1::TEXT[])
               AND sa.artifact_type = ANY($2::TEXT[])
             ORDER BY sh.bead_id, sa.artifact_type, sa.created_at DESC, sa.id DESC",
        )
        .bind(bead_ids)
        .bind(artifact_types)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!(
                "Failed to load artifact contents for resume context: {e}"
            ))
        })?;

        let mut map: HashMap<String, Vec<ResumeArtifactDetailContract>> = HashMap::new();
        for row in rows {
            let bead_id = row.bead_id;
            let content = row.content;
            let byte_length = content.as_bytes().len() as u64;
            let detail = ResumeArtifactDetailContract {
                artifact_type: row.artifact_type,
                created_at: row.created_at,
                content,
                metadata: row.metadata,
                content_hash: row.content_hash,
                byte_length,
            };
            map.entry(bead_id).or_default().push(detail);
        }

        Ok(map)
    }

    /// Retrieves all artifacts for a specific stage history.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_stage_artifacts(&self, stage_history_id: i64) -> Result<Vec<StageArtifact>> {
        sqlx::query_as::<_, StageArtifactRow>(
            "SELECT id, stage_history_id, artifact_type, content, metadata, created_at, content_hash
             FROM stage_artifacts
             WHERE stage_history_id = $1
             ORDER BY created_at ASC",
        )
        .bind(stage_history_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get stage artifacts: {e}")))
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    ArtifactType::try_from(row.artifact_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|artifact_type| StageArtifact {
                            id: row.id,
                            stage_history_id: row.stage_history_id,
                            artifact_type,
                            content: row.content,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            content_hash: row.content_hash,
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
    }

    /// Retrieves artifacts of a specific type for a bead.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_bead_artifacts_by_type(
        &self,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<Vec<StageArtifact>> {
        sqlx::query_as::<_, StageArtifactRow>(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sa.stage_history_id = sh.id
             WHERE sh.bead_id = $1 AND sa.artifact_type = $2
             ORDER BY sa.created_at ASC",
        )
        .bind(bead_id.value())
        .bind(artifact_type.as_str())
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to get bead artifacts by type: {e}"))
        })
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    ArtifactType::try_from(row.artifact_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|mapped_type| StageArtifact {
                            id: row.id,
                            stage_history_id: row.stage_history_id,
                            artifact_type: mapped_type,
                            content: row.content,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            content_hash: row.content_hash,
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
    }

    /// Retrieves artifacts for a bead with optional type filter.
    pub async fn get_bead_artifacts(
        &self,
        bead_id: &BeadId,
        artifact_type: Option<ArtifactType>,
    ) -> Result<Vec<StageArtifact>> {
        if let Some(kind) = artifact_type {
            return self.get_bead_artifacts_by_type(bead_id, kind).await;
        }

        sqlx::query_as::<_, StageArtifactRow>(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sa.stage_history_id = sh.id
             WHERE sh.bead_id = $1
             ORDER BY sa.created_at ASC, sa.id ASC",
        )
        .bind(bead_id.value())
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get bead artifacts: {e}")))
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    ArtifactType::try_from(row.artifact_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|mapped_type| StageArtifact {
                            id: row.id,
                            stage_history_id: row.stage_history_id,
                            artifact_type: mapped_type,
                            content: row.content,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            content_hash: row.content_hash,
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
    }

    /// Retrieves the first artifact of a specific type for a bead.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_first_bead_artifact_by_type(
        &self,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<Option<StageArtifact>> {
        sqlx::query_as::<_, StageArtifactRow>(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sa.stage_history_id = sh.id
             WHERE sh.bead_id = $1 AND sa.artifact_type = $2
             ORDER BY sa.created_at ASC
             LIMIT 1",
        )
        .bind(bead_id.value())
        .bind(artifact_type.as_str())
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to get first bead artifact by type: {e}"))
        })
        .and_then(|maybe_row| {
            maybe_row
                .map(|row| {
                    ArtifactType::try_from(row.artifact_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|mapped_type| StageArtifact {
                            id: row.id,
                            stage_history_id: row.stage_history_id,
                            artifact_type: mapped_type,
                            content: row.content,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            content_hash: row.content_hash,
                        })
                })
                .transpose()
        })
    }

    /// Checks if a bead has any artifacts of a specific type.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn bead_has_artifact_type(
        &self,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM stage_artifacts sa
                 JOIN stage_history sh ON sa.stage_history_id = sh.id
                 WHERE sh.bead_id = $1 AND sa.artifact_type = $2
             )",
        )
        .bind(bead_id.value())
        .bind(artifact_type.as_str())
        .fetch_one(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to check bead artifact existence: {e}"))
        })
    }

    /// Retrieves unread messages for an agent, optionally filtered by bead.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_unread_messages(
        &self,
        agent_id: &AgentId,
        bead_id: Option<&BeadId>,
    ) -> Result<Vec<AgentMessage>> {
        sqlx::query_as::<_, AgentMessageRow>(
            "SELECT id, from_repo_id, from_agent_id, to_repo_id, to_agent_id, bead_id, message_type,
                    subject, body, metadata, created_at, read_at, read
             FROM get_unread_messages($1, $2, $3)",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .bind(bead_id.map(BeadId::value))
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get unread messages: {e}")))
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    MessageType::try_from(row.message_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|message_type| AgentMessage {
                            id: row.id,
                            from_repo_id: row.from_repo_id,
                            from_agent_id: to_u32_i32(row.from_agent_id),
                            to_repo_id: row.to_repo_id,
                            to_agent_id: row.to_agent_id.map(to_u32_i32),
                            bead_id: row.bead_id.map(BeadId::new),
                            message_type,
                            subject: row.subject,
                            body: row.body,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            read_at: row.read_at,
                            read: row.read,
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
    }

    /// Retrieves all unread messages across the system.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database query fails.
    pub async fn get_all_unread_messages(&self) -> Result<Vec<AgentMessage>> {
        sqlx::query_as::<_, AgentMessageRow>(
            "SELECT id, from_repo_id, from_agent_id, to_repo_id, to_agent_id, bead_id, message_type,
                    subject, body, metadata, created_at, read_at, read
             FROM v_unread_messages",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to get all unread messages: {e}"))
        })
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    MessageType::try_from(row.message_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|message_type| AgentMessage {
                            id: row.id,
                            from_repo_id: row.from_repo_id,
                            from_agent_id: to_u32_i32(row.from_agent_id),
                            to_repo_id: row.to_repo_id,
                            to_agent_id: row.to_agent_id.map(to_u32_i32),
                            bead_id: row.bead_id.map(BeadId::new),
                            message_type,
                            subject: row.subject,
                            body: row.body,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            read_at: row.read_at,
                            read: row.read,
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
    }
}

fn diagnostics_from_row(row: &ExecutionEventRow) -> Option<FailureDiagnostics> {
    row.diagnostics_category
        .as_ref()
        .zip(row.diagnostics_retryable)
        .zip(row.diagnostics_next_command.as_ref())
        .map(|((category, retryable), next_command)| FailureDiagnostics {
            category: category.clone(),
            retryable,
            next_command: next_command.clone(),
            detail: row.diagnostics_detail.clone(),
        })
}

fn parse_resume_attempts(json: serde_json::Value) -> Result<Vec<ResumeStageAttempt>> {
    serde_json::from_value::<Vec<ResumeStageAttemptJson>>(json)
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to decode resume attempts: {e}")))
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    Stage::try_from(row.stage.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|stage| ResumeStageAttempt {
                            stage,
                            attempt_number: to_u32_i32(row.attempt_number),
                            status: row.status,
                            feedback: row.feedback,
                            started_at: row.started_at,
                            completed_at: row.completed_at,
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
}

fn parse_resume_artifacts(json: serde_json::Value) -> Result<Vec<ResumeArtifactSummary>> {
    serde_json::from_value::<Vec<ResumeArtifactSummaryJson>>(json)
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to decode resume artifacts: {e}")))
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    ArtifactType::try_from(row.artifact_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|artifact_type| ResumeArtifactSummary {
                            artifact_type,
                            created_at: row.created_at,
                            content_hash: row.content_hash,
                            byte_length: row.byte_length.max(0).cast_unsigned(),
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
}

fn resume_artifact_type_names() -> Vec<String> {
    [
        ArtifactType::ContractDocument,
        ArtifactType::ImplementationCode,
        ArtifactType::FailureDetails,
        ArtifactType::ErrorMessage,
        ArtifactType::Feedback,
        ArtifactType::ValidationReport,
        ArtifactType::TestResults,
        ArtifactType::StageLog,
        ArtifactType::RetryPacket,
    ]
    .iter()
    .map(|value| value.as_str().to_string())
    .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use super::{parse_resume_artifacts, parse_resume_attempts, resume_artifact_type_names};
    use crate::types::{ArtifactType, Stage};
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn parse_resume_attempts_decodes_stored_history() {
        let now = Utc::now();
        let attempts_json = json!([
            {
                "stage": "rust-contract",
                "attempt_number": 1,
                "status": "passed",
                "feedback": null,
                "started_at": now,
                "completed_at": now,
            },
            {
                "stage": "implement",
                "attempt_number": 2,
                "status": "failed",
                "feedback": "missing artifact",
                "started_at": now,
                "completed_at": null,
            }
        ]);

        let attempts = parse_resume_attempts(attempts_json).expect("parse resume attempts");

        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].stage, Stage::RustContract);
        assert_eq!(attempts[0].attempt_number, 1);
        assert_eq!(attempts[1].stage, Stage::Implement);
        assert_eq!(attempts[1].status, "failed");
        assert_eq!(attempts[1].feedback.as_deref(), Some("missing artifact"));
        assert!(attempts[1].completed_at.is_none());
    }

    #[test]
    fn parse_resume_artifacts_clamps_negative_byte_lengths_and_maps_types() {
        let now = Utc::now();
        let artifacts_json = json!([
            {
                "artifact_type": "contract_document",
                "created_at": now,
                "content_hash": "doc-hash",
                "byte_length": 256,
            },
            {
                "artifact_type": "failure_details",
                "created_at": now,
                "content_hash": null,
                "byte_length": -10,
            }
        ]);

        let artifacts = parse_resume_artifacts(artifacts_json).expect("parse resume artifacts");

        assert_eq!(artifacts.len(), 2);
        assert_eq!(artifacts[0].artifact_type, ArtifactType::ContractDocument);
        assert_eq!(artifacts[1].artifact_type, ArtifactType::FailureDetails);
        assert_eq!(artifacts[1].byte_length, 0);
        assert_eq!(artifacts[0].content_hash.as_deref(), Some("doc-hash"));
    }

    #[test]
    fn resume_artifact_query_types_include_contract_and_implementation() {
        let query_types = resume_artifact_type_names();
        assert!(
            query_types.contains(&"contract_document".to_string()),
            "contract artifact missing from query type list"
        );
        assert!(
            query_types.contains(&"implementation_code".to_string()),
            "implementation artifact missing from query type list"
        );
        assert!(
            query_types.contains(&"retry_packet".to_string()),
            "retry packet artifact missing from resume query type list"
        );
        assert!(
            !query_types.contains(&"requirements".to_string()),
            "non-actionable requirements artifact leaked into resume query"
        );
    }
}
