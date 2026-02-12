use crate::ddd::{
    RuntimeAgentId, RuntimeAgentState, RuntimeAgentStatus, RuntimeBeadId, RuntimeRepoId,
    RuntimeStage,
};
use crate::error::{Result, SwarmError};
use crate::types::{
    AgentId, AgentMessage, AgentStatus, ArtifactType, AvailableAgent, BeadId,
    DeepResumeContextContract, ExecutionEvent, MessageType, ProgressSummary, RepoId,
    ResumeContextProjection, Stage, StageArtifact, SwarmConfig, SwarmStatus,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct SwarmDb {
    pool: PgPool,
    schema_cache: Arc<Mutex<HashMap<(String, String), bool>>>,
}

impl Clone for SwarmDb {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            schema_cache: Arc::clone(&self.schema_cache),
        }
    }
}

impl SwarmDb {
    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when the connection cannot be established.
    pub async fn new(connection_string: &str) -> Result<Self> {
        Self::new_with_timeout(connection_string, None).await
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when the connection cannot be established.
    pub async fn new_with_timeout(
        connection_string: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Self> {
        let connect_timeout = Duration::from_millis(timeout_ms.unwrap_or(3_000));
        PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(connect_timeout)
            .connect(connection_string)
            .await
            .map(|pool| Self {
                pool,
                schema_cache: Arc::new(Mutex::new(HashMap::new())),
            })
            .map_err(|error| {
                SwarmError::DatabaseError(format!("Failed to connect to database: {error}"))
            })
    }

    #[must_use]
    pub fn new_with_pool(pool: PgPool) -> Self {
        Self {
            pool,
            schema_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    #[must_use]
    pub fn check_schema_cache(&self, table_name: &str, column_name: &str) -> Option<bool> {
        let cache = self.schema_cache.lock().ok()?;
        // Convert inputs to owned strings for cache lookup
        let key = (table_name.to_string(), column_name.to_string());
        cache.get(&key).copied()
    }

    pub fn update_schema_cache(&self, table_name: &str, column_name: &str, value: bool) {
        if let Ok(mut cache) = self.schema_cache.lock() {
            let key = (table_name.to_string(), column_name.to_string());
            cache.insert(key, value);
        }
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence or mapping fails.
    pub async fn get_stage_artifacts(
        &self,
        repo_id: &RepoId,
        stage_history_id: i64,
    ) -> Result<Vec<StageArtifact>> {
        let rows = sqlx::query_as::<
            _,
            (
                i64,
                i64,
                String,
                String,
                Option<serde_json::Value>,
                chrono::DateTime<chrono::Utc>,
                Option<String>,
            ),
        >(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sh.id = sa.stage_history_id
             WHERE sh.repo_id = $1 AND sa.stage_history_id = $2
             ORDER BY sa.created_at ASC, sa.id ASC",
        )
        .bind(repo_id.value())
        .bind(stage_history_id)
        .fetch_all(self.pool())
        .await
        .map_err(|error| SwarmError::DatabaseError(format!("Failed to load stage artifacts: {error}")))?;

        rows.into_iter()
            .map(
                |(
                    id,
                    stage_history_id,
                    artifact_type,
                    content,
                    metadata,
                    created_at,
                    content_hash,
                )| {
                    let artifact_type = ArtifactType::try_from(artifact_type.as_str())
                        .map_err(SwarmError::DatabaseError)?;
                    Ok(StageArtifact {
                        id,
                        stage_history_id,
                        artifact_type,
                        content,
                        metadata,
                        created_at,
                        content_hash,
                    })
                },
            )
            .collect()
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence or mapping fails.
    pub async fn get_config(&self, repo_id: &RepoId) -> Result<SwarmConfig> {
        let row = sqlx::query_as::<
            _,
            (
                i32,
                i32,
                Option<String>,
                Option<chrono::DateTime<chrono::Utc>>,
                String,
            ),
        >(
            "SELECT max_agents, max_implementation_attempts, claim_label, swarm_started_at, swarm_status
             FROM swarm_config
             WHERE repo_id = $1
             ORDER BY swarm_started_at DESC NULLS LAST
             LIMIT 1",
        )
        .bind(repo_id.value())
        .fetch_optional(self.pool())
        .await
        .map_err(|error| SwarmError::DatabaseError(format!("Failed to load swarm config: {error}")))?;

        if let Some((max_agents, max_attempts, claim_label, swarm_started_at, swarm_status)) = row {
            let status =
                SwarmStatus::try_from(swarm_status.as_str()).map_err(SwarmError::DatabaseError)?;
            return Ok(SwarmConfig {
                repo_id: repo_id.clone(),
                max_agents: max_agents.max(0).cast_unsigned(),
                max_implementation_attempts: max_attempts.max(0).cast_unsigned(),
                claim_label: claim_label.unwrap_or_else(|| "swarm".to_string()),
                swarm_started_at,
                swarm_status: status,
            });
        }

        Ok(SwarmConfig {
            repo_id: repo_id.clone(),
            max_agents: 10,
            max_implementation_attempts: 3,
            claim_label: "swarm".to_string(),
            swarm_started_at: None,
            swarm_status: SwarmStatus::Initializing,
        })
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence or mapping fails.
    pub async fn get_bead_artifacts_by_type(
        &self,
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<Vec<StageArtifact>> {
        let rows = sqlx::query_as::<
            _,
            (
                i64,
                i64,
                String,
                String,
                Option<serde_json::Value>,
                chrono::DateTime<chrono::Utc>,
                Option<String>,
            ),
        >(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sh.id = sa.stage_history_id
             WHERE sh.repo_id = $1 AND sh.bead_id = $2 AND sa.artifact_type = $3
             ORDER BY sa.created_at ASC, sa.id ASC",
        )
        .bind(repo_id.value())
        .bind(bead_id.value())
        .bind(artifact_type.as_str())
        .fetch_all(self.pool())
        .await
        .map_err(|error| SwarmError::DatabaseError(format!("Failed to load bead artifacts: {error}")))?;

        rows.into_iter()
            .map(
                |(
                    id,
                    stage_history_id,
                    artifact_type,
                    content,
                    metadata,
                    created_at,
                    content_hash,
                )| {
                    let artifact_type = ArtifactType::try_from(artifact_type.as_str())
                        .map_err(SwarmError::DatabaseError)?;
                    Ok(StageArtifact {
                        id,
                        stage_history_id,
                        artifact_type,
                        content,
                        metadata,
                        created_at,
                        content_hash,
                    })
                },
            )
            .collect()
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
    pub async fn get_first_bead_artifact_by_type(
        &self,
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<Option<StageArtifact>> {
        self.get_bead_artifacts_by_type(repo_id, bead_id, artifact_type)
            .await
            .map(|mut artifacts| {
                if artifacts.is_empty() {
                    None
                } else {
                    Some(artifacts.remove(0))
                }
            })
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
    pub async fn get_latest_bead_artifact_by_type(
        &self,
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<Option<StageArtifact>> {
        self.get_bead_artifacts_by_type(repo_id, bead_id, artifact_type)
            .await
            .map(|mut artifacts| artifacts.pop())
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence or mapping fails.
    pub async fn get_agent_state(&self, agent_id: &AgentId) -> Result<Option<RuntimeAgentState>> {
        let row = sqlx::query_as::<_, (Option<String>, Option<String>, String, i32)>(
            "SELECT bead_id, current_stage, status, implementation_attempt
             FROM agent_state
             WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .fetch_optional(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load agent state: {error}"))
        })?;

        row.map_or(
            Ok(None),
            |(bead_id, current_stage, status, implementation_attempt)| {
                let parsed_stage = current_stage
                    .map(|value| RuntimeStage::try_from(value.as_str()))
                    .transpose()
                    .map_err(SwarmError::DatabaseError)?;
                let parsed_status = RuntimeAgentStatus::try_from(status.as_str())
                    .map_err(SwarmError::DatabaseError)?;
                let runtime_agent = RuntimeAgentId::new(
                    RuntimeRepoId::new(agent_id.repo_id().value().to_string()),
                    agent_id.number(),
                );
                let state = RuntimeAgentState::new(
                    runtime_agent,
                    bead_id.map(RuntimeBeadId::new),
                    parsed_stage,
                    parsed_status,
                    implementation_attempt.max(0).cast_unsigned(),
                );
                state
                    .validate_invariants()
                    .map_err(|error| SwarmError::DatabaseError(error.to_string()))?;
                Ok(Some(state))
            },
        )
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
    pub async fn bead_has_artifact_type(
        &self,
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1
                 FROM stage_artifacts sa
                 JOIN stage_history sh ON sh.id = sa.stage_history_id
                 WHERE sh.repo_id = $1 AND sh.bead_id = $2 AND sa.artifact_type = $3
             )",
        )
        .bind(repo_id.value())
        .bind(bead_id.value())
        .bind(artifact_type.as_str())
        .fetch_one(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to inspect artifact presence: {error}"))
        })
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence or mapping fails.
    pub async fn get_available_agents(&self, repo_id: &RepoId) -> Result<Vec<AvailableAgent>> {
        let rows = sqlx::query_as::<_, (i32, String, i32, i32, i32)>(
            "SELECT
                a.agent_id,
                a.status,
                a.implementation_attempt,
                c.max_implementation_attempts,
                c.max_agents
             FROM agent_state a
             JOIN swarm_config c ON c.repo_id = a.repo_id
             WHERE a.repo_id = $1
             ORDER BY a.agent_id ASC",
        )
        .bind(repo_id.value())
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load available agents: {error}"))
        })?;

        rows.into_iter()
            .map(
                |(agent_id, status, implementation_attempt, max_attempts, max_agents)| {
                    let status = AgentStatus::try_from(status.as_str())
                        .map_err(SwarmError::DatabaseError)?;
                    Ok(AvailableAgent {
                        repo_id: repo_id.clone(),
                        agent_id: agent_id.max(0).cast_unsigned(),
                        status,
                        implementation_attempt: implementation_attempt.max(0).cast_unsigned(),
                        max_implementation_attempts: max_attempts.max(0).cast_unsigned(),
                        max_agents: max_agents.max(0).cast_unsigned(),
                    })
                },
            )
            .collect()
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
    pub async fn get_progress(&self, repo_id: &RepoId) -> Result<ProgressSummary> {
        let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
            "SELECT
                COUNT(*) FILTER (WHERE status = 'working') AS working,
                COUNT(*) FILTER (WHERE status = 'idle') AS idle,
                COUNT(*) FILTER (WHERE status = 'waiting') AS waiting,
                COUNT(*) FILTER (WHERE status = 'done') AS done,
                COUNT(*) FILTER (WHERE status = 'error') AS errors
             FROM agent_state
             WHERE repo_id = $1",
        )
        .bind(repo_id.value())
        .fetch_one(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load progress summary: {error}"))
        })?;

        let (working, idle, waiting, completed, errors) = row;
        Ok(ProgressSummary {
            completed: completed.max(0).cast_unsigned(),
            working: working.max(0).cast_unsigned(),
            waiting: waiting.max(0).cast_unsigned(),
            errors: errors.max(0).cast_unsigned(),
            idle: idle.max(0).cast_unsigned(),
            total_agents: (working + idle + waiting + completed + errors)
                .max(0)
                .cast_unsigned(),
        })
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
    pub async fn get_active_agents(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<(RepoId, u32, Option<String>, String)>> {
        sqlx::query_as::<_, (i32, Option<String>, String)>(
            "SELECT agent_id, bead_id, status
             FROM agent_state
             WHERE repo_id = $1 AND status <> 'idle'
             ORDER BY agent_id ASC",
        )
        .bind(repo_id.value())
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load active agents: {error}"))
        })
        .map(|rows| {
            rows.into_iter()
                .map(|(agent_id, bead_id, status)| {
                    (
                        repo_id.clone(),
                        agent_id.max(0).cast_unsigned(),
                        bead_id,
                        status,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
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
        sqlx::query_as::<
            _,
            (
                i64,
                chrono::DateTime<chrono::Utc>,
                String,
                serde_json::Value,
                bool,
                i64,
                Option<String>,
            ),
        >(
            "SELECT seq, t, cmd, args, ok, ms, error_code
             FROM command_audit
             ORDER BY seq DESC
             LIMIT $1",
        )
        .bind(limit.max(0))
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load command history: {error}"))
        })
        .map(|rows| {
            rows.into_iter()
                .map(|(seq, t, cmd, args, ok, ms, error_code)| {
                    (
                        seq,
                        t.timestamp_millis(),
                        cmd,
                        args,
                        ok,
                        ms.max(0).cast_unsigned(),
                        error_code,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
    pub async fn list_active_resource_locks(&self) -> Result<Vec<(String, String, i64, i64)>> {
        sqlx::query_as::<
            _,
            (
                String,
                String,
                chrono::DateTime<chrono::Utc>,
                chrono::DateTime<chrono::Utc>,
            ),
        >(
            "SELECT resource, agent, until_at, until_at
             FROM resource_locks
             WHERE until_at > NOW()
             ORDER BY resource ASC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load active resource locks: {error}"))
        })
        .map(|rows| {
            rows.into_iter()
                .map(|(resource, agent, until_at, expires_at)| {
                    (
                        resource,
                        agent,
                        until_at.timestamp(),
                        expires_at.timestamp(),
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence or mapping fails.
    pub async fn get_execution_events(
        &self,
        repo_id: &RepoId,
        bead_filter: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ExecutionEvent>> {
        let rows = sqlx::query_as::<
            _,
            (
                i64,
                i32,
                String,
                String,
                Option<String>,
                Option<i32>,
                Option<String>,
                Option<String>,
                Option<serde_json::Value>,
                Option<serde_json::Value>,
                chrono::DateTime<chrono::Utc>,
            ),
        >(
            "SELECT seq, schema_version, event_type, entity_id, bead_id, agent_id, stage, causation_id, diagnostics, payload, created_at
             FROM execution_events
             WHERE repo_id = $1 AND ($2::text IS NULL OR bead_id = $2)
             ORDER BY seq DESC
             LIMIT $3",
        )
        .bind(repo_id.value())
        .bind(bead_filter)
        .bind(limit.max(1))
        .fetch_all(self.pool())
        .await
        .map_err(|error| SwarmError::DatabaseError(format!("Failed to load execution events: {error}")))?;

        rows.into_iter()
            .map(
                |(
                    seq,
                    schema_version,
                    event_type,
                    entity_id,
                    bead_id,
                    agent_id,
                    stage,
                    causation_id,
                    diagnostics,
                    payload,
                    created_at,
                )| {
                    let diagnostics = diagnostics
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|error| {
                            SwarmError::DatabaseError(format!(
                                "Failed to decode diagnostics payload: {error}"
                            ))
                        })?;
                    Ok(ExecutionEvent {
                        seq,
                        schema_version,
                        event_type,
                        entity_id,
                        bead_id,
                        agent_id: agent_id.map(i32::cast_unsigned),
                        stage,
                        causation_id,
                        diagnostics,
                        payload,
                        created_at,
                    })
                },
            )
            .collect()
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence or mapping fails.
    pub async fn get_all_unread_messages(&self) -> Result<Vec<AgentMessage>> {
        let rows = sqlx::query_as::<
            _,
            (
                i64,
                String,
                i32,
                Option<String>,
                Option<i32>,
                Option<String>,
                String,
                String,
                String,
                Option<serde_json::Value>,
                chrono::DateTime<chrono::Utc>,
                Option<chrono::DateTime<chrono::Utc>>,
                bool,
            ),
        >(
            "SELECT id, from_repo_id, from_agent_id, to_repo_id, to_agent_id, bead_id, message_type, subject, body, metadata, created_at, read_at, read
             FROM agent_messages
             WHERE read = FALSE
             ORDER BY created_at DESC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|error| SwarmError::DatabaseError(format!("Failed to load unread messages: {error}")))?;

        rows.into_iter()
            .map(
                |(
                    id,
                    from_repo_id,
                    from_agent_id,
                    to_repo_id,
                    to_agent_id,
                    bead_id,
                    message_type,
                    subject,
                    body,
                    metadata,
                    created_at,
                    read_at,
                    read,
                )| {
                    let message_type = MessageType::try_from(message_type.as_str())
                        .map_err(SwarmError::DatabaseError)?;
                    Ok(AgentMessage {
                        id,
                        from_repo_id,
                        from_agent_id: from_agent_id.max(0).cast_unsigned(),
                        to_repo_id,
                        to_agent_id: to_agent_id.map(i32::cast_unsigned),
                        bead_id: bead_id.map(BeadId::new),
                        message_type,
                        subject,
                        body,
                        metadata,
                        created_at,
                        read_at,
                        read,
                    })
                },
            )
            .collect()
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence or mapping fails.
    pub async fn get_resume_context_projections(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<ResumeContextProjection>> {
        let rows = sqlx::query_as::<_, (i32, String, String, Option<String>, i32, Option<String>)>(
            "SELECT agent_id, bead_id, status, current_stage, implementation_attempt, feedback
             FROM agent_state
             WHERE repo_id = $1 AND bead_id IS NOT NULL
             ORDER BY agent_id ASC",
        )
        .bind(repo_id.value())
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!(
                "Failed to load resume context projections: {error}"
            ))
        })?;

        rows.into_iter()
            .map(
                |(agent_id, bead_id, status, current_stage, implementation_attempt, feedback)| {
                    let status = AgentStatus::try_from(status.as_str())
                        .map_err(SwarmError::DatabaseError)?;
                    let current_stage = current_stage
                        .map(|value| Stage::try_from(value.as_str()))
                        .transpose()
                        .map_err(SwarmError::DatabaseError)?;
                    Ok(ResumeContextProjection {
                        agent_id: agent_id.max(0).cast_unsigned(),
                        bead_id: BeadId::new(bead_id),
                        status,
                        current_stage,
                        implementation_attempt: implementation_attempt.max(0).cast_unsigned(),
                        feedback,
                        attempts: Vec::new(),
                        artifacts: Vec::new(),
                    })
                },
            )
            .collect()
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
    pub async fn get_deep_resume_contexts(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<DeepResumeContextContract>> {
        self.get_resume_context_projections(repo_id)
            .await
            .map(|contexts| {
                contexts
                    .into_iter()
                    .map(|projection| DeepResumeContextContract {
                        agent_id: projection.agent_id,
                        bead_id: projection.bead_id.value().to_string(),
                        status: projection.status.as_str().to_string(),
                        current_stage: projection
                            .current_stage
                            .map(|stage| stage.as_str().to_string()),
                        implementation_attempt: projection.implementation_attempt,
                        feedback: projection.feedback,
                        attempts: projection
                            .attempts
                            .into_iter()
                            .map(|attempt| crate::ResumeStageAttemptContract {
                                stage: attempt.stage.as_str().to_string(),
                                attempt_number: attempt.attempt_number,
                                status: attempt.status,
                                feedback: attempt.feedback,
                                started_at: attempt.started_at,
                                completed_at: attempt.completed_at,
                            })
                            .collect::<Vec<_>>(),
                        diagnostics: None,
                        artifacts: Vec::new(),
                    })
                    .collect::<Vec<_>>()
            })
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
    pub async fn get_bead_artifacts(
        &self,
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: Option<ArtifactType>,
    ) -> Result<Vec<StageArtifact>> {
        artifact_type.map_or_else(
            || {
                sqlx::query_as::<
                    _,
                    (
                        i64,
                        i64,
                        String,
                        String,
                        Option<serde_json::Value>,
                        chrono::DateTime<chrono::Utc>,
                        Option<String>,
                    ),
                >(
                    "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
                     FROM stage_artifacts sa
                     JOIN stage_history sh ON sh.id = sa.stage_history_id
                     WHERE sh.repo_id = $1 AND sh.bead_id = $2
                     ORDER BY sa.created_at ASC, sa.id ASC",
                )
                .bind(repo_id.value())
                .bind(bead_id.value())
                .fetch_all(self.pool())
            },
            |kind| {
                sqlx::query_as::<
                    _,
                    (
                        i64,
                        i64,
                        String,
                        String,
                        Option<serde_json::Value>,
                        chrono::DateTime<chrono::Utc>,
                        Option<String>,
                    ),
                >(
                    "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
                     FROM stage_artifacts sa
                     JOIN stage_history sh ON sh.id = sa.stage_history_id
                     WHERE sh.repo_id = $1 AND sh.bead_id = $2 AND sa.artifact_type = $3
                     ORDER BY sa.created_at ASC, sa.id ASC",
                )
                .bind(repo_id.value())
                .bind(bead_id.value())
                .bind(kind.as_str())
                .fetch_all(self.pool())
            },
        )
        .await
        .map_err(|error| SwarmError::DatabaseError(format!("Failed to load bead artifacts: {error}")))?
        .into_iter()
        .map(
            |(id, stage_history_id, artifact_type, content, metadata, created_at, content_hash)| {
                let artifact_type = ArtifactType::try_from(artifact_type.as_str())
                    .map_err(SwarmError::DatabaseError)?;
                Ok(StageArtifact {
                    id,
                    stage_history_id,
                    artifact_type,
                    content,
                    metadata,
                    created_at,
                    content_hash,
                })
            },
        )
        .collect()
    }

    /// # Errors
    /// Returns [`SwarmError::DatabaseError`] when persistence fails.
    pub async fn claim_next_bead(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_bead($1, $2)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .fetch_one(self.pool())
            .await
            .map_err(|error| {
                SwarmError::DatabaseError(format!("Failed to claim next bead: {error}"))
            })
            .map(|value| value.map(BeadId::new))
    }
}
