use crate::db::mappers::{parse_agent_state, parse_swarm_config, to_u32_i32, AgentStateFields};
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{
    AgentId, AgentMessage, AgentState, AgentStatus, ArtifactType, AvailableAgent, BeadId,
    MessageType, ProgressSummary, RepoId, StageArtifact, SwarmConfig,
};
use sqlx::FromRow;

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

impl SwarmDb {
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
                        row.ms as u64,
                        row.error_code,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

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

    pub async fn get_agent_state(&self, agent_id: &AgentId) -> Result<Option<AgentState>> {
        sqlx::query_as::<_, AgentStateRow>(
            "SELECT bead_id, current_stage, stage_started_at, status, last_update, implementation_attempt, feedback
             FROM agent_state WHERE agent_id = $1",
        )
            .bind(agent_id.number() as i32)
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

    pub async fn get_progress(&self, _repo_id: &RepoId) -> Result<ProgressSummary> {
        sqlx::query_as::<_, ProgressRow>(
            "SELECT done_agents, working_agents, waiting_agents, error_agents, idle_agents, total_agents
             FROM v_swarm_progress",
        )
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get progress: {e}")))
            .map(|row| ProgressSummary {
                completed: row.done as u64,
                working: row.working as u64,
                waiting: row.waiting as u64,
                errors: row.error as u64,
                idle: row.idle as u64,
                total_agents: row.total as u64,
            })
    }

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
                    row.swarm_status,
                )
            })
    }

    pub async fn list_repos(&self) -> Result<Vec<(RepoId, String)>> {
        Ok(vec![(RepoId::new("local"), "local".to_string())])
    }

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

    pub async fn claim_next_bead(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        sqlx::query_scalar::<_, Option<String>>("SELECT claim_next_p0_bead($1)")
            .bind(agent_id.number() as i32)
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to claim next bead: {e}")))
            .map(|value| value.map(BeadId::new))
    }

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
        .bind(agent_id.number() as i32)
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
