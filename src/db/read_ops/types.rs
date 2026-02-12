use serde::Deserialize;
use sqlx::FromRow;

#[derive(FromRow)]
pub(crate) struct AgentStateRow {
    pub(crate) bead_id: Option<String>,
    pub(crate) current_stage: Option<String>,
    pub(crate) stage_started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) status: String,
    pub(crate) last_update: chrono::DateTime<chrono::Utc>,
    pub(crate) implementation_attempt: i32,
    pub(crate) feedback: Option<String>,
}

#[derive(FromRow)]
pub(crate) struct AvailableAgentRow {
    pub(crate) agent_id: i32,
    pub(crate) status: String,
    pub(crate) implementation_attempt: i32,
    pub(crate) max_implementation_attempts: i32,
    pub(crate) max_agents: i32,
}

#[derive(FromRow)]
pub(crate) struct ProgressRow {
    pub(crate) done: i64,
    pub(crate) working: i64,
    pub(crate) waiting: i64,
    pub(crate) error: i64,
    pub(crate) idle: i64,
    pub(crate) total: i64,
}

#[derive(FromRow)]
pub(crate) struct SwarmConfigRow {
    pub(crate) max_agents: i32,
    pub(crate) max_implementation_attempts: i32,
    pub(crate) claim_label: String,
    pub(crate) swarm_started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) swarm_status: String,
}

#[derive(FromRow)]
pub(crate) struct ActiveAgentRow {
    pub(crate) repo_id: String,
    pub(crate) agent_id: i32,
    pub(crate) bead_id: Option<String>,
    pub(crate) status: String,
}

#[derive(FromRow)]
pub(crate) struct ResumeContextAggregateRow {
    pub(crate) agent_id: i32,
    pub(crate) bead_id: String,
    pub(crate) current_stage: Option<String>,
    pub(crate) implementation_attempt: i32,
    pub(crate) feedback: Option<String>,
    pub(crate) status: String,
    pub(crate) attempts_json: serde_json::Value,
    pub(crate) artifacts_json: serde_json::Value,
}

#[derive(Deserialize)]
pub(crate) struct ResumeStageAttemptJson {
    pub(crate) stage: String,
    pub(crate) attempt_number: i32,
    pub(crate) status: String,
    pub(crate) feedback: Option<String>,
    pub(crate) started_at: chrono::DateTime<chrono::Utc>,
    pub(crate) completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Deserialize)]
pub(crate) struct ResumeArtifactSummaryJson {
    pub(crate) artifact_type: String,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
    pub(crate) content_hash: Option<String>,
    pub(crate) byte_length: i64,
}

#[derive(FromRow)]
pub(crate) struct StageArtifactRow {
    pub(crate) id: i64,
    pub(crate) stage_history_id: i64,
    pub(crate) artifact_type: String,
    pub(crate) content: String,
    pub(crate) metadata: Option<serde_json::Value>,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
    pub(crate) content_hash: Option<String>,
}

#[derive(FromRow)]
pub(crate) struct ResumeArtifactDetailRow {
    pub(crate) bead_id: String,
    pub(crate) artifact_type: String,
    pub(crate) content: String,
    pub(crate) metadata: Option<serde_json::Value>,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
    pub(crate) content_hash: Option<String>,
}

#[derive(FromRow)]
pub(crate) struct AgentMessageRow {
    pub(crate) id: i64,
    pub(crate) from_repo_id: String,
    pub(crate) from_agent_id: i32,
    pub(crate) to_repo_id: Option<String>,
    pub(crate) to_agent_id: Option<i32>,
    pub(crate) bead_id: Option<String>,
    pub(crate) message_type: String,
    pub(crate) subject: String,
    pub(crate) body: String,
    pub(crate) metadata: Option<serde_json::Value>,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
    pub(crate) read_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) read: bool,
}

#[derive(FromRow)]
pub(crate) struct CommandAuditRow {
    pub(crate) seq: i64,
    pub(crate) t: chrono::DateTime<chrono::Utc>,
    pub(crate) cmd: String,
    pub(crate) args: serde_json::Value,
    pub(crate) ok: bool,
    pub(crate) ms: i32,
    pub(crate) error_code: Option<String>,
}

#[derive(FromRow)]
pub(crate) struct ResourceLockRow {
    pub(crate) resource: String,
    pub(crate) agent: String,
    pub(crate) since: chrono::DateTime<chrono::Utc>,
    pub(crate) until_at: chrono::DateTime<chrono::Utc>,
}

#[derive(FromRow)]
pub(crate) struct ExecutionEventRow {
    pub(crate) seq: i64,
    pub(crate) schema_version: i32,
    pub(crate) event_type: String,
    pub(crate) entity_id: String,
    pub(crate) bead_id: Option<String>,
    pub(crate) agent_id: Option<i32>,
    pub(crate) stage: Option<String>,
    pub(crate) causation_id: Option<String>,
    pub(crate) diagnostics_category: Option<String>,
    pub(crate) diagnostics_retryable: Option<bool>,
    pub(crate) diagnostics_next_command: Option<String>,
    pub(crate) diagnostics_detail: Option<String>,
    pub(crate) payload: Option<serde_json::Value>,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
}
