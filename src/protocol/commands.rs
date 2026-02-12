use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use thiserror::Error;

use crate::protocol_runtime::ProtocolRequest;
use crate::{ArtifactType, StageArtifact};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorInput {
    pub json: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelpInput {
    pub short: Option<bool>,
    pub s: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusInput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInput {
    pub id: u32,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitInput {
    pub dry: Option<bool>,
    pub database_url: Option<String>,
    pub schema: Option<String>,
    pub seed_agents: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterInput {
    pub count: Option<u32>,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseInput {
    pub agent_id: u32,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorInput {
    pub view: Option<String>,
    pub watch_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitDbInput {
    pub url: Option<String>,
    pub schema: Option<String>,
    pub seed_agents: Option<u32>,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitLocalDbInput {
    pub container_name: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub seed_agents: Option<u32>,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapInput {
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpawnPromptsInput {
    pub template: Option<String>,
    pub out_dir: Option<String>,
    pub count: Option<u32>,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptInput {
    pub id: u32,
    pub skill: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmokeInput {
    pub id: u32,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchInput {
    pub ops: Vec<serde_json::Value>,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateInput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryInput {
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockInput {
    pub resource: String,
    pub agent: String,
    pub ttl_ms: i64,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnlockInput {
    pub resource: String,
    pub agent: String,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentsInput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastInput {
    pub msg: String,
    pub from: String,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadProfileInput {
    pub agents: Option<u32>,
    pub rounds: Option<u32>,
    pub timeout_ms: Option<u64>,
    pub dry: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRetrievalRequest {
    pub repo_id: String,
    pub bead_id: String,
    pub artifact_type: Option<ArtifactType>,
}

#[derive(Debug, Error)]
pub enum ArtifactRetrievalError {
    #[error("Invalid artifact type: {0}")]
    InvalidArtifactType(String),

    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("No artifacts found for bead {0}")]
    NoArtifactsFound(String),
}

/// Retrieves artifacts for a specific bead and repository
///
/// # Errors
/// Returns `ArtifactRetrievalError` if:
/// - Database query fails
/// - Artifact type parsing fails
/// - No artifacts found (not an error, but results in empty vector)
pub async fn artifact_retrieval(
    pool: &PgPool,
    request: ArtifactRetrievalRequest,
) -> Result<Vec<StageArtifact>, ArtifactRetrievalError> {
    let artifacts = match &request.artifact_type {
        Some(filter_type) => sqlx::query(
            r"SELECT
                    id,
                    stage_history_id,
                    artifact_type,
                    content,
                    metadata,
                    created_at,
                    content_hash
                FROM stage_artifacts sa
                JOIN stage_history sh ON sa.stage_history_id = sh.id
                WHERE sh.repo_id = $1 AND sh.bead_id = $2 AND artifact_type = $3
                ORDER BY created_at ASC",
        )
        .bind(&request.repo_id)
        .bind(&request.bead_id)
        .bind(filter_type.as_str())
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(
            |row: sqlx::postgres::PgRow| -> Result<StageArtifact, ArtifactRetrievalError> {
                Ok(StageArtifact {
                    id: row.get("id"),
                    stage_history_id: row.get("stage_history_id"),
                    artifact_type: row
                        .get::<&str, _>("artifact_type")
                        .try_into()
                        .map_err(ArtifactRetrievalError::InvalidArtifactType)?,
                    content: row.get("content"),
                    metadata: row.get("metadata"),
                    created_at: row.get("created_at"),
                    content_hash: row.get("content_hash"),
                })
            },
        )
        .collect::<Result<Vec<_>, _>>()?,
        None => sqlx::query(
            r"SELECT
                    id,
                    stage_history_id,
                    artifact_type,
                    content,
                    metadata,
                    created_at,
                    content_hash
                FROM stage_artifacts sa
                JOIN stage_history sh ON sa.stage_history_id = sh.id
                WHERE sh.repo_id = $1 AND sh.bead_id = $2
                ORDER BY created_at ASC",
        )
        .bind(&request.repo_id)
        .bind(&request.bead_id)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(
            |row: sqlx::postgres::PgRow| -> Result<StageArtifact, ArtifactRetrievalError> {
                Ok(StageArtifact {
                    id: row.get("id"),
                    stage_history_id: row.get("stage_history_id"),
                    artifact_type: row
                        .get::<&str, _>("artifact_type")
                        .try_into()
                        .map_err(ArtifactRetrievalError::InvalidArtifactType)?,
                    content: row.get("content"),
                    metadata: row.get("metadata"),
                    created_at: row.get("created_at"),
                    content_hash: row.get("content_hash"),
                })
            },
        )
        .collect::<Result<Vec<_>, _>>()?,
    };

    if artifacts.is_empty() {
        return Err(ArtifactRetrievalError::NoArtifactsFound(request.bead_id));
    }

    Ok(artifacts)
}

impl HistoryInput {
    /// Parses a protocol request into `HistoryInput`
    ///
    /// # Errors
    /// Returns `String` error if:
    /// - Request parsing fails
    /// - Required fields are missing or invalid
    pub fn parse_input(request: &ProtocolRequest) -> Result<Self, String> {
        Ok(Self {
            limit: request
                .args
                .get("limit")
                .and_then(serde_json::Value::as_i64),
        })
    }
}
