use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;

use crate::types::artifacts::{ArtifactType, StageArtifact};

#[derive(Debug, Serialize, Deserialize)]
pub struct ArtifactRetrievalRequest {
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

pub async fn artifact_retrieval(
    pool: &PgPool, 
    request: ArtifactRetrievalRequest
) -> Result<Vec<StageArtifact>, ArtifactRetrievalError> {
    // Use SQL to fetch artifacts based on bead_id and optional artifact_type
    let artifacts = match &request.artifact_type {
        Some(filter_type) => {
            sqlx::query_as!(
                StageArtifact,
                r#"SELECT 
                    id, 
                    stage_history_id, 
                    artifact_type AS "artifact_type: ArtifactType", 
                    content, 
                    metadata, 
                    created_at, 
                    content_hash
                FROM stage_artifacts sa
                JOIN stage_history sh ON sa.stage_history_id = sh.id
                WHERE sh.bead_id = $1 AND artifact_type = $2
                ORDER BY created_at ASC"#,
                request.bead_id,
                filter_type.as_str()
            )
            .fetch_all(pool)
            .await?
        },
        None => {
            sqlx::query_as!(
                StageArtifact,
                r#"SELECT 
                    id, 
                    stage_history_id, 
                    artifact_type AS "artifact_type: ArtifactType", 
                    content, 
                    metadata, 
                    created_at, 
                    content_hash
                FROM stage_artifacts sa
                JOIN stage_history sh ON sa.stage_history_id = sh.id
                WHERE sh.bead_id = $1
                ORDER BY created_at ASC"#,
                request.bead_id
            )
            .fetch_all(pool)
            .await?
        }
    };
    
    if artifacts.is_empty() {
        return Err(ArtifactRetrievalError::NoArtifactsFound(request.bead_id));
    }
    
    Ok(artifacts)
}