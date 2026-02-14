use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{ArtifactType, BeadId, RepoId, StageArtifact};

impl SwarmDb {
    /// # Errors
    /// Returns an error if the database operation fails.
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
    /// Returns an error if the database operation fails.
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
    /// Returns an error if the database operation fails.
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
    /// Returns an error if the database operation fails.
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
    /// Returns an error if the database operation fails.
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
    /// Returns an error if the database operation fails.
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
}
