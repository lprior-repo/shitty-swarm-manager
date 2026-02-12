use crate::db::mappers::to_u32_i32;
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{ArtifactType, BeadId, RepoId, StageArtifact};

use super::types::StageArtifactRow;

impl SwarmDb {
    pub async fn get_stage_artifacts(
        &self,
        repo_id: &RepoId,
        stage_history_id: i64,
    ) -> Result<Vec<StageArtifact>> {
        sqlx::query_as::<_, StageArtifactRow>(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sa.stage_history_id = sh.id
             JOIN bead_claims bc ON bc.bead_id = sh.bead_id
             WHERE sa.stage_history_id = $1 AND bc.repo_id = $2
             ORDER BY sa.created_at ASC",
        )
        .bind(stage_history_id)
        .bind(repo_id.value())
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
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<Vec<StageArtifact>> {
        sqlx::query_as::<_, StageArtifactRow>(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sa.stage_history_id = sh.id
             JOIN bead_claims bc ON bc.bead_id = sh.bead_id
             WHERE sh.bead_id = $1 AND bc.repo_id = $2 AND sa.artifact_type = $3
             ORDER BY sa.created_at ASC",
        )
        .bind(bead_id.value())
        .bind(repo_id.value())
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

    pub async fn get_bead_artifacts(
        &self,
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: Option<ArtifactType>,
    ) -> Result<Vec<StageArtifact>> {
        if let Some(kind) = artifact_type {
            return self
                .get_bead_artifacts_by_type(repo_id, bead_id, kind)
                .await;
        }

        sqlx::query_as::<_, StageArtifactRow>(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sa.stage_history_id = sh.id
             JOIN bead_claims bc ON bc.bead_id = sh.bead_id
             WHERE sh.bead_id = $1 AND bc.repo_id = $2
             ORDER BY sa.created_at ASC, sa.id ASC",
        )
        .bind(bead_id.value())
        .bind(repo_id.value())
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

    pub async fn get_latest_bead_artifact_by_type(
        &self,
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<Option<StageArtifact>> {
        sqlx::query_as::<_, StageArtifactRow>(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sa.stage_history_id = sh.id
             JOIN bead_claims bc ON bc.bead_id = sh.bead_id
             WHERE sh.bead_id = $1 AND bc.repo_id = $2 AND sa.artifact_type = $3
             ORDER BY sa.created_at DESC, sa.id DESC
             LIMIT 1",
        )
        .bind(bead_id.value())
        .bind(repo_id.value())
        .bind(artifact_type.as_str())
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to get latest bead artifact by type: {e}"))
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

    pub async fn get_first_bead_artifact_by_type(
        &self,
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<Option<StageArtifact>> {
        sqlx::query_as::<_, StageArtifactRow>(
            "SELECT sa.id, sa.stage_history_id, sa.artifact_type, sa.content, sa.metadata, sa.created_at, sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sa.stage_history_id = sh.id
             JOIN bead_claims bc ON bc.bead_id = sh.bead_id
             WHERE sh.bead_id = $1 AND bc.repo_id = $2 AND sa.artifact_type = $3
             ORDER BY sa.created_at ASC
             LIMIT 1",
        )
        .bind(bead_id.value())
        .bind(repo_id.value())
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
        repo_id: &RepoId,
        bead_id: &BeadId,
        artifact_type: ArtifactType,
    ) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM stage_artifacts sa
                 JOIN stage_history sh ON sa.stage_history_id = sh.id
                 JOIN bead_claims bc ON bc.bead_id = sh.bead_id
                 WHERE sh.bead_id = $1 AND bc.repo_id = $2 AND sa.artifact_type = $3
             )",
        )
        .bind(bead_id.value())
        .bind(repo_id.value())
        .bind(artifact_type.as_str())
        .fetch_one(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to check bead artifact existence: {e}"))
        })
    }
}
