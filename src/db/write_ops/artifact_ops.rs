#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::ArtifactType;

impl SwarmDb {
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
}
