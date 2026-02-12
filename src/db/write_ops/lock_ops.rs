#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use sqlx::Acquire;

impl SwarmDb {
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

    pub async fn unlock_resource(&self, resource: &str, agent: &str) -> Result<bool> {
        sqlx::query("DELETE FROM resource_locks WHERE resource = $1 AND agent = $2")
            .bind(resource)
            .bind(agent)
            .execute(self.pool())
            .await
            .map(|result| result.rows_affected() > 0)
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to unlock resource: {e}")))
    }
}
