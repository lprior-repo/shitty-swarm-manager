#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use sqlx::PgPool;

use crate::error::{Result, SwarmError};

pub struct LockRepository {
    pool: PgPool,
}

impl LockRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn acquire(
        &self,
        resource: &str,
        agent: &str,
        ttl_ms: i64,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
        sqlx::query("DELETE FROM resource_locks WHERE until_at <= NOW()")
            .execute(&self.pool)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to cleanup locks: {e}")))?;

        let acquired: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "INSERT INTO resource_locks (resource, agent, until_at)
             VALUES ($1, $2, NOW() + ($3 * INTERVAL '1 millisecond'))
             ON CONFLICT (resource) DO NOTHING
             RETURNING until_at",
        )
        .bind(resource)
        .bind(agent)
        .bind(ttl_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire lock: {e}")))?;

        Ok(acquired)
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn release(&self, resource: &str, agent: &str) -> Result<bool> {
        sqlx::query("DELETE FROM resource_locks WHERE resource = $1 AND agent = $2")
            .bind(resource)
            .bind(agent)
            .execute(&self.pool)
            .await
            .map(|result| result.rows_affected() > 0)
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to unlock resource: {e}")))
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn list_active(&self) -> Result<Vec<(String, String, i64, i64)>> {
        sqlx::query("DELETE FROM resource_locks WHERE until_at <= NOW()")
            .execute(&self.pool)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to cleanup locks: {e}")))?;

        sqlx::query_as::<
            _,
            (
                String,
                String,
                chrono::DateTime<chrono::Utc>,
                chrono::DateTime<chrono::Utc>,
            ),
        >("SELECT resource, agent, since, until_at FROM resource_locks ORDER BY since ASC")
        .fetch_all(&self.pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(resource, agent, since, until_at)| {
                    (
                        resource,
                        agent,
                        since.timestamp_millis(),
                        until_at.timestamp_millis(),
                    )
                })
                .collect()
        })
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to load resource locks: {e}")))
    }
}
