use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::{Result, SwarmError};

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
    /// Returns an error if the database connection fails.
    pub async fn new(connection_string: &str) -> Result<Self> {
        Self::new_with_timeout(connection_string, None).await
    }

    /// # Errors
    /// Returns an error if the database connection fails.
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
        let key = (table_name.to_string(), column_name.to_string());
        cache.get(&key).copied()
    }

    pub fn update_schema_cache(&self, table_name: &str, column_name: &str, value: bool) {
        if let Ok(mut cache) = self.schema_cache.lock() {
            let key = (table_name.to_string(), column_name.to_string());
            cache.insert(key, value);
        }
    }

    pub(crate) async fn table_has_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<bool> {
        if let Some(cached) = self.check_schema_cache(table_name, column_name) {
            return Ok(cached);
        }

        let result = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = $1
                  AND column_name = $2
            )",
        )
        .bind(table_name)
        .bind(column_name)
        .fetch_one(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!(
                "Failed to inspect schema for {table_name}.{column_name}: {e}"
            ))
        })?;

        self.update_schema_cache(table_name, column_name, result);
        Ok(result)
    }
}
