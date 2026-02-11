mod mappers;
mod read_ops;
mod write_ops;

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::info;

use crate::error::Result;

pub use crate::types::{AgentMessage, StageArtifact};

#[derive(Clone)]
pub struct SwarmDb {
    pool: PgPool,
}

impl SwarmDb {
    /// Creates a new `SwarmDb` instance connected to the database.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database connection fails.
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::new_with_timeout(database_url, None).await
    }

    /// Creates a new `SwarmDb` instance with explicit connection timeout.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the database connection fails
    /// or times out.
    pub async fn new_with_timeout(
        database_url: &str,
        connect_timeout_ms: Option<u64>,
    ) -> Result<Self> {
        let max_connections = resolve_pool_max_connections();

        let mut options = PgPoolOptions::new().max_connections(max_connections);

        if let Some(timeout_ms) = connect_timeout_ms {
            options = options.acquire_timeout(Duration::from_millis(timeout_ms));
        }

        let pool = options.connect(database_url).await?;

        info!("Connected to PostgreSQL swarm database");
        Ok(Self { pool })
    }

    /// Create a new `SwarmDb` with an existing pool (for testing).
    #[must_use]
    pub const fn new_with_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

fn resolve_pool_max_connections() -> u32 {
    resolve_pool_max_connections_from(|key| std::env::var(key).ok())
}

fn resolve_pool_max_connections_from<F>(env_lookup: F) -> u32
where
    F: Fn(&str) -> Option<String>,
{
    env_lookup("SWARM_DB_MAX_CONNECTIONS")
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or_else(|| {
            let agent_count = env_lookup("SWARM_MAX_AGENTS")
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(10);

            32_u32.max(agent_count.saturating_mul(3))
        })
}

#[cfg(test)]
mod tests {
    use super::resolve_pool_max_connections_from;
    use std::collections::HashMap;

    fn lookup(map: HashMap<String, String>) -> impl Fn(&str) -> Option<String> {
        move |key| map.get(key).cloned()
    }

    #[test]
    fn pool_size_defaults_to_three_x_agents_with_minimum_floor() {
        assert_eq!(
            resolve_pool_max_connections_from(lookup(HashMap::from([(
                "SWARM_MAX_AGENTS".to_string(),
                "8".to_string(),
            )]))),
            32
        );

        assert_eq!(
            resolve_pool_max_connections_from(lookup(HashMap::from([(
                "SWARM_MAX_AGENTS".to_string(),
                "15".to_string(),
            )]))),
            45
        );
    }

    #[test]
    fn explicit_pool_override_wins_over_computed_value() {
        assert_eq!(
            resolve_pool_max_connections_from(lookup(HashMap::from([
                ("SWARM_MAX_AGENTS".to_string(), "20".to_string()),
                ("SWARM_DB_MAX_CONNECTIONS".to_string(), "64".to_string()),
            ]))),
            64
        );
    }
}
