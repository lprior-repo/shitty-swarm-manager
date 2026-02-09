mod mappers;
mod read_ops;
mod write_ops;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::info;

use crate::error::{Result, SwarmError};

pub use crate::types::{AgentMessage, StageArtifact};

#[derive(Clone)]
pub struct SwarmDb {
    pool: PgPool,
}

impl SwarmDb {
    pub async fn new(database_url: &str) -> Result<Self> {
        let max_connections = std::env::var("SWARM_DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .map_or_else(|| 8, |v| v);

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to connect: {}", e)))?;

        info!("Connected to PostgreSQL swarm database");
        Ok(Self { pool })
    }

    /// Create a new SwarmDb with an existing pool (for testing).
    pub fn new_with_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}
