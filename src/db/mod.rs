mod mappers;
mod read_ops;
mod write_ops;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::info;

use crate::error::{Result, SwarmError};

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

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}
