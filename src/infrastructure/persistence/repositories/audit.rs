#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use sqlx::PgPool;

use crate::error::{Result, SwarmError};

pub struct AuditRepository {
    pool: PgPool,
}

impl AuditRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn record_command(
        &self,
        cmd: &str,
        rid: Option<&str>,
        args: serde_json::Value,
        ok: bool,
        ms: u64,
        error_code: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO command_audit (cmd, rid, args, ok, ms, error_code)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(cmd)
        .bind(rid)
        .bind(args)
        .bind(ok)
        .bind(ms as i64)
        .bind(error_code)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to write command audit: {e}")))
    }
}
