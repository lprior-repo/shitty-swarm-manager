#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};

impl SwarmDb {
    pub async fn record_command_audit(
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
        .bind(ms.cast_signed())
        .bind(error_code)
        .execute(self.pool())
        .await
        .map(|_result| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to write command audit: {e}")))
    }
}
