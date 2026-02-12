use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, SwarmError, CANONICAL_COORDINATOR_SCHEMA_PATH};
use serde_json::json;
use std::path::PathBuf;
use tokio::fs;
use tokio::process::Command;

pub const EMBEDDED_COORDINATOR_SCHEMA_SQL: &str = include_str!("../../schema.sql");
pub const EMBEDDED_COORDINATOR_SCHEMA_REF: &str = "embedded:crates/swarm-coordinator/schema.sql";

pub async fn current_repo_root() -> std::result::Result<PathBuf, Box<ProtocolEnvelope>> {
    Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| super::helpers::to_protocol_failure(e, None))
        .and_then(|output| {
            if output.status.success() {
                Ok(PathBuf::from(
                    String::from_utf8_lossy(&output.stdout).trim().to_string(),
                ))
            } else {
                Err(Box::new(
                    ProtocolEnvelope::error(
                        None,
                        code::INVALID.to_string(),
                        "Not in git repository".to_string(),
                    )
                    .with_fix("Run bootstrap from repository root".to_string()),
                ))
            }
        })
}

pub async fn load_schema_sql(
    rid: Option<String>,
    schema: Option<&str>,
) -> std::result::Result<(String, String), Box<ProtocolEnvelope>> {
    match schema {
        Some(path) => fs::read_to_string(path)
            .await
            .map(|sql| (sql, path.to_string()))
            .map_err(|err| {
                Box::new(
                    ProtocolEnvelope::error(
                        rid,
                        code::INVALID.to_string(),
                        format!("Failed to read schema: {err}"),
                    )
                    .with_fix(format!(
                        "Run from swarm repo root or pass --schema <path> (canonical: {CANONICAL_COORDINATOR_SCHEMA_PATH})"
                    ))
                    .with_ctx(json!({"schema": path})),
                )
            }),
        None => Ok((
            EMBEDDED_COORDINATOR_SCHEMA_SQL.to_string(),
            EMBEDDED_COORDINATOR_SCHEMA_REF.to_string(),
        )),
    }
}
