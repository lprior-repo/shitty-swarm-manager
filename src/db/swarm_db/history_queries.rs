use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{ExecutionEvent, RepoId};

impl SwarmDb {
    pub async fn get_command_history(
        &self,
        limit: i64,
    ) -> Result<
        Vec<(
            i64,
            i64,
            String,
            serde_json::Value,
            bool,
            u64,
            Option<String>,
        )>,
    > {
        sqlx::query_as::<
            _,
            (
                i64,
                chrono::DateTime<chrono::Utc>,
                String,
                serde_json::Value,
                bool,
                i64,
                Option<String>,
            ),
        >(
            "SELECT seq, t, cmd, args, ok, ms, error_code
             FROM command_audit
             ORDER BY seq DESC
             LIMIT $1",
        )
        .bind(limit.max(0))
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load command history: {error}"))
        })
        .map(|rows| {
            rows.into_iter()
                .map(|(seq, t, cmd, args, ok, ms, error_code)| {
                    (
                        seq,
                        t.timestamp_millis(),
                        cmd,
                        args,
                        ok,
                        ms.max(0).cast_unsigned(),
                        error_code,
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    pub async fn list_active_resource_locks(&self) -> Result<Vec<(String, String, i64, i64)>> {
        sqlx::query_as::<
            _,
            (
                String,
                String,
                chrono::DateTime<chrono::Utc>,
                chrono::DateTime<chrono::Utc>,
            ),
        >(
            "SELECT resource, agent, until_at, until_at
             FROM resource_locks
             WHERE until_at > NOW()
             ORDER BY resource ASC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!("Failed to load active resource locks: {error}"))
        })
        .map(|rows| {
            rows.into_iter()
                .map(|(resource, agent, until_at, expires_at)| {
                    (
                        resource,
                        agent,
                        until_at.timestamp(),
                        expires_at.timestamp(),
                    )
                })
                .collect::<Vec<_>>()
        })
    }

    pub async fn get_execution_events(
        &self,
        repo_id: &RepoId,
        bead_filter: Option<&str>,
        limit: i64,
    ) -> Result<Vec<ExecutionEvent>> {
        let rows = sqlx::query_as::<
            _,
            (
                i64,
                i32,
                String,
                String,
                Option<String>,
                Option<i32>,
                Option<String>,
                Option<String>,
                Option<serde_json::Value>,
                Option<serde_json::Value>,
                chrono::DateTime<chrono::Utc>,
            ),
        >(
            "SELECT seq, schema_version, event_type, entity_id, bead_id, agent_id, stage, causation_id, diagnostics, payload, created_at
             FROM execution_events
             WHERE repo_id = $1 AND ($2::text IS NULL OR bead_id = $2)
             ORDER BY seq DESC
             LIMIT $3",
        )
        .bind(repo_id.value())
        .bind(bead_filter)
        .bind(limit.max(1))
        .fetch_all(self.pool())
        .await
        .map_err(|error| SwarmError::DatabaseError(format!("Failed to load execution events: {error}")))?;

        rows.into_iter()
            .map(
                |(
                    seq,
                    schema_version,
                    event_type,
                    entity_id,
                    bead_id,
                    agent_id,
                    stage,
                    causation_id,
                    diagnostics,
                    payload,
                    created_at,
                )| {
                    let diagnostics = diagnostics
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|error| {
                            SwarmError::DatabaseError(format!(
                                "Failed to decode diagnostics payload: {error}"
                            ))
                        })?;
                    Ok(ExecutionEvent {
                        seq,
                        schema_version,
                        event_type,
                        entity_id,
                        bead_id,
                        agent_id: agent_id.map(i32::cast_unsigned),
                        stage,
                        causation_id,
                        diagnostics,
                        payload,
                        created_at,
                    })
                },
            )
            .collect()
    }
}
