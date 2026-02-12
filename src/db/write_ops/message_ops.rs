#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, BeadId, MessageType};

impl SwarmDb {
    pub async fn write_broadcast(&self, from_agent: &str, msg: &str) -> Result<i64> {
        sqlx::query("DELETE FROM resource_locks WHERE until_at <= NOW()")
            .execute(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to cleanup locks: {e}")))?;

        sqlx::query("INSERT INTO broadcast_log (from_agent, msg) VALUES ($1, $2)")
            .bind(from_agent)
            .bind(msg)
            .execute(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to write broadcast: {e}")))?;

        sqlx::query_scalar::<_, i64>("SELECT COUNT(DISTINCT agent) FROM resource_locks")
            .fetch_one(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to count agents: {e}")))
    }

    pub async fn send_agent_message(
        &self,
        from_agent: &AgentId,
        to_agent: Option<&AgentId>,
        bead_id: Option<&BeadId>,
        message_type: MessageType,
        content: (&str, &str),
        metadata: Option<serde_json::Value>,
    ) -> Result<i64> {
        let (subject, body) = content;
        let to_repo_id = to_agent.map(|agent| agent.repo_id().value().to_string());
        let to_agent_id = to_agent.map(|agent| agent.number().cast_signed());

        sqlx::query_scalar::<_, i64>(
            "SELECT send_agent_message($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(from_agent.repo_id().value())
        .bind(from_agent.number().cast_signed())
        .bind(to_repo_id)
        .bind(to_agent_id)
        .bind(bead_id.map(BeadId::value))
        .bind(message_type.as_str())
        .bind(subject)
        .bind(body)
        .bind(metadata)
        .fetch_one(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to send agent message: {e}")))
    }

    pub async fn mark_messages_read(&self, agent_id: &AgentId, message_ids: &[i64]) -> Result<()> {
        if message_ids.is_empty() {
            return Ok(());
        }

        sqlx::query("SELECT mark_messages_read($1, $2, $3)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number().cast_signed())
            .bind(message_ids)
            .execute(self.pool())
            .await
            .map(|_result| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to mark messages read: {e}")))
    }
}
