use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentMessage, BeadId, MessageType};

impl SwarmDb {
    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn get_all_unread_messages(&self) -> Result<Vec<AgentMessage>> {
        let rows = sqlx::query_as::<
            _,
            (
                i64,
                String,
                i32,
                Option<String>,
                Option<i32>,
                Option<String>,
                String,
                String,
                String,
                Option<serde_json::Value>,
                chrono::DateTime<chrono::Utc>,
                Option<chrono::DateTime<chrono::Utc>>,
                bool,
            ),
        >(
            "SELECT id, from_repo_id, from_agent_id, to_repo_id, to_agent_id, bead_id, message_type, subject, body, metadata, created_at, read_at, read
             FROM agent_messages
             WHERE read = FALSE
             ORDER BY created_at DESC",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|error| SwarmError::DatabaseError(format!("Failed to load unread messages: {error}")))?;

        rows.into_iter()
            .map(
                |(
                    id,
                    from_repo_id,
                    from_agent_id,
                    to_repo_id,
                    to_agent_id,
                    bead_id,
                    message_type,
                    subject,
                    body,
                    metadata,
                    created_at,
                    read_at,
                    read,
                )| {
                    let message_type = MessageType::try_from(message_type.as_str())
                        .map_err(SwarmError::DatabaseError)?;
                    Ok(AgentMessage {
                        id,
                        from_repo_id,
                        from_agent_id: from_agent_id.max(0).cast_unsigned(),
                        to_repo_id,
                        to_agent_id: to_agent_id.map(i32::cast_unsigned),
                        bead_id: bead_id.map(BeadId::new),
                        message_type,
                        subject,
                        body,
                        metadata,
                        created_at,
                        read_at,
                        read,
                    })
                },
            )
            .collect()
    }
}
