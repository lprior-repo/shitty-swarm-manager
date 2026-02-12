use crate::db::mappers::to_u32_i32;
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{AgentId, AgentMessage, BeadId, MessageType};

use super::types::AgentMessageRow;

impl SwarmDb {
    pub async fn get_unread_messages(
        &self,
        agent_id: &AgentId,
        bead_id: Option<&BeadId>,
    ) -> Result<Vec<AgentMessage>> {
        sqlx::query_as::<_, AgentMessageRow>(
            "SELECT id, from_repo_id, from_agent_id, to_repo_id, to_agent_id, bead_id, message_type,
                    subject, body, metadata, created_at, read_at, read
             FROM get_unread_messages($1, $2, $3)",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number() as i32)
        .bind(bead_id.map(BeadId::value))
        .fetch_all(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get unread messages: {e}")))
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    MessageType::try_from(row.message_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|message_type| AgentMessage {
                            id: row.id,
                            from_repo_id: row.from_repo_id,
                            from_agent_id: to_u32_i32(row.from_agent_id),
                            to_repo_id: row.to_repo_id,
                            to_agent_id: row.to_agent_id.map(to_u32_i32),
                            bead_id: row.bead_id.map(BeadId::new),
                            message_type,
                            subject: row.subject,
                            body: row.body,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            read_at: row.read_at,
                            read: row.read,
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
    }

    pub async fn get_all_unread_messages(&self) -> Result<Vec<AgentMessage>> {
        sqlx::query_as::<_, AgentMessageRow>(
            "SELECT id, from_repo_id, from_agent_id, to_repo_id, to_agent_id, bead_id, message_type,
                    subject, body, metadata, created_at, read_at, read
             FROM v_unread_messages",
        )
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to get all unread messages: {e}"))
        })
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    MessageType::try_from(row.message_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|message_type| AgentMessage {
                            id: row.id,
                            from_repo_id: row.from_repo_id,
                            from_agent_id: to_u32_i32(row.from_agent_id),
                            to_repo_id: row.to_repo_id,
                            to_agent_id: row.to_agent_id.map(to_u32_i32),
                            bead_id: row.bead_id.map(BeadId::new),
                            message_type,
                            subject: row.subject,
                            body: row.body,
                            metadata: row.metadata,
                            created_at: row.created_at,
                            read_at: row.read_at,
                            read: row.read,
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
    }
}
