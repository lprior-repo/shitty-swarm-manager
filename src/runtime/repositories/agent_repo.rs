#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::runtime::agent::{AgentState, AgentStatus};
use crate::runtime::shared::{RuntimeAgentId, RuntimeBeadId, RuntimeError};
use sqlx::PgPool;

pub struct RuntimePgAgentRepository {
    pool: PgPool,
}

impl RuntimePgAgentRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// # Errors
    /// Returns an error if the database operation fails or data is invalid.
    pub async fn find_by_id(
        &self,
        agent_id: &RuntimeAgentId,
    ) -> crate::runtime::shared::Result<Option<AgentState>> {
        let maybe_row = sqlx::query_as::<_, (Option<String>, Option<String>, String, i32)>(
            "SELECT bead_id, current_stage, status, implementation_attempt FROM agent_state WHERE repo_id = $1 AND agent_id = $2",
        )
        .bind(agent_id.repo_id().value())
        .bind(agent_id.number().cast_signed())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| RuntimeError::RepositoryError(format!("find_agent: {e}")))?;

        maybe_row.map_or(
            Ok(None),
            |(bead_id, current_stage, status, impl_attempt)| {
                let parsed_stage = current_stage
                    .map(|stage| {
                        stage.as_str().try_into().map_err(|err: String| {
                            RuntimeError::RepositoryError(format!(
                                "find_agent invalid stage '{stage}': {err}"
                            ))
                        })
                    })
                    .transpose()?;

                let parsed_status = status.as_str().try_into().map_err(|err: String| {
                    RuntimeError::RepositoryError(format!(
                        "find_agent invalid status '{status}': {err}"
                    ))
                })?;

                let implementation_attempt = if impl_attempt < 0 {
                    0
                } else {
                    impl_attempt.cast_unsigned()
                };

                let state = AgentState::new(
                    agent_id.clone(),
                    bead_id.map(RuntimeBeadId::new),
                    parsed_stage,
                    parsed_status,
                    implementation_attempt,
                );

                state.validate_invariants()?;
                Ok(Some(state))
            },
        )
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn update_status(
        &self,
        agent_id: &RuntimeAgentId,
        status: AgentStatus,
    ) -> crate::runtime::shared::Result<()> {
        sqlx::query("UPDATE agent_state SET status = $2, last_update = NOW() WHERE repo_id = $1 AND agent_id = $3")
            .bind(agent_id.repo_id().value())
            .bind(status.as_str())
            .bind(agent_id.number().cast_signed())
            .execute(&self.pool)
            .await
            .map_err(|e| RuntimeError::RepositoryError(format!("update_status: {e}")))
            .map(|_| ())
    }
}
