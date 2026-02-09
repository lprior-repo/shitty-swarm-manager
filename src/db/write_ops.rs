use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{
    AgentId, ArtifactType, BeadId, MessageType, RepoId, Stage, StageResult, SwarmStatus,
};
use sqlx::Acquire;
use std::future::Future;
use std::pin::Pin;
use tracing::{debug, info};

#[derive(Debug, Clone, PartialEq, Eq)]
enum StageTransition {
    Finalize,
    Advance(Stage),
    RetryImplement,
    NoOp,
}

impl SwarmDb {
    pub async fn register_repo(&self, _repo_id: &RepoId, _name: &str, _path: &str) -> Result<()> {
        info!("Repository registration skipped for single-repo coordinator");
        Ok(())
    }

    pub async fn register_agent(&self, agent_id: &AgentId) -> Result<bool> {
        sqlx::query(
            "INSERT INTO agent_state (agent_id, status) VALUES ($1, 'idle')
             ON CONFLICT (agent_id) DO NOTHING",
        )
        .bind(agent_id.number() as i32)
        .execute(self.pool())
        .await
        .map(|rows| rows.rows_affected() > 0)
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to register agent: {}", e)))
    }

    pub async fn claim_bead(&self, agent_id: &AgentId, bead_id: &BeadId) -> Result<bool> {
        self.claim_next_bead(agent_id)
            .await
            .map(|claimed| claimed.as_ref().map(BeadId::value) == Some(bead_id.value()))
    }

    pub async fn record_stage_started(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
    ) -> Result<i64> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {}", e)))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {}", e)))?;

        let stage_history_id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO stage_history (agent_id, bead_id, stage, attempt_number, status)
             VALUES ($1, $2, $3, $4, 'started')
             RETURNING id",
        )
        .bind(agent_id.number() as i32)
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt as i32)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to record stage start: {}", e)))?;

        sqlx::query(
            "UPDATE agent_state
             SET current_stage = $2, stage_started_at = NOW(), status = 'working'
             WHERE agent_id = $1",
        )
        .bind(agent_id.number() as i32)
        .bind(stage.as_str())
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update stage start: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {}", e)))
            .map(|_| stage_history_id)
    }

    pub async fn record_stage_complete(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
        result: StageResult,
        duration_ms: u64,
    ) -> Result<()> {
        let status = result.as_str();
        let message = result.message();

        sqlx::query(
            "UPDATE stage_history
             SET status = $5, result = $6, feedback = $7, completed_at = NOW(), duration_ms = $8
             WHERE id = (
                SELECT id FROM stage_history
                WHERE agent_id = $1 AND bead_id = $2 AND stage = $3 AND attempt_number = $4 AND status = 'started'
                ORDER BY started_at DESC LIMIT 1
             )",
        )
        .bind(agent_id.number() as i32)
        .bind(bead_id.value())
        .bind(stage.as_str())
        .bind(attempt as i32)
        .bind(status)
        .bind(message)
        .bind(message)
        .bind(duration_ms as i32)
        .execute(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update stage history: {}", e)))?;

        self.apply_stage_transition(
            &determine_transition(stage, &result),
            agent_id,
            bead_id,
            message,
        )
        .await?;

        debug!(
            "Agent {} completed stage {} for bead {}: {:?}",
            agent_id, stage, bead_id, result
        );
        Ok(())
    }

    pub async fn store_stage_artifact(
        &self,
        stage_history_id: i64,
        artifact_type: ArtifactType,
        content: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<i64> {
        sqlx::query_scalar::<_, i64>("SELECT store_stage_artifact($1, $2, $3, $4)")
            .bind(stage_history_id)
            .bind(artifact_type.as_str())
            .bind(content)
            .bind(metadata)
            .fetch_one(self.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to store stage artifact: {}", e))
            })
    }

    pub async fn send_agent_message(
        &self,
        from_agent: &AgentId,
        to_agent: Option<&AgentId>,
        bead_id: Option<&BeadId>,
        message_type: MessageType,
        subject: &str,
        body: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<i64> {
        let to_repo_id = to_agent.map(|agent| agent.repo_id().value().to_string());
        let to_agent_id = to_agent.map(|agent| agent.number() as i32);

        sqlx::query_scalar::<_, i64>(
            "SELECT send_agent_message($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(from_agent.repo_id().value())
        .bind(from_agent.number() as i32)
        .bind(to_repo_id)
        .bind(to_agent_id)
        .bind(bead_id.map(BeadId::value))
        .bind(message_type.as_str())
        .bind(subject)
        .bind(body)
        .bind(metadata)
        .fetch_one(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to send agent message: {}", e)))
    }

    pub async fn mark_messages_read(&self, agent_id: &AgentId, message_ids: &[i64]) -> Result<()> {
        if message_ids.is_empty() {
            return Ok(());
        }

        sqlx::query("SELECT mark_messages_read($1, $2, $3)")
            .bind(agent_id.repo_id().value())
            .bind(agent_id.number() as i32)
            .bind(message_ids)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to mark messages read: {}", e)))
    }

    async fn apply_stage_transition(
        &self,
        transition: &StageTransition,
        agent_id: &AgentId,
        bead_id: &BeadId,
        message: Option<&str>,
    ) -> Result<()> {
        match transition {
            StageTransition::Finalize => self.finalize_agent_and_bead(agent_id, bead_id).await,
            StageTransition::Advance(next_stage) => {
                self.advance_to_stage(agent_id, *next_stage).await
            }
            StageTransition::RetryImplement => {
                self.apply_failure_transition(agent_id, message).await
            }
            StageTransition::NoOp => Ok(()),
        }
    }

    pub async fn set_swarm_status(&self, _repo_id: &RepoId, status: SwarmStatus) -> Result<()> {
        sqlx::query("UPDATE swarm_config SET swarm_status = $1 WHERE id = TRUE")
            .bind(status.as_str())
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to update status: {}", e)))
    }

    pub async fn start_swarm(&self, _repo_id: &RepoId) -> Result<()> {
        sqlx::query("UPDATE swarm_config SET swarm_status = 'running', swarm_started_at = NOW() WHERE id = TRUE")
            .execute(self.pool())
            .await
            .map(|_| info!("Started swarm"))
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to start swarm: {}", e)))
    }

    pub async fn initialize_schema_from_sql(&self, schema_sql: &str) -> Result<()> {
        sqlx::raw_sql(schema_sql)
            .execute(self.pool())
            .await
            .map(|_| ())
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to initialize schema: {}", e)))
    }

    pub async fn seed_idle_agents(&self, count: u32) -> Result<()> {
        seed_idle_agents_recursive(self, 1, count).await
    }

    pub async fn enqueue_backlog_batch(&self, prefix: &str, count: u32) -> Result<()> {
        sqlx::query(
            "INSERT INTO bead_backlog (bead_id, priority, status)
             SELECT format('%s-%s', $1, g), 'p0', 'pending'
             FROM generate_series(1, $2) AS g",
        )
        .bind(prefix)
        .bind(count as i32)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to enqueue backlog batch: {}", e)))
    }

    pub async fn mark_bead_blocked(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        reason: &str,
    ) -> Result<()> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {}", e)))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {}", e)))?;

        sqlx::query("UPDATE bead_claims SET status = 'blocked' WHERE bead_id = $1")
            .bind(bead_id.value())
            .execute(&mut *conn)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to block claim: {}", e)))?;

        sqlx::query("UPDATE bead_backlog SET status = 'blocked' WHERE bead_id = $1")
            .bind(bead_id.value())
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to block backlog bead: {}", e))
            })?;

        sqlx::query("UPDATE agent_state SET status = 'error', feedback = $2 WHERE agent_id = $1")
            .bind(agent_id.number() as i32)
            .bind(reason)
            .execute(&mut *conn)
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to mark agent error: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {}", e)))
    }

    pub async fn release_agent(&self, agent_id: &AgentId) -> Result<Option<BeadId>> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to begin tx: {}", e)))?;

        let conn = tx
            .acquire()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to acquire tx conn: {}", e)))?;

        let bead = sqlx::query_scalar::<_, Option<String>>(
            "SELECT bead_id FROM agent_state WHERE agent_id = $1 FOR UPDATE",
        )
        .bind(agent_id.number() as i32)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to read agent state: {}", e)))?
        .flatten();

        if let Some(bead_id) = bead.as_deref() {
            sqlx::query("DELETE FROM bead_claims WHERE bead_id = $1")
                .bind(bead_id)
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    SwarmError::DatabaseError(format!(
                        "Failed to clear bead claim on release: {}",
                        e
                    ))
                })?;

            sqlx::query(
                "UPDATE bead_backlog
                 SET status = 'pending'
                 WHERE bead_id = $1 AND status <> 'completed'",
            )
            .bind(bead_id)
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!(
                    "Failed to reset backlog status on release: {}",
                    e
                ))
            })?;
        }

        sqlx::query(
            "UPDATE agent_state
             SET bead_id = NULL,
                 current_stage = NULL,
                 stage_started_at = NULL,
                 status = 'idle',
                 feedback = NULL,
                 implementation_attempt = 0
             WHERE agent_id = $1",
        )
        .bind(agent_id.number() as i32)
        .execute(&mut *conn)
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to reset agent state: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to commit tx: {}", e)))?;

        Ok(bead.map(BeadId::new))
    }

    async fn finalize_agent_and_bead(&self, agent_id: &AgentId, bead_id: &BeadId) -> Result<()> {
        sqlx::query(
            "UPDATE agent_state SET status = 'done', current_stage = 'done' WHERE agent_id = $1",
        )
        .bind(agent_id.number() as i32)
        .execute(self.pool())
        .await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to finalize agent: {}", e)))?;

        sqlx::query("UPDATE bead_claims SET status = 'completed' WHERE bead_id = $1")
            .bind(bead_id.value())
            .execute(self.pool())
            .await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to finalize bead: {}", e)))
            .map(|_| ())
    }

    async fn advance_to_stage(&self, agent_id: &AgentId, next_stage: Stage) -> Result<()> {
        sqlx::query(
            "UPDATE agent_state
             SET current_stage = $2, stage_started_at = NOW(), status = 'working'
             WHERE agent_id = $1",
        )
        .bind(agent_id.number() as i32)
        .bind(next_stage.as_str())
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to advance stage: {}", e)))
    }

    async fn apply_failure_transition(
        &self,
        agent_id: &AgentId,
        message: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE agent_state
             SET status = 'waiting', feedback = $2, implementation_attempt = implementation_attempt + 1, current_stage = 'implement'
             WHERE agent_id = $1",
        )
        .bind(agent_id.number() as i32)
        .bind(message)
        .execute(self.pool())
        .await
        .map(|_| ())
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to record failed stage: {}", e)))
    }
}

fn seed_idle_agents_recursive<'a>(
    db: &'a SwarmDb,
    next: u32,
    count: u32,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if next > count {
            Ok(())
        } else {
            sqlx::query(
                "INSERT INTO agent_state (agent_id, status)
                 VALUES ($1, 'idle')
                 ON CONFLICT (agent_id) DO UPDATE
                 SET status = 'idle', bead_id = NULL, current_stage = NULL, stage_started_at = NULL,
                     feedback = NULL, implementation_attempt = 0",
            )
            .bind(next as i32)
            .execute(db.pool())
            .await
            .map_err(|e| {
                SwarmError::DatabaseError(format!("Failed to seed agent {}: {}", next, e))
            })?;

            seed_idle_agents_recursive(db, next.saturating_add(1), count).await
        }
    })
}

fn determine_transition(stage: Stage, result: &StageResult) -> StageTransition {
    if !result.is_success() {
        StageTransition::RetryImplement
    } else if stage == Stage::RedQueen {
        StageTransition::Finalize
    } else {
        stage
            .next()
            .map_or(StageTransition::NoOp, StageTransition::Advance)
    }
}

#[cfg(test)]
#[path = "write_ops_tests.rs"]
mod tests;
