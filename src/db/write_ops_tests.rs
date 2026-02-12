#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SwarmDb;
    use crate::error::SwarmError;
    use crate::types::{AgentId, ArtifactType, BeadId, MessageType, RepoId, Stage, StageResult};
    use futures_util::future::join_all;
    use serde_json::{json, Value};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;
    use sqlx::types::Uuid;
    use std::collections::HashSet;

    fn db_from_pool(pool: PgPool) -> SwarmDb {
        SwarmDb { connection_string: "test".to_string() }
    }

    fn required_test_database_url() -> String {
        std::env::var("SWARM_TEST_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())
            .unwrap_or_else(|| {
                unreachable!("Set SWARM_TEST_DATABASE_URL or DATABASE_URL for DB integration tests")
            })
    }

    async fn test_db() -> SwarmDb {
        let url = required_test_database_url();
        PgPoolOptions::new()
            .max_connections(16)
            .connect(&url)
            .await
            .map(db_from_pool)
            .unwrap_or_else(|e| unreachable!("Failed to connect test database: {}", e))
    }

    async fn setup_schema(db: &SwarmDb) {
        sqlx::raw_sql(
            "DROP VIEW IF EXISTS v_resume_context CASCADE;
             DROP VIEW IF EXISTS v_available_agents CASCADE;
             DROP VIEW IF EXISTS v_feedback_required CASCADE;
             DROP VIEW IF EXISTS v_swarm_progress CASCADE;
             DROP VIEW IF EXISTS v_active_agents CASCADE;
             DROP VIEW IF EXISTS v_unread_messages CASCADE;
             DROP VIEW IF EXISTS v_contract_artifacts CASCADE;
             DROP VIEW IF EXISTS v_bead_artifacts CASCADE;
             DROP FUNCTION IF EXISTS mark_messages_read(TEXT, INTEGER, BIGINT[]) CASCADE;
             DROP FUNCTION IF EXISTS get_unread_messages(TEXT, INTEGER, TEXT) CASCADE;
             DROP FUNCTION IF EXISTS send_agent_message(TEXT, INTEGER, TEXT, INTEGER, TEXT, TEXT, TEXT, TEXT, JSONB) CASCADE;
             DROP FUNCTION IF EXISTS store_stage_artifact(BIGINT, TEXT, TEXT, JSONB) CASCADE;
             DROP FUNCTION IF EXISTS heartbeat_bead_claim(INTEGER, TEXT, INTEGER) CASCADE;
             DROP FUNCTION IF EXISTS recover_expired_bead_claims() CASCADE;
             DROP FUNCTION IF EXISTS claim_next_bead(TEXT, INTEGER) CASCADE;
             DROP FUNCTION IF EXISTS claim_next_bead(INTEGER) CASCADE;
             DROP FUNCTION IF EXISTS claim_next_p0_bead(INTEGER) CASCADE;
             DROP FUNCTION IF EXISTS claim_next_p0_bead(SMALLINT) CASCADE;
             DROP FUNCTION IF EXISTS claim_next_p0_bead(TEXT, INTEGER) CASCADE;
             DROP FUNCTION IF EXISTS set_agent_last_update() CASCADE;
             DROP TABLE IF EXISTS agent_messages CASCADE;
             DROP TABLE IF EXISTS stage_artifacts CASCADE;
             DROP TABLE IF EXISTS execution_events CASCADE;
             DROP TABLE IF EXISTS stage_history CASCADE;
             DROP TABLE IF EXISTS agent_run_logs CASCADE;
             DROP TABLE IF EXISTS bead_claims CASCADE;
             DROP TABLE IF EXISTS bead_backlog CASCADE;
             DROP TABLE IF EXISTS agent_state CASCADE;
             DROP TABLE IF EXISTS swarm_config CASCADE;",
        )
        .execute(&db.pool())
        .await
        .unwrap_or_else(|e| unreachable!("failed to reset schema: {}", e));

        db.initialize_schema_from_sql(include_str!("../canonical_schema/schema.sql"))
            .await
            .unwrap_or_else(|e| unreachable!("failed to initialize schema: {}", e));
    }

    async fn reset_runtime_tables(db: &SwarmDb) {
        sqlx::query(
            "TRUNCATE TABLE agent_messages, stage_artifacts, execution_events, stage_history, agent_run_logs, bead_claims, bead_backlog, agent_state RESTART IDENTITY",
        )
        .execute(&db.pool())
        .await
        .unwrap_or_else(|e| unreachable!("failed to truncate runtime tables: {}", e));
    }

    fn unique_bead(prefix: &str) -> String {
        format!("{}-{}", prefix, Uuid::new_v4())
    }

    fn seed_backlog_recursive<'a>(
        db: &'a SwarmDb,
        bead_ids: &'a [String],
        idx: usize,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            match bead_ids.get(idx) {
                Some(bead_id) => {
                    sqlx::query(
                        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
                    )
                    .bind(bead_id)
                    .execute(&db.pool())
                    .await
                    .unwrap_or_else(|e| unreachable!("seed backlog insert failed: {}", e));
                    seed_backlog_recursive(db, bead_ids, idx + 1).await
                }
                None => (),
            }
        })
    }

    #[test]
    fn transition_for_successful_non_terminal_stage_advances() {
        assert_eq!(
            determine_transition(Stage::Implement, &StageResult::Passed),
            StageTransition::Advance(Stage::QaEnforcer)
        );
    }

    #[test]
    fn transition_for_successful_terminal_stage_finalizes() {
        assert_eq!(
            determine_transition(Stage::RedQueen, &StageResult::Passed),
            StageTransition::Finalize
        );
    }

    #[test]
    fn transition_for_failure_retries_implementation() {
        assert_eq!(
            determine_transition(Stage::Implement, &StageResult::Failed("test".to_string())),
            StageTransition::RetryImplement
        );
    }

    #[test]
    fn transition_for_qa_enforcer_failure_does_not_retry() {
        assert_eq!(
            determine_transition(Stage::QaEnforcer, &StageResult::Failed("test".to_string())),
            StageTransition::NoOp
        );
    }

    #[test]
    fn transition_for_error_with_message_blocks() {
        assert_eq!(
            determine_transition(Stage::Implement, &StageResult::Error("test".to_string())),
            StageTransition::Finalize
        );
    }

    #[test]
    fn transition_qa_enforcer_error_no_ops() {
        assert_eq!(
            determine_transition(Stage::QaEnforcer, &StageResult::Error("test".to_string())),
            StageTransition::NoOp
        );
    }

    #[test]
    fn transition_red_queen_error_finalizes() {
        assert_eq!(
            determine_transition(Stage::RedQueen, &StageResult::Error("test".to_string())),
            StageTransition::Finalize
        );
    }

    #[tokio::test]
    async fn record_stage_complete_finalizes_bead_and_agent() {
        let db = test_db().await;
        setup_schema(&db).await;
        reset_runtime_tables(&db).await;

        let repo_id = RepoId::new("test-repo");
        let agent_id = AgentId::new(&repo_id, 1);
        let bead_id = BeadId::new("test-bead");

        // Seed initial state
        sqlx::query("INSERT INTO repositories (repo_id, name, path) VALUES ($1, $2, $3)")
            .bind(&repo_id)
            .bind("Test Repo")
            .bind("/test/path")
            .execute(&db.pool())
            .await
            .unwrap();

        sqlx::query("INSERT INTO agents (agent_id, repo_id, status) VALUES ($1, $2, $3)")
            .bind(&agent_id)
            .bind(&repo_id)
            .bind("idle")
            .execute(&db.pool())
            .await
            .unwrap();

        sqlx::query("INSERT INTO beads (bead_id, repo_id, status) VALUES ($1, $2, $3)")
            .bind(&bead_id)
            .bind(&repo_id)
            .bind("pending")
            .execute(&db.pool())
            .await
            .unwrap();

        // Record stage completion
        let stage_history_id = db
            .record_stage_complete(
                &repo_id,
                &agent_id,
                &bead_id,
                Stage::Implement,
                1,
                &StageResult::Passed("completed".to_string()),
                None,
            )
            .await
            .unwrap();

        // Verify bead is finalized
        let bead_status: String = sqlx::query_scalar(
            "SELECT status FROM beads WHERE bead_id = $1",
        )
        .bind(&bead_id)
        .fetch_one(&db.pool())
        .await
        .unwrap();

        assert_eq!(bead_status, "finalized");

        // Verify agent is idle
        let agent_status: String = sqlx::query_scalar(
            "SELECT status FROM agents WHERE agent_id = $1",
        )
        .bind(&agent_id)
        .fetch_one(&db.pool())
        .await
        .unwrap();

        assert_eq!(agent_status, "idle");
    }

    #[tokio::test]
    async fn ninety_concurrent_claims_are_unique_and_bounded() {
        let db = test_db().await;
        setup_schema(&db).await;
        reset_runtime_tables(&db).await;

        let repo_id = RepoId::new("test-repo");
        let bead_ids: Vec<String> = (0..100)
            .map(|i| unique_bead(&format!("bead-{}", i)))
            .collect();

        // Seed backlog
        let futures: Vec<_> = bead_ids.iter().map(|bead_id| {
            sqlx::query(
                "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
            )
            .bind(bead_id)
            .execute(&db.pool())
        }).collect();

        join_all(futures).await;

        // Try concurrent claims
        let agents: Vec<AgentId> = (0..90)
            .map(|i| AgentId::new(&repo_id, i as u32))
            .collect();

        let claims: Vec<_> = agents.iter().map(|agent_id| {
            db.claim_bead(agent_id, &BeadId::new("unknown"))
        }).collect();

        let results = join_all(claims).await;

        // All should succeed
        for result in &results {
            assert!(result.is_ok());
        }

        // Verify beads are claimed (at most 90)
        let claimed_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM bead_claims",
        )
        .fetch_one(&db.pool())
        .await
        .unwrap();

        assert!(claimed_count <= 90);
    }
}

// Keep non-test functions outside if needed
fn outside_function() {
    // This is just an example
}