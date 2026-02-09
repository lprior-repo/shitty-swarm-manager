use super::{determine_transition, StageTransition};
use crate::db::SwarmDb;
use crate::types::{AgentId, ArtifactType, BeadId, MessageType, RepoId, Stage, StageResult};
use futures_util::future::join_all;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashSet;
use uuid::Uuid;

fn db_from_pool(pool: PgPool) -> SwarmDb {
    SwarmDb { pool }
}

fn required_test_database_url() -> String {
    std::env::var("SWARM_TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| {
            panic!("Set SWARM_TEST_DATABASE_URL or DATABASE_URL for DB integration tests")
        })
}

async fn test_db() -> SwarmDb {
    let url = required_test_database_url();
    PgPoolOptions::new()
        .max_connections(16)
        .connect(&url)
        .await
        .map(db_from_pool)
        .unwrap_or_else(|e| panic!("Failed to connect test database: {}", e))
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
         DROP FUNCTION IF EXISTS claim_next_p0_bead(INTEGER) CASCADE;
         DROP FUNCTION IF EXISTS claim_next_p0_bead(SMALLINT) CASCADE;
         DROP FUNCTION IF EXISTS set_agent_last_update() CASCADE;
         DROP TABLE IF EXISTS agent_messages CASCADE;
         DROP TABLE IF EXISTS stage_artifacts CASCADE;
         DROP TABLE IF EXISTS stage_history CASCADE;
         DROP TABLE IF EXISTS agent_run_logs CASCADE;
         DROP TABLE IF EXISTS bead_claims CASCADE;
         DROP TABLE IF EXISTS bead_backlog CASCADE;
         DROP TABLE IF EXISTS agent_state CASCADE;
         DROP TABLE IF EXISTS swarm_config CASCADE;",
    )
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("failed to reset schema: {}", e));

    db.initialize_schema_from_sql(include_str!("../../crates/swarm-coordinator/schema.sql"))
        .await
        .unwrap_or_else(|e| panic!("failed to initialize schema: {}", e));
}

async fn reset_runtime_tables(db: &SwarmDb) {
    sqlx::query(
        "TRUNCATE TABLE agent_messages, stage_artifacts, stage_history, agent_run_logs, bead_claims, bead_backlog, agent_state RESTART IDENTITY",
    )
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("failed to truncate runtime tables: {}", e));
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
                .execute(db.pool())
                .await
                .unwrap_or_else(|e| panic!("seed backlog insert failed: {}", e));
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
        determine_transition(Stage::QaEnforcer, &StageResult::Failed("x".to_string())),
        StageTransition::RetryImplement
    );
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn record_stage_complete_finalizes_bead_and_agent() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-finalize"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("insert backlog failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));
    db.record_stage_started(&agent_id, &bead_id, Stage::RedQueen, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));
    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::RedQueen,
        1,
        StageResult::Passed,
        1,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM agent_state WHERE agent_id = $1")
            .bind(agent_id.number() as i32)
            .fetch_one(db.pool())
            .await
            .ok();
    let claim_status =
        sqlx::query_scalar::<_, String>("SELECT status FROM bead_claims WHERE bead_id = $1")
            .bind(bead_id.value())
            .fetch_one(db.pool())
            .await
            .ok();

    assert_eq!(status.as_deref(), Some("done"));
    assert_eq!(claim_status.as_deref(), Some("completed"));
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn record_stage_complete_failure_sets_waiting_and_attempt() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-retry"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("insert backlog failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));
    db.record_stage_started(&agent_id, &bead_id, Stage::Implement, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));
    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::Implement,
        1,
        StageResult::Failed("needs work".to_string()),
        1,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM agent_state WHERE agent_id = $1")
            .bind(agent_id.number() as i32)
            .fetch_one(db.pool())
            .await
            .ok();
    let attempts = sqlx::query_scalar::<_, i32>(
        "SELECT implementation_attempt FROM agent_state WHERE agent_id = $1",
    )
    .bind(agent_id.number() as i32)
    .fetch_one(db.pool())
    .await
    .ok();

    assert_eq!(status.as_deref(), Some("waiting"));
    assert_eq!(attempts, Some(1));
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn ninety_concurrent_claims_are_unique_and_bounded() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    db.seed_idle_agents(95)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));

    let bead_ids = (1..=90)
        .map(|n| unique_bead(&format!("bead-{}", n)))
        .collect::<Vec<_>>();
    seed_backlog_recursive(&db, &bead_ids, 0).await;

    let claim_futures = (1..=90)
        .map(|n| {
            let db = db.clone();
            async move {
                let agent = AgentId::new(RepoId::new("local"), n);
                db.claim_next_bead(&agent)
                    .await
                    .ok()
                    .flatten()
                    .map(|b| b.value().to_string())
            }
        })
        .collect::<Vec<_>>();

    let claims = join_all(claim_futures).await;
    let claimed = claims.into_iter().flatten().collect::<Vec<_>>();
    let unique = claimed.iter().cloned().collect::<HashSet<_>>();

    assert_eq!(claimed.len(), 90);
    assert_eq!(unique.len(), 90);

    let overflow_futures = (91..=95)
        .map(|n| {
            let db = db.clone();
            async move {
                let agent = AgentId::new(RepoId::new("local"), n);
                db.claim_next_bead(&agent).await.ok().flatten()
            }
        })
        .collect::<Vec<_>>();

    let overflow = join_all(overflow_futures).await;
    assert!(overflow.into_iter().all(|v| v.is_none()));
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn stage_artifact_store_is_deduplicated_by_hash_and_type() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-artifact"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("insert backlog failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));
    let stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::RustContract, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    let first_id = db
        .store_stage_artifact(
            stage_history_id,
            ArtifactType::ContractDocument,
            "contract-body",
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("store_stage_artifact first failed: {}", e));

    let duplicate_id = db
        .store_stage_artifact(
            stage_history_id,
            ArtifactType::ContractDocument,
            "contract-body",
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("store_stage_artifact duplicate failed: {}", e));

    let second_type_id = db
        .store_stage_artifact(
            stage_history_id,
            ArtifactType::StageLog,
            "contract-body",
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("store_stage_artifact different type failed: {}", e));

    assert_eq!(first_id, duplicate_id);
    assert_ne!(first_id, second_type_id);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn record_stage_started_returns_stage_history_id_for_immediate_artifact_writes() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-stage-id"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("insert backlog failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::Implement, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    let persisted_id = sqlx::query_scalar::<_, i64>(
        "SELECT id
         FROM stage_history
         WHERE agent_id = $1 AND bead_id = $2 AND stage = $3 AND attempt_number = $4
         ORDER BY id DESC
         LIMIT 1",
    )
    .bind(agent_id.number() as i32)
    .bind(bead_id.value())
    .bind(Stage::Implement.as_str())
    .bind(1_i32)
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|e| panic!("fetch stage_history id failed: {}", e));

    assert_eq!(stage_history_id, persisted_id);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn message_send_receive_and_mark_read_round_trip_works() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let from_agent = AgentId::new(RepoId::new("local"), 1);
    let to_agent = AgentId::new(RepoId::new("local"), 2);
    let bead_id = BeadId::new(unique_bead("bead-msg"));

    db.seed_idle_agents(2)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));

    let message_id = db
        .send_agent_message(
            &from_agent,
            Some(&to_agent),
            Some(&bead_id),
            MessageType::QaFailed,
            "qa failed",
            "3 tests failed",
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("send_agent_message failed: {}", e));

    let unread_before = db
        .get_unread_messages(&to_agent, Some(&bead_id))
        .await
        .unwrap_or_else(|e| panic!("get_unread_messages failed: {}", e));

    assert_eq!(unread_before.len(), 1);
    assert_eq!(unread_before[0].id, message_id);
    assert_eq!(unread_before[0].message_type, MessageType::QaFailed);

    db.mark_messages_read(&to_agent, &[message_id])
        .await
        .unwrap_or_else(|e| panic!("mark_messages_read failed: {}", e));

    let unread_after = db
        .get_unread_messages(&to_agent, Some(&bead_id))
        .await
        .unwrap_or_else(|e| panic!("get_unread_messages after mark failed: {}", e));

    assert!(unread_after.is_empty());
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn release_agent_resets_agent_and_requeues_bead() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-release"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("insert backlog failed: {}", e));

    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let released = db
        .release_agent(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("release_agent failed: {}", e));

    assert_eq!(released.as_ref().map(BeadId::value), Some(bead_id.value()));

    let status =
        sqlx::query_scalar::<_, String>("SELECT status FROM agent_state WHERE agent_id = $1")
            .bind(agent_id.number() as i32)
            .fetch_one(db.pool())
            .await
            .unwrap_or_else(|e| panic!("fetch agent status failed: {}", e));
    assert_eq!(status, "idle");

    let backlog_status =
        sqlx::query_scalar::<_, String>("SELECT status FROM bead_backlog WHERE bead_id = $1")
            .bind(bead_id.value())
            .fetch_one(db.pool())
            .await
            .unwrap_or_else(|e| panic!("fetch backlog status failed: {}", e));
    assert_eq!(backlog_status, "pending");

    let claim_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM bead_claims WHERE bead_id = $1")
            .bind(bead_id.value())
            .fetch_one(db.pool())
            .await
            .unwrap_or_else(|e| panic!("fetch claim count failed: {}", e));
    assert_eq!(claim_count, 0);
}
