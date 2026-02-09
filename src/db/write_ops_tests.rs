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
async fn claim_next_bead_prefers_existing_in_progress_claim() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-resume"));

    let seed_result = db.seed_idle_agents(1).await;
    assert!(
        seed_result.is_ok(),
        "seed_idle_agents failed: {:?}",
        seed_result
    );

    let insert_result = sqlx::query(
        "INSERT INTO bead_claims (bead_id, claimed_by, status) VALUES ($1, $2, 'in_progress')",
    )
    .bind(bead_id.value())
    .bind(agent_id.number() as i32)
    .execute(db.pool())
    .await;
    assert!(
        insert_result.is_ok(),
        "insert bead_claims failed: {:?}",
        insert_result
    );

    let claim_result = db.claim_next_bead(&agent_id).await;
    assert!(
        claim_result.is_ok(),
        "claim_next_bead failed: {:?}",
        claim_result
    );

    let claimed = match claim_result {
        Ok(value) => value,
        Err(err) => panic!("claim_next_bead failed: {}", err),
    };
    assert_eq!(
        claimed.as_ref().map(|bead| bead.value()),
        Some(bead_id.value())
    );

    let state_row = sqlx::query_as::<_, (Option<String>, String, Option<String>)>(
        "SELECT bead_id, status, current_stage FROM agent_state WHERE agent_id = $1",
    )
    .bind(agent_id.number() as i32)
    .fetch_optional(db.pool())
    .await;
    assert!(
        state_row.is_ok(),
        "fetch agent_state failed: {:?}",
        state_row
    );

    let (state_bead, status, stage) = match state_row.ok().flatten() {
        Some(values) => values,
        None => (None, "missing".to_string(), None),
    };

    assert_eq!(state_bead.as_deref(), Some(bead_id.value()));
    assert_eq!(status.as_str(), "working");
    assert_eq!(stage.as_deref(), Some("rust-contract"));
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

    let seed_result = db.seed_idle_agents(1).await;
    assert!(
        seed_result.is_ok(),
        "seed_idle_agents failed: {:?}",
        seed_result
    );

    let insert_backlog_result = sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await;
    assert!(
        insert_backlog_result.is_ok(),
        "insert backlog failed: {:?}",
        insert_backlog_result
    );

    let claim_result = db.claim_next_bead(&agent_id).await;
    assert!(
        claim_result.is_ok(),
        "claim_next_bead failed: {:?}",
        claim_result
    );

    let stage_history_id_result = db
        .record_stage_started(&agent_id, &bead_id, Stage::Implement, 1)
        .await;
    let stage_history_id = match stage_history_id_result {
        Ok(id) => id,
        Err(err) => panic!("record_stage_started failed: {}", err),
    };

    let persisted_id_result = sqlx::query_scalar::<_, i64>(
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
    .await;
    let persisted_id = match persisted_id_result {
        Ok(id) => id,
        Err(err) => panic!("fetch stage_history id failed: {}", err),
    };

    assert_eq!(stage_history_id, persisted_id);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn artifact_lookup_helpers_avoid_overfetch_and_preserve_ordering() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-artifact-helper"));

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

    let first_stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::RustContract, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started first failed: {}", e));

    db.store_stage_artifact(
        first_stage_history_id,
        ArtifactType::ContractDocument,
        "first-contract",
        None,
    )
    .await
    .unwrap_or_else(|e| panic!("store first contract failed: {}", e));

    let second_stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::RustContract, 2)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started second failed: {}", e));

    db.store_stage_artifact(
        second_stage_history_id,
        ArtifactType::ContractDocument,
        "second-contract",
        None,
    )
    .await
    .unwrap_or_else(|e| panic!("store second contract failed: {}", e));

    let has_contract = db
        .bead_has_artifact_type(&bead_id, ArtifactType::ContractDocument)
        .await
        .unwrap_or_else(|e| panic!("bead_has_artifact_type failed: {}", e));
    let has_test_results = db
        .bead_has_artifact_type(&bead_id, ArtifactType::TestResults)
        .await
        .unwrap_or_else(|e| panic!("bead_has_artifact_type missing failed: {}", e));

    assert!(has_contract);
    assert!(!has_test_results);

    let first_contract = db
        .get_first_bead_artifact_by_type(&bead_id, ArtifactType::ContractDocument)
        .await
        .unwrap_or_else(|e| panic!("get_first_bead_artifact_by_type failed: {}", e));

    assert_eq!(
        first_contract
            .as_ref()
            .map(|artifact| artifact.content.as_str()),
        Some("first-contract")
    );
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
            ("qa failed", "3 tests failed"),
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
async fn mark_messages_read_updates_requested_ids_in_single_bulk_call() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let from_agent = AgentId::new(RepoId::new("local"), 1);
    let to_agent = AgentId::new(RepoId::new("local"), 2);
    let other_agent = AgentId::new(RepoId::new("local"), 3);
    let bead_id = BeadId::new(unique_bead("bead-msg-bulk"));

    db.seed_idle_agents(3)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));

    let first_id = db
        .send_agent_message(
            &from_agent,
            Some(&to_agent),
            Some(&bead_id),
            MessageType::QaFailed,
            ("qa failed 1", "first"),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("send first message failed: {}", e));

    let second_id = db
        .send_agent_message(
            &from_agent,
            Some(&to_agent),
            Some(&bead_id),
            MessageType::QaFailed,
            ("qa failed 2", "second"),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("send second message failed: {}", e));

    let other_agent_message_id = db
        .send_agent_message(
            &from_agent,
            Some(&other_agent),
            Some(&bead_id),
            MessageType::QaFailed,
            ("qa failed 3", "third"),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("send third message failed: {}", e));

    db.mark_messages_read(&to_agent, &[first_id, second_id, other_agent_message_id])
        .await
        .unwrap_or_else(|e| panic!("mark_messages_read failed: {}", e));

    let target_unread = db
        .get_unread_messages(&to_agent, Some(&bead_id))
        .await
        .unwrap_or_else(|e| panic!("get_unread_messages target failed: {}", e));
    assert!(
        target_unread.is_empty(),
        "target agent messages should be read"
    );

    let other_unread = db
        .get_unread_messages(&other_agent, Some(&bead_id))
        .await
        .unwrap_or_else(|e| panic!("get_unread_messages other failed: {}", e));
    assert_eq!(
        other_unread.len(),
        1,
        "other agent message should remain unread"
    );
    assert_eq!(other_unread[0].id, other_agent_message_id);
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
