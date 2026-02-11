use super::{determine_transition, StageTransition};
use crate::db::write_ops::persist_retry_packet;
use crate::db::SwarmDb;
use crate::error::SwarmError;
use crate::types::{
    AgentId, ArtifactType, BeadId, MessageType, RepoId, Stage, StageArtifact, StageResult,
};
use chrono::Utc;
use futures_util::future::join_all;
use serde_json::{json, Value};
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
         DROP FUNCTION IF EXISTS heartbeat_bead_claim(INTEGER, TEXT, INTEGER) CASCADE;
         DROP FUNCTION IF EXISTS recover_expired_bead_claims() CASCADE;
         DROP FUNCTION IF EXISTS claim_next_p0_bead(INTEGER) CASCADE;
         DROP FUNCTION IF EXISTS claim_next_p0_bead(SMALLINT) CASCADE;
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
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("failed to reset schema: {}", e));

    db.initialize_schema_from_sql(include_str!("../../crates/swarm-coordinator/schema.sql"))
        .await
        .unwrap_or_else(|e| panic!("failed to initialize schema: {}", e));
}

async fn reset_runtime_tables(db: &SwarmDb) {
    sqlx::query(
        "TRUNCATE TABLE agent_messages, stage_artifacts, execution_events, stage_history, agent_run_logs, bead_claims, bead_backlog, agent_state RESTART IDENTITY",
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
async fn finalize_after_push_confirmation_rejects_unconfirmed_push_without_db_io() {
    let lazy_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://localhost/unused")
        .unwrap_or_else(|e| panic!("failed to build lazy pool: {e}"));
    let db = db_from_pool(lazy_pool);
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-push-gate"));

    let result = db
        .finalize_after_push_confirmation(&agent_id, &bead_id, false)
        .await;

    assert!(matches!(result, Err(SwarmError::AgentError(_))));
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
async fn record_stage_complete_persists_transcript_for_success() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-transcript-success"));

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

    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::ContractDocument,
        "contract content",
        None,
    )
    .await
    .unwrap_or_else(|e| panic!("store_stage_artifact failed: {}", e));

    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::RustContract,
        1,
        StageResult::Passed,
        1,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let transcript_text =
        sqlx::query_scalar::<_, String>("SELECT transcript FROM stage_history WHERE id = $1")
            .bind(stage_history_id)
            .fetch_one(db.pool())
            .await
            .unwrap_or_else(|e| panic!("fetch transcript failed: {}", e));

    assert!(!transcript_text.trim().is_empty());

    let transcript_value: Value = serde_json::from_str(&transcript_text)
        .unwrap_or_else(|e| panic!("parse transcript json failed: {}", e));

    assert_eq!(transcript_value["stage"], Value::from("rust-contract"));
    assert_eq!(transcript_value["status"], Value::from("passed"));
    assert_eq!(
        transcript_value["metadata"]["stage_history_id"],
        Value::from(stage_history_id)
    );

    let artifact_types = transcript_value["metadata"]["artifact_types"]
        .as_array()
        .expect("artifact_types should be an array");
    assert!(artifact_types
        .iter()
        .any(|value| value == &Value::from("contract_document")));

    let artifact_refs = transcript_value["artifacts"]
        .as_array()
        .expect("artifacts should be an array");
    assert!(artifact_refs
        .iter()
        .any(|artifact| artifact["artifact_type"] == Value::from("contract_document")));

    let stage_log_artifact = db
        .get_stage_artifacts(stage_history_id)
        .await
        .unwrap_or_else(|e| panic!("get_stage_artifacts failed: {}", e))
        .into_iter()
        .find(|artifact| {
            artifact.artifact_type == ArtifactType::StageLog
                && artifact
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("stage_history_id"))
                    .and_then(Value::as_i64)
                    == Some(stage_history_id)
        });

    assert!(stage_log_artifact.is_some());
    assert_eq!(stage_log_artifact.unwrap().content, transcript_text);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn record_stage_complete_persists_transcript_for_failure() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-transcript-failure"));

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

    let failure_message = "transcript failure simulation".to_string();
    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::RustContract,
        1,
        StageResult::Failed(failure_message.clone()),
        1,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let transcript_text =
        sqlx::query_scalar::<_, String>("SELECT transcript FROM stage_history WHERE id = $1")
            .bind(stage_history_id)
            .fetch_one(db.pool())
            .await
            .unwrap_or_else(|e| panic!("fetch transcript failed: {}", e));

    let transcript_value: Value = serde_json::from_str(&transcript_text)
        .unwrap_or_else(|e| panic!("parse transcript json failed: {}", e));

    assert_eq!(transcript_value["status"], Value::from("failed"));
    assert_eq!(transcript_value["message"], Value::from(failure_message));

    let stage_log_artifact = db
        .get_stage_artifacts(stage_history_id)
        .await
        .unwrap_or_else(|e| panic!("get_stage_artifacts failed: {}", e))
        .into_iter()
        .find(|artifact| {
            artifact.artifact_type == ArtifactType::StageLog
                && artifact
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("stage_history_id"))
                    .and_then(Value::as_i64)
                    == Some(stage_history_id)
        });

    assert!(stage_log_artifact.is_some());
    assert_eq!(stage_log_artifact.unwrap().content, transcript_text);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn record_stage_complete_errors_without_started_row() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-transcript-no-stage"));

    let result = db
        .record_stage_complete(
            &agent_id,
            &bead_id,
            Stage::QaEnforcer,
            1,
            StageResult::Passed,
            1,
        )
        .await;

    assert!(matches!(result, Err(SwarmError::DatabaseError(_))));
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn record_stage_complete_errors_when_transcript_storage_fails() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-transcript-storage-failure"));

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

    sqlx::query(
        "CREATE OR REPLACE FUNCTION fail_stage_log_transcript() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.artifact_type = 'stage_log' THEN
                 RAISE EXCEPTION 'simulated transcript failure';
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("create trigger function failed: {}", e));

    sqlx::query(
        "CREATE TRIGGER fail_stage_log_before_insert
         BEFORE INSERT ON stage_artifacts
         FOR EACH ROW
         EXECUTE FUNCTION fail_stage_log_transcript();",
    )
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("create trigger failed: {}", e));

    let result = db
        .record_stage_complete(
            &agent_id,
            &bead_id,
            Stage::RustContract,
            1,
            StageResult::Passed,
            1,
        )
        .await;

    assert!(matches!(result, Err(SwarmError::DatabaseError(_))));

    sqlx::query("DROP TRIGGER IF EXISTS fail_stage_log_before_insert ON stage_artifacts")
        .execute(db.pool())
        .await
        .unwrap_or_else(|e| panic!("drop trigger failed: {}", e));
    sqlx::query("DROP FUNCTION IF EXISTS fail_stage_log_transcript() CASCADE")
        .execute(db.pool())
        .await
        .unwrap_or_else(|e| panic!("drop function failed: {}", e));
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
async fn record_stage_complete_persists_transcript_on_failure() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-transcript-failure"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {e}"));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("insert backlog failed: {e}"));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {e}"));
    db.record_stage_started(&agent_id, &bead_id, Stage::Implement, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {e}"));
    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::Implement,
        1,
        StageResult::Failed("needs work".to_string()),
        1,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {e}"));

    let stage_history = sqlx::query!(
        "SELECT id, transcript FROM stage_history WHERE bead_id = $1 ORDER BY id DESC LIMIT 1",
        bead_id.value()
    )
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|e| panic!("fetch stage_history failed: {e}"));

    let transcript = stage_history
        .transcript
        .unwrap_or_else(|| panic!("missing transcript"));
    let transcript_value: Value = serde_json::from_str(&transcript)
        .unwrap_or_else(|e| panic!("parse transcript failed: {e}"));

    assert_eq!(transcript_value["stage"].as_str(), Some("implement"));
    assert_eq!(transcript_value["status"].as_str(), Some("failed"));
    assert_eq!(transcript_value["message"].as_str(), Some("needs work"));
    assert_eq!(
        transcript_value["metadata"]["stage_history_id"].as_i64(),
        Some(stage_history.id)
    );

    let stage_log = sqlx::query!(
        "SELECT content FROM stage_artifacts WHERE stage_history_id = $1 AND artifact_type = 'stage_log' ORDER BY id DESC LIMIT 1",
        stage_history.id
    )
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|e| panic!("fetch stage log failed: {e}"));

    assert_eq!(stage_log.content, transcript);

    let raw_stage_log = sqlx::query!(
        "SELECT content FROM stage_artifacts WHERE stage_history_id = $1 AND artifact_type = 'stage_log' ORDER BY id ASC LIMIT 1",
        stage_history.id
    )
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|e| panic!("fetch earliest stage log failed: {e}"));

    assert_eq!(
        transcript_value["full_log"].as_str(),
        Some(raw_stage_log.content.as_str())
    );
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
async fn claim_next_bead_recovers_expired_claim_for_another_agent() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let original_agent = AgentId::new(RepoId::new("local"), 1);
    let recovery_agent = AgentId::new(RepoId::new("local"), 2);
    let bead_id = BeadId::new(unique_bead("bead-recover-expired"));

    db.seed_idle_agents(2)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));

    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("insert backlog failed: {}", e));

    db.claim_next_bead(&original_agent)
        .await
        .unwrap_or_else(|e| panic!("initial claim failed: {}", e));

    sqlx::query(
        "UPDATE bead_claims
         SET heartbeat_at = NOW() - INTERVAL '12 minutes',
             lease_expires_at = NOW() - INTERVAL '1 minute'
         WHERE bead_id = $1",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("expire lease failed: {}", e));

    let recovered = db
        .claim_next_bead(&recovery_agent)
        .await
        .unwrap_or_else(|e| panic!("recovery claim failed: {}", e));

    assert_eq!(recovered.as_ref().map(BeadId::value), Some(bead_id.value()));

    let owner = sqlx::query_scalar::<_, i32>(
        "SELECT claimed_by FROM bead_claims WHERE bead_id = $1 AND status = 'in_progress'",
    )
    .bind(bead_id.value())
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|e| panic!("fetch owner failed: {}", e));
    assert_eq!(owner, recovery_agent.number() as i32);

    let original_status =
        sqlx::query_scalar::<_, String>("SELECT status FROM agent_state WHERE agent_id = $1")
            .bind(original_agent.number() as i32)
            .fetch_one(db.pool())
            .await
            .unwrap_or_else(|e| panic!("fetch original status failed: {}", e));
    assert_eq!(original_status, "idle");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn heartbeat_extends_lease_monotonically_for_owner_claim() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-heartbeat"));

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

    let before =
        sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT heartbeat_at, lease_expires_at FROM bead_claims WHERE bead_id = $1",
        )
        .bind(bead_id.value())
        .fetch_one(db.pool())
        .await
        .unwrap_or_else(|e| panic!("fetch claim before heartbeat failed: {}", e));

    let heartbeat_ok = db
        .heartbeat_claim(&agent_id, &bead_id, 300_000)
        .await
        .unwrap_or_else(|e| panic!("heartbeat_claim failed: {}", e));
    assert!(heartbeat_ok);

    let after =
        sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            "SELECT heartbeat_at, lease_expires_at FROM bead_claims WHERE bead_id = $1",
        )
        .bind(bead_id.value())
        .fetch_one(db.pool())
        .await
        .unwrap_or_else(|e| panic!("fetch claim after heartbeat failed: {}", e));

    assert!(after.0 >= before.0, "heartbeat_at must be monotonic");
    assert!(after.1 > before.1, "lease_expires_at should be extended");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn finalize_after_push_confirmation_rejects_non_owner_claim_mutation() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let owner_agent = AgentId::new(RepoId::new("local"), 1);
    let non_owner_agent = AgentId::new(RepoId::new("local"), 2);
    let bead_id = BeadId::new(unique_bead("bead-finalize-owner"));

    db.seed_idle_agents(2)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("insert backlog failed: {}", e));

    db.claim_next_bead(&owner_agent)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let finalize_result = db
        .finalize_after_push_confirmation(&non_owner_agent, &bead_id, true)
        .await;
    assert!(
        matches!(finalize_result, Err(SwarmError::AgentError(_))),
        "expected AgentError for non-owner finalize, got: {:?}",
        finalize_result
    );

    let claim_row = sqlx::query_as::<_, (String, i32)>(
        "SELECT status, claimed_by FROM bead_claims WHERE bead_id = $1",
    )
    .bind(bead_id.value())
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|e| panic!("fetch claim row failed: {}", e));
    assert_eq!(claim_row.0, "in_progress");
    assert_eq!(claim_row.1, owner_agent.number() as i32);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn mark_bead_blocked_rejects_non_owner_claim_mutation() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let owner_agent = AgentId::new(RepoId::new("local"), 1);
    let non_owner_agent = AgentId::new(RepoId::new("local"), 2);
    let bead_id = BeadId::new(unique_bead("bead-block-owner"));

    db.seed_idle_agents(2)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("insert backlog failed: {}", e));

    db.claim_next_bead(&owner_agent)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let block_result = db
        .mark_bead_blocked(&non_owner_agent, &bead_id, "not owner")
        .await;
    assert!(
        matches!(block_result, Err(SwarmError::AgentError(_))),
        "expected AgentError for non-owner block, got: {:?}",
        block_result
    );

    let claim_status =
        sqlx::query_scalar::<_, String>("SELECT status FROM bead_claims WHERE bead_id = $1")
            .bind(bead_id.value())
            .fetch_one(db.pool())
            .await
            .unwrap_or_else(|e| panic!("fetch claim status failed: {}", e));
    assert_eq!(claim_status, "in_progress");
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
async fn stage_lifecycle_emits_deterministic_execution_events() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-events"));

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
        StageResult::Failed("test suite failed".to_string()),
        15,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let rows = sqlx::query_as::<_, (i64, String, Option<String>)>(
        "SELECT seq, event_type, causation_id
         FROM execution_events
         WHERE bead_id = $1
         ORDER BY seq ASC",
    )
    .bind(bead_id.value())
    .fetch_all(db.pool())
    .await
    .unwrap_or_else(|e| panic!("query events failed: {}", e));

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].1, "stage_started");
    assert_eq!(rows[1].1, "stage_completed");
    assert_eq!(rows[2].1, "transition_retry");
    assert_eq!(rows[0].2, rows[1].2);
    assert_eq!(rows[1].2, rows[2].2);
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn transition_retry_event_includes_structured_redacted_diagnostics() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-diag"));

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
        StageResult::Failed("test failed token=abc123".to_string()),
        19,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let row = sqlx::query_as::<_, (String, bool, String, Option<String>)>(
        "SELECT diagnostics_category, diagnostics_retryable, diagnostics_next_command, diagnostics_detail
         FROM execution_events
         WHERE bead_id = $1 AND event_type = 'transition_retry'
         ORDER BY seq DESC
         LIMIT 1",
    )
    .bind(bead_id.value())
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|e| panic!("query transition_retry diagnostics failed: {}", e));

    assert_eq!(row.0, "test_failure");
    assert!(row.1);
    assert_eq!(row.2, "swarm stage --stage implement");
    assert!(row
        .3
        .as_deref()
        .is_some_and(|detail| detail.contains("token=<redacted>")));
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn transition_retry_records_retry_packet_for_qa_failure() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("bead-retry-qa"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("seed backlog failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::QaEnforcer, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    let artifacts = vec![
        build_stage_artifact(
            stage_history_id,
            1,
            ArtifactType::FailureDetails,
            Some("hash-1"),
        ),
        build_stage_artifact(
            stage_history_id,
            2,
            ArtifactType::TestResults,
            Some("hash-2"),
        ),
    ];

    persist_retry_packet(
        &db,
        stage_history_id,
        Stage::QaEnforcer,
        1,
        Some("qa tests failed password=secret"),
        &artifacts,
    )
    .await
    .unwrap_or_else(|e| panic!("persist_retry_packet failed: {}", e));

    let packet = load_retry_packet(&db, stage_history_id).await;

    assert_eq!(packet["stage"], "qa-enforcer");
    assert_eq!(packet["attempt"], 1);
    assert_eq!(packet["remaining_attempts"], 2);
    assert_eq!(packet["failure_category"], "test_failure");
    assert_eq!(
        packet["failure_message"].as_str().unwrap(),
        "qa tests failed password=<redacted>"
    );
    let references = packet["artifact_references"]
        .as_array()
        .expect("artifact_references should be array");
    assert_eq!(references.len(), 2);
    assert_eq!(
        references[0]["artifact_type"],
        ArtifactType::FailureDetails.as_str()
    );
    assert_eq!(
        references[1]["artifact_type"],
        ArtifactType::TestResults.as_str()
    );
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn transition_retry_records_retry_packet_for_red_queen_failure() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 2);
    let bead_id = BeadId::new(unique_bead("bead-retry-rq"));

    db.seed_idle_agents(2)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("seed backlog failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::RedQueen, 2)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    let artifacts = vec![
        build_stage_artifact(
            stage_history_id,
            1,
            ArtifactType::ImplementationCode,
            Some("hash-impl"),
        ),
        build_stage_artifact(
            stage_history_id,
            2,
            ArtifactType::TestResults,
            Some("hash-tests"),
        ),
    ];

    persist_retry_packet(
        &db,
        stage_history_id,
        Stage::RedQueen,
        2,
        Some("red-queen timeout on moon run"),
        &artifacts,
    )
    .await
    .unwrap_or_else(|e| panic!("persist_retry_packet failed: {}", e));

    let packet = load_retry_packet(&db, stage_history_id).await;

    assert_eq!(packet["stage"], "red-queen");
    assert_eq!(packet["remaining_attempts"], 1);
    assert_eq!(packet["failure_category"], "timeout");
    let references = packet["artifact_references"]
        .as_array()
        .expect("artifact_references should be array");
    let artifact_types: Vec<String> = references
        .iter()
        .filter_map(|reference| {
            reference
                .get("artifact_type")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string())
        })
        .collect();
    assert!(artifact_types.contains(&ArtifactType::ImplementationCode.as_str().to_string()));
    assert!(artifact_types.contains(&ArtifactType::TestResults.as_str().to_string()));
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn retry_packet_handles_missing_failure_message_and_references() {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 3);
    let bead_id = BeadId::new(unique_bead("bead-retry-none"));

    db.seed_idle_agents(3)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("seed backlog failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::QaEnforcer, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    let artifacts = vec![build_stage_artifact(
        stage_history_id,
        1,
        ArtifactType::ImplementationCode,
        None,
    )];

    persist_retry_packet(
        &db,
        stage_history_id,
        Stage::QaEnforcer,
        1,
        None,
        &artifacts,
    )
    .await
    .unwrap_or_else(|e| panic!("persist_retry_packet failed: {}", e));

    let packet = load_retry_packet(&db, stage_history_id).await;

    assert_eq!(packet["failure_category"], "stage_failure");
    assert!(packet["failure_message"].is_null());
    let references = packet["artifact_references"]
        .as_array()
        .expect("artifact_references should be array");
    assert_eq!(references[0]["content_hash"], serde_json::Value::Null);
}

async fn load_retry_packet(db: &SwarmDb, stage_history_id: i64) -> Value {
    let raw = sqlx::query_scalar::<_, String>(
        "SELECT content FROM stage_artifacts WHERE stage_history_id = $1 AND artifact_type = 'retry_packet' LIMIT 1",
    )
    .bind(stage_history_id)
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|e| panic!("failed to fetch retry packet: {}", e));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("failed to parse retry packet JSON: {}", e))
}

fn build_stage_artifact(
    stage_history_id: i64,
    artifact_id: i64,
    artifact_type: ArtifactType,
    content_hash: Option<&str>,
) -> StageArtifact {
    StageArtifact {
        id: artifact_id,
        stage_history_id,
        artifact_type,
        content: format!("artifact {} content", artifact_type.as_str()),
        metadata: None,
        created_at: Utc::now(),
        content_hash: content_hash.map(String::from),
    }
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn given_qa_failure_with_retries_remaining_when_transition_retry_is_recorded_then_retry_packet_artifact_is_persisted_with_expected_fields(
) {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("qa-retry-packet"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("bead_backlog insert failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::QaEnforcer, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::TestResults,
        "{}",
        Some(json!({"phase": "qa"})),
    )
    .await
    .unwrap_or_else(|e| panic!("store_stage_artifact failed: {}", e));

    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::QaEnforcer,
        1,
        StageResult::Failed("test suite failed".to_string()),
        12,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let retry_packet = db
        .get_stage_artifacts(stage_history_id)
        .await
        .unwrap_or_else(|e| panic!("get_stage_artifacts failed: {}", e))
        .into_iter()
        .find(|artifact| artifact.artifact_type == ArtifactType::RetryPacket)
        .unwrap_or_else(|| panic!("retry packet artifact missing"));

    let payload: Value = serde_json::from_str(&retry_packet.content)
        .unwrap_or_else(|e| panic!("deserialize retry packet failed: {}", e));

    assert_eq!(payload["stage"], "qa-enforcer");
    assert_eq!(payload["attempt"], 1);
    assert_eq!(payload["remaining_attempts"], 2);
    assert_eq!(payload["failure_category"], "test_failure");
    assert!(payload["artifact_refs"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["artifact_type"] == "test_results"));
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn given_red_queen_failure_with_retries_remaining_when_transition_retry_is_recorded_then_retry_packet_points_to_latest_implementation_and_test_artifacts(
) {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("rq-retry-packet"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("bead_backlog insert failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let implement_history = db
        .record_stage_started(&agent_id, &bead_id, Stage::Implement, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    db.store_stage_artifact(
        implement_history,
        ArtifactType::ImplementationCode,
        "fn main() {}",
        Some(json!({"source": "implement"})),
    )
    .await
    .unwrap_or_else(|e| panic!("store_stage_artifact failed: {}", e));

    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::Implement,
        1,
        StageResult::Passed,
        10,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let qa_history = db
        .record_stage_started(&agent_id, &bead_id, Stage::QaEnforcer, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    db.store_stage_artifact(
        qa_history,
        ArtifactType::TestResults,
        "{}",
        Some(json!({"suite": "red-queen"})),
    )
    .await
    .unwrap_or_else(|e| panic!("store_stage_artifact failed: {}", e));

    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::QaEnforcer,
        1,
        StageResult::Passed,
        15,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let red_history = db
        .record_stage_started(&agent_id, &bead_id, Stage::RedQueen, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::RedQueen,
        1,
        StageResult::Failed("red queen failed".to_string()),
        8,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let retry_packet = db
        .get_stage_artifacts(red_history)
        .await
        .unwrap_or_else(|e| panic!("get_stage_artifacts failed: {}", e))
        .into_iter()
        .find(|artifact| artifact.artifact_type == ArtifactType::RetryPacket)
        .unwrap_or_else(|| panic!("retry packet artifact missing"));

    let payload: Value = serde_json::from_str(&retry_packet.content)
        .unwrap_or_else(|e| panic!("deserialize retry packet failed: {}", e));

    let refs = payload["artifact_refs"].as_array().unwrap();
    let impl_ref = refs
        .iter()
        .find(|entry| entry["artifact_type"] == "implementation_code")
        .unwrap_or_else(|| panic!("implementation reference not found"));
    let qa_ref = refs
        .iter()
        .find(|entry| entry["artifact_type"] == "test_results")
        .unwrap_or_else(|| panic!("test results reference not found"));

    assert_eq!(impl_ref["stage_history_id"], implement_history.into());
    assert_eq!(qa_ref["stage_history_id"], qa_history.into());
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn given_missing_failure_diagnostics_when_retry_packet_creation_runs_then_command_returns_explicit_error_diagnostics(
) {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("missing-diag"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("bead_backlog insert failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::QaEnforcer, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::QaEnforcer,
        1,
        StageResult::Failed("".to_string()),
        7,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let row = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT diagnostics_category, diagnostics_detail
         FROM execution_events
         WHERE bead_id = $1 AND event_type = 'transition_retry'
         ORDER BY seq DESC
         LIMIT 1",
    )
    .bind(bead_id.value())
    .fetch_one(db.pool())
    .await
    .unwrap_or_else(|e| panic!("fetch diagnostics failed: {}", e));

    assert_eq!(row.0, "stage_failure");
    assert!(row.1.is_none());

    let retry_packet = db
        .get_stage_artifacts(stage_history_id)
        .await
        .unwrap()
        .into_iter()
        .find(|artifact| artifact.artifact_type == ArtifactType::RetryPacket)
        .unwrap();

    let payload: Value = serde_json::from_str(&retry_packet.content).unwrap();
    assert!(payload["failure_detail"].is_null());
    assert_eq!(payload["failure_category"], "stage_failure");
}

#[tokio::test]
#[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
async fn given_invalid_artifact_references_when_retry_packet_creation_runs_then_packet_marks_missing_refs_without_crashing(
) {
    let db = test_db().await;
    setup_schema(&db).await;
    reset_runtime_tables(&db).await;

    let agent_id = AgentId::new(RepoId::new("local"), 1);
    let bead_id = BeadId::new(unique_bead("missing-artifacts"));

    db.seed_idle_agents(1)
        .await
        .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));
    sqlx::query(
        "INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')",
    )
    .bind(bead_id.value())
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("bead_backlog insert failed: {}", e));
    db.claim_next_bead(&agent_id)
        .await
        .unwrap_or_else(|e| panic!("claim_next_bead failed: {}", e));

    let stage_history_id = db
        .record_stage_started(&agent_id, &bead_id, Stage::QaEnforcer, 1)
        .await
        .unwrap_or_else(|e| panic!("record_stage_started failed: {}", e));

    db.record_stage_complete(
        &agent_id,
        &bead_id,
        Stage::QaEnforcer,
        1,
        StageResult::Failed("failure without diagnostics".to_string()),
        9,
    )
    .await
    .unwrap_or_else(|e| panic!("record_stage_complete failed: {}", e));

    let retry_packet = db
        .get_stage_artifacts(stage_history_id)
        .await
        .unwrap()
        .into_iter()
        .find(|artifact| artifact.artifact_type == ArtifactType::RetryPacket)
        .unwrap();

    let payload: Value = serde_json::from_str(&retry_packet.content).unwrap();
    let refs = payload["artifact_refs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| {
            entry["artifact_type"].as_str().map_or(false, |kind| {
                matches!(kind, "implementation_code" | "test_results")
            })
        })
        .collect::<Vec<_>>();
    assert!(refs
        .iter()
        .any(|entry| entry["missing"] == true && entry["artifact_type"] == "implementation_code"));
    assert!(refs
        .iter()
        .any(|entry| entry["missing"] == true && entry["artifact_type"] == "test_results"));
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
