#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use crate::gate_cache::GateExecutionCache;
use crate::types::{ArtifactType, BeadId, RepoId};
use crate::{AgentId, SwarmDb};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use super::gate_stage::{execute_qa_stage, execute_red_queen_stage, run_moon_task};

fn test_database_url() -> Option<String> {
    std::env::var("SWARM_TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .filter(|url| !url.trim().is_empty())
        .filter(|url| url.starts_with("postgres://") || url.starts_with("postgresql://"))
}

async fn test_pool_or_skip() -> Option<PgPool> {
    let run_integration = std::env::var("SWARM_RUN_DB_INTEGRATION")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    if !run_integration {
        return None;
    }

    let url = test_database_url()?;
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .ok()
}

async fn setup_schema(db: &SwarmDb) {
    db.initialize_schema_from_sql(include_str!("../../schema.sql"))
        .await
        .expect("Failed to initialize schema");
}

async fn insert_bead_claim(pool: &PgPool, bead_id: &BeadId, agent_id: &AgentId) {
    sqlx::query(
        "INSERT INTO bead_claims (repo_id, bead_id, claimed_by, status) VALUES ($1, $2, $3, 'in_progress')",
    )
    .bind(agent_id.repo_id().value())
    .bind(bead_id.value())
    .bind(agent_id.number().cast_signed())
    .execute(pool)
    .await
    .expect("Failed to insert bead claim");
}

async fn insert_started_stage_history(pool: &PgPool, bead_id: &BeadId, agent_id: &AgentId) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "INSERT INTO stage_history (repo_id, agent_id, bead_id, stage, attempt_number, status, started_at)\n             VALUES ($1, $2, $3, 'implement', 1, 'started', NOW())\n             RETURNING id",
    )
    .bind(agent_id.repo_id().value())
    .bind(agent_id.number().cast_signed())
    .bind(bead_id.value())
    .fetch_one(pool)
    .await
    .expect("Failed to insert stage history")
}

async fn seed_artifact(
    db: &SwarmDb,
    pool: &PgPool,
    bead_id: &BeadId,
    agent_id: &AgentId,
    artifact_type: ArtifactType,
    content: &str,
) {
    insert_bead_claim(pool, bead_id, agent_id).await;
    let stage_history_id = insert_started_stage_history(pool, bead_id, agent_id).await;
    db.store_stage_artifact(stage_history_id, artifact_type, content, None)
        .await
        .expect("Failed to seed stage artifact");
}

#[tokio::test]
async fn given_cached_stderr_only_output_when_running_moon_task_then_log_translation_is_preserved()
{
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");

    cache
        .put(
            ":quick".to_string(),
            true,
            None,
            String::new(),
            "stderr only".to_string(),
        )
        .await
        .expect("cache write");

    let output = run_moon_task(":quick", Some(&cache))
        .await
        .expect("cached command output");

    assert!(output.success);
    assert_eq!(output.exit_code, None);
    assert_eq!(output.full_log, "stderr only");
    assert_eq!(output.feedback, "");
}

#[tokio::test]
async fn given_cached_stdout_and_stderr_failure_when_running_moon_task_then_feedback_matches_combined_log(
) {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");

    cache
        .put(
            ":test".to_string(),
            false,
            Some(2),
            "stdout payload".to_string(),
            "stderr payload".to_string(),
        )
        .await
        .expect("cache write");

    let output = run_moon_task(":test", Some(&cache))
        .await
        .expect("cached command output");

    assert!(!output.success);
    assert_eq!(output.full_log, "stdout payload\nstderr payload");
    assert_eq!(output.feedback, "stdout payload\nstderr payload");
}

#[tokio::test]
async fn given_missing_implementation_artifact_when_executing_qa_stage_then_failure_output_is_returned(
) {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool);
    let bead_id = BeadId::new("qa-missing-impl");
    let agent_id = AgentId::new(RepoId::new("local"), 11);
    setup_schema(&db).await;

    let output = execute_qa_stage(&bead_id, &agent_id, &db, None)
        .await
        .expect("qa stage should complete with failure output");

    assert!(!output.success);
    assert_eq!(
        output.feedback,
        "No implementation artifact found for QA stage"
    );
}

#[tokio::test]
async fn given_cached_failed_gate_and_implementation_artifact_when_executing_qa_stage_then_failure_artifacts_are_extracted(
) {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("qa-failed-cache");
    let agent_id = AgentId::new(RepoId::new("local"), 12);
    setup_schema(&db).await;
    seed_artifact(
        &db,
        &pool,
        &bead_id,
        &agent_id,
        ArtifactType::ImplementationCode,
        "fn main() {}",
    )
    .await;

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");
    cache
        .put(
            ":quick".to_string(),
            false,
            Some(1),
            "running tests\n1 failed".to_string(),
            String::new(),
        )
        .await
        .expect("cache write");

    let output = execute_qa_stage(&bead_id, &agent_id, &db, Some(&cache))
        .await
        .expect("qa stage should run from cache");

    assert!(!output.success);
    assert!(output.artifacts.contains_key("test_output"));
    assert!(output.artifacts.contains_key("failure_details"));
    assert!(output.test_results.is_some());
}

#[tokio::test]
async fn given_missing_test_results_artifact_when_executing_red_queen_stage_then_failure_output_is_returned(
) {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool);
    let bead_id = BeadId::new("rq-missing-tests");
    let agent_id = AgentId::new(RepoId::new("local"), 13);
    setup_schema(&db).await;

    let output = execute_red_queen_stage(&bead_id, &agent_id, &db, None)
        .await
        .expect("red-queen stage should complete with failure output");

    assert!(!output.success);
    assert_eq!(
        output.feedback,
        "No QA test_results artifact found for red-queen stage"
    );
}
