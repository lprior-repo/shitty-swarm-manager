#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use crate::gate_cache::GateExecutionCache;
use crate::types::{ArtifactType, BeadId, RepoId};
use crate::{AgentId, SwarmDb};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use super::gate_stage::execute_red_queen_stage;

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
async fn given_cached_success_gate_and_test_results_artifact_when_executing_red_queen_stage_then_quality_report_is_emitted(
) {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("rq-success-cache");
    let agent_id = AgentId::new(RepoId::new("local"), 141);
    setup_schema(&db).await;
    seed_artifact(
        &db,
        &pool,
        &bead_id,
        &agent_id,
        ArtifactType::TestResults,
        r#"{"passed":11,"failed":0}"#,
    )
    .await;

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");
    cache
        .put(
            ":test".to_string(),
            true,
            Some(0),
            "all adversarial checks passed".to_string(),
            String::new(),
        )
        .await
        .expect("cache write");

    let output = execute_red_queen_stage(&bead_id, &agent_id, &db, Some(&cache))
        .await
        .expect("red-queen stage should run from cache");

    assert!(output.success);
    assert!(output.artifacts.contains_key("quality_gate_report"));
    assert!(output.adversarial_report.is_none());
}

#[tokio::test]
async fn given_cached_failed_gate_and_test_results_artifact_when_executing_red_queen_stage_then_adversarial_report_is_emitted(
) {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("rq-failed-cache");
    let agent_id = AgentId::new(RepoId::new("local"), 14);
    setup_schema(&db).await;
    seed_artifact(
        &db,
        &pool,
        &bead_id,
        &agent_id,
        ArtifactType::TestResults,
        r#"{"passed":10,"failed":1}"#,
    )
    .await;

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");
    cache
        .put(
            ":test".to_string(),
            false,
            Some(1),
            "adversarial regression found".to_string(),
            String::new(),
        )
        .await
        .expect("cache write");

    let output = execute_red_queen_stage(&bead_id, &agent_id, &db, Some(&cache))
        .await
        .expect("red-queen stage should run from cache");

    assert!(!output.success);
    assert_eq!(
        output.adversarial_report.as_deref(),
        Some("adversarial regression found")
    );
    assert!(output.artifacts.contains_key("adversarial_report"));
}
