#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use crate::types::{ArtifactType, BeadId, RepoId};
use crate::{AgentId, SwarmDb};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use super::implement_stage::execute_implement_stage;

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

#[tokio::test]
async fn given_first_implement_attempt_when_contract_exists_then_only_contract_context_is_loaded() {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("test-bead-1");
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    setup_schema(&db).await;
    insert_bead_claim(&pool, &bead_id, &agent_id).await;

    let stage_history_id = insert_started_stage_history(&pool, &bead_id, &agent_id).await;
    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::ContractDocument,
        "contract content",
        None,
    )
    .await
    .expect("Failed to store contract");

    let result = execute_implement_stage(&bead_id, &agent_id, &db)
        .await
        .expect("execute_implement_stage should succeed");

    assert!(result.success);
    assert!(result.implementation_code.is_some());

    let implementation = result
        .implementation_code
        .as_deref()
        .expect("implementation code should exist");
    assert!(implementation.contains("## Contract Document\ncontract content"));
    assert!(!implementation.contains("## Retry Packet"));
    assert!(!implementation.contains("## Test Output"));
}

#[tokio::test]
async fn given_retry_implement_attempt_when_retry_artifacts_exist_then_retry_context_is_loaded() {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("test-bead-2");
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    setup_schema(&db).await;
    insert_bead_claim(&pool, &bead_id, &agent_id).await;

    sqlx::query(
        "INSERT INTO agent_state (agent_id, status, implementation_attempt) VALUES ($1, 'working', 1)",
    )
    .bind(agent_id.number().cast_signed())
    .execute(&pool)
    .await
    .expect("Failed to seed implementation attempt");

    let stage_history_id = insert_started_stage_history(&pool, &bead_id, &agent_id).await;
    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::ContractDocument,
        "contract content",
        None,
    )
    .await
    .expect("Failed to store contract");

    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::RetryPacket,
        r#"{"attempt": 1, "failure_reason": "test failed", "remaining_attempts": 2}"#,
        None,
    )
    .await
    .expect("Failed to store retry packet");

    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::TestOutput,
        "test failure output",
        None,
    )
    .await
    .expect("Failed to store test output");

    let result = execute_implement_stage(&bead_id, &agent_id, &db)
        .await
        .expect("execute_implement_stage should succeed");

    assert!(result.success);
    assert!(result.implementation_code.is_some());

    let implementation = result
        .implementation_code
        .as_deref()
        .expect("implementation code should exist");
    assert!(implementation.contains("## Contract Document\ncontract content"));
    assert!(implementation.contains("## Retry Packet"));
    assert!(implementation.contains("\"failure_reason\": \"test failed\""));
    assert!(implementation.contains("## Test Output\ntest failure output"));
}

#[tokio::test]
async fn given_first_implement_attempt_when_contract_missing_then_failure_output_is_returned() {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("test-bead-3");
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    setup_schema(&db).await;
    insert_bead_claim(&pool, &bead_id, &agent_id).await;

    let result = execute_implement_stage(&bead_id, &agent_id, &db)
        .await
        .expect("execute_implement_stage should return error");

    assert!(!result.success);
    assert!(result.feedback.contains("contract"));
}

#[tokio::test]
async fn given_retry_attempt_when_retry_packet_missing_then_failure_output_returned() {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("test-bead-retry-missing");
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    setup_schema(&db).await;
    insert_bead_claim(&pool, &bead_id, &agent_id).await;

    sqlx::query(
        "INSERT INTO agent_state (agent_id, status, implementation_attempt) VALUES ($1, 'working', 1)",
    )
    .bind(agent_id.number().cast_signed())
    .execute(&pool)
    .await
    .expect("Failed to seed implementation attempt");

    let stage_history_id = insert_started_stage_history(&pool, &bead_id, &agent_id).await;
    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::ContractDocument,
        "contract content",
        None,
    )
    .await
    .expect("Failed to store contract");

    let result = execute_implement_stage(&bead_id, &agent_id, &db)
        .await
        .expect("execute_implement_stage should return error");

    assert!(!result.success);
    assert!(result.feedback.contains("retry packet"));
}

#[tokio::test]
async fn given_agent_state_missing_when_executing_then_defaults_to_zero_attempts() {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("test-bead-no-agent-state");
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    setup_schema(&db).await;
    insert_bead_claim(&pool, &bead_id, &agent_id).await;

    let stage_history_id = insert_started_stage_history(&pool, &bead_id, &agent_id).await;
    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::ContractDocument,
        "contract content",
        None,
    )
    .await
    .expect("Failed to store contract");

    let result = execute_implement_stage(&bead_id, &agent_id, &db)
        .await
        .expect("execute_implement_stage should succeed");

    assert!(result.success);
    assert!(!result.feedback.contains("retry packet"));
    assert!(result.implementation_code.is_some());
}

#[tokio::test]
async fn given_all_optional_artifacts_missing_when_executing_then_only_contract_in_context() {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("test-bead-only-contract");
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    setup_schema(&db).await;
    insert_bead_claim(&pool, &bead_id, &agent_id).await;

    let stage_history_id = insert_started_stage_history(&pool, &bead_id, &agent_id).await;
    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::ContractDocument,
        "contract content",
        None,
    )
    .await
    .expect("Failed to store contract");

    let result = execute_implement_stage(&bead_id, &agent_id, &db)
        .await
        .expect("execute_implement_stage should succeed");

    assert!(result.success);
    let implementation = result
        .implementation_code
        .expect("implementation code should exist");
    assert!(implementation.contains("## Contract Document\ncontract content"));
    assert!(!implementation.contains("## Retry Packet"));
    assert!(!implementation.contains("## Failure Details"));
    assert!(!implementation.contains("## Test Results"));
    assert!(!implementation.contains("## Test Output"));
}

#[tokio::test]
async fn given_mixed_optional_artifacts_when_executing_then_correct_sections_included() {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("test-bead-mixed-optionals");
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    setup_schema(&db).await;
    insert_bead_claim(&pool, &bead_id, &agent_id).await;

    let stage_history_id = insert_started_stage_history(&pool, &bead_id, &agent_id).await;
    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::ContractDocument,
        "contract content",
        None,
    )
    .await
    .expect("Failed to store contract");
    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::FailureDetails,
        "failure details content",
        None,
    )
    .await
    .expect("Failed to store failure details");
    db.store_stage_artifact(
        stage_history_id,
        ArtifactType::TestOutput,
        "test output content",
        None,
    )
    .await
    .expect("Failed to store test output");

    let result = execute_implement_stage(&bead_id, &agent_id, &db)
        .await
        .expect("execute_implement_stage should succeed");

    assert!(result.success);
    let implementation = result
        .implementation_code
        .expect("implementation code should exist");
    assert!(implementation.contains("## Contract Document\ncontract content"));
    assert!(implementation.contains("## Failure Details\nfailure details content"));
    assert!(implementation.contains("## Test Output\ntest output content"));
    assert!(!implementation.contains("## Retry Packet"));
    assert!(!implementation.contains("## Test Results"));
}

#[tokio::test]
async fn given_empty_contract_content_when_executing_then_empty_context_passed_to_scaffold() {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("test-bead-empty-contract");
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    setup_schema(&db).await;
    insert_bead_claim(&pool, &bead_id, &agent_id).await;

    let stage_history_id = insert_started_stage_history(&pool, &bead_id, &agent_id).await;
    db.store_stage_artifact(stage_history_id, ArtifactType::ContractDocument, "", None)
        .await
        .expect("Failed to store empty contract");

    let result = execute_implement_stage(&bead_id, &agent_id, &db)
        .await
        .expect("execute_implement_stage should succeed");

    assert!(result.success);
    assert!(result.implementation_code.is_some());
}

#[tokio::test]
async fn given_all_sections_empty_when_executing_then_empty_context_to_scaffold() {
    let Some(pool) = test_pool_or_skip().await else {
        eprintln!("Skipping DB-dependent test: no reachable test database");
        return;
    };
    let db = SwarmDb::new_with_pool(pool.clone());
    let bead_id = BeadId::new("test-bead-all-empty");
    let agent_id = AgentId::new(RepoId::new("local"), 1);
    setup_schema(&db).await;
    insert_bead_claim(&pool, &bead_id, &agent_id).await;

    let stage_history_id = insert_started_stage_history(&pool, &bead_id, &agent_id).await;
    db.store_stage_artifact(stage_history_id, ArtifactType::ContractDocument, "", None)
        .await
        .expect("Failed to store empty contract");

    let result = execute_implement_stage(&bead_id, &agent_id, &db)
        .await
        .expect("execute_implement_stage should succeed");

    assert!(result.success);
    assert!(result.implementation_code.is_some());
}
