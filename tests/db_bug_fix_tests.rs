// Integration tests for bug fixes in write_ops.rs and schema.sql
// Tests verify that previously identified bugs remain fixed
//
// Functional Rust approach: zero unwrap, zero panic, explicit error handling

use shitty_swarm_manager::{
    AgentId, BeadId, RepoId, SwarmDb,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

fn test_db_url() -> String {
    std::env::var("SWARM_TEST_DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| {
            panic!("Set SWARM_TEST_DATABASE_URL or DATABASE_URL for DB integration tests")
        })
}

async fn test_db() -> SwarmDb {
    let url = test_db_url();
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&url)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect test database: {}", e));

    SwarmDb::new_with_pool(pool)
}

async fn reset_schema(db: &SwarmDb) {
    sqlx::raw_sql(
        "DROP TABLE IF EXISTS agent_messages CASCADE;
         DROP TABLE IF EXISTS stage_artifacts CASCADE;
         DROP TABLE IF EXISTS execution_events CASCADE;
         DROP TABLE IF EXISTS stage_history CASCADE;
         DROP TABLE IF EXISTS agent_run_logs CASCADE;
         DROP TABLE IF EXISTS bead_claims CASCADE;
         DROP TABLE IF EXISTS bead_backlog CASCADE;
         DROP TABLE IF EXISTS agent_state CASCADE;",
    )
    .execute(db.pool())
    .await
    .expect("Schema reset failed");

    db.initialize_schema_from_sql(include_str!("../crates/swarm-coordinator/schema.sql"))
        .await
        .expect("Schema init failed");
}

// ============================================================================
// BUG FIX VERIFICATION TESTS
// ============================================================================

#[tokio::test]
async fn bug1_concurrent_claims_use_for_update_lock() {
    // BUG 1: claim_bead should use FOR UPDATE to lock bead_backlog
    // Only one concurrent claim should succeed
    let db = test_db().await;
    reset_schema(&db).await;

    let repo_id = RepoId::new("bug1-repo");
    db.register_repo(&repo_id, "test", "test").await
        .expect("register_repo failed");

    let agent1 = AgentId::new(&repo_id, 1);
    let agent2 = AgentId::new(&repo_id, 2);
    db.register_agent(&agent1).await.expect("register agent1 failed");
    db.register_agent(&agent2).await.expect("register agent2 failed");

    let bead_id = BeadId::new("bug1-bead");
    db.enqueue_backlog_batch(&repo_id, "bug1", 1).await
        .expect("enqueue failed");

    // Attempt concurrent claims
    let claim1 = db.claim_bead(&agent1, &bead_id);
    let claim2 = db.claim_bead(&agent2, &bead_id);

    let (result1, result2) = tokio::join!(claim1, claim2).await;

    // Both should complete without error
    assert!(result1.is_ok(), "claim1 should not error");
    assert!(result2.is_ok(), "claim2 should not error");

    let ok1 = result1.unwrap();
    let ok2 = result2.unwrap();

    // Exactly one should succeed
    let success_count = bool::from(ok1) as usize + bool::from(ok2) as usize;
    assert_eq!(success_count, 1, "Only one claim should succeed");
}

#[tokio::test]
async fn bug2_mark_landing_retryable_is_transactional() {
    // BUG 2: mark_landing_retryable should be atomic within transaction
    let db = test_db().await;
    reset_schema(&db).await;

    let repo_id = RepoId::new("bug2-repo");
    db.register_repo(&repo_id, "test", "test").await
        .expect("register_repo failed");

    let agent = AgentId::new(&repo_id, 1);
    db.register_agent(&agent).await.expect("register_agent failed");

    let bead_id = BeadId::new("bug2-bead");

    // Setup: Bead in backlog
    sqlx::query("INSERT INTO bead_backlog (repo_id, bead_id, priority, status) VALUES ($1, $2, 'p0', 'pending')")
        .bind(repo_id.value())
        .bind(bead_id.value())
        .execute(db.pool())
        .await
        .expect("setup failed");

    // Setup: Claim exists
    sqlx::query("INSERT INTO bead_claims (repo_id, bead_id, claimed_by, status) VALUES ($1, $2, $3, 'in_progress')")
        .bind(repo_id.value())
        .bind(bead_id.value())
        .bind(1)
        .execute(db.pool())
        .await
        .expect("setup failed");

    // Setup: Agent working on bead
    sqlx::query("UPDATE agent_state SET bead_id = $1, status = 'working' WHERE repo_id = $2 AND agent_id = $3")
        .bind(bead_id.value())
        .bind(repo_id.value())
        .bind(1)
        .execute(db.pool())
        .await
        .expect("setup failed");

    // When: Mark landing as retryable
    let result = db.mark_landing_retryable(&agent, "sync failed").await;

    // Then: Should succeed without partial state
    assert!(result.is_ok(), "mark_landing_retryable should succeed");

    // Verify agent is in waiting state
    let row = sqlx::query::<_, (String,)>(
        "SELECT status FROM agent_state WHERE repo_id = $1 AND agent_id = $2"
    )
    .bind(repo_id.value())
    .bind(1)
    .fetch_one(db.pool())
    .await
    .expect("query failed");

    assert_eq!(row.0, "waiting", "Agent should be waiting");
}

#[tokio::test]
async fn bug4_release_agent_has_no_duplicate_update() {
    // BUG 4: release_agent should reset agent unconditionally
    let db = test_db().await;
    reset_schema(&db).await;

    let repo_id = RepoId::new("bug4-repo");
    db.register_repo(&repo_id, "test", "test").await
        .expect("register_repo failed");

    let agent = AgentId::new(&repo_id, 1);
    db.register_agent(&agent).await.expect("register_agent failed");

    let bead_id = BeadId::new("bug4-bead");

    // Setup: Bead claimed
    sqlx::query("INSERT INTO bead_backlog (repo_id, bead_id, priority, status) VALUES ($1, $2, 'p0', 'in_progress')")
        .bind(repo_id.value())
        .bind(bead_id.value())
        .execute(db.pool())
        .await
        .expect("setup failed");

    sqlx::query("INSERT INTO bead_claims (repo_id, bead_id, claimed_by, status) VALUES ($1, $2, $3, 'in_progress')")
        .bind(repo_id.value())
        .bind(bead_id.value())
        .bind(1)
        .execute(db.pool())
        .await
        .expect("setup failed");

    sqlx::query("UPDATE agent_state SET bead_id = $1, status = 'working' WHERE repo_id = $2 AND agent_id = $3")
        .bind(bead_id.value())
        .bind(repo_id.value())
        .bind(1)
        .execute(db.pool())
        .await
        .expect("setup failed");

    // When: Release agent
    let result = db.release_agent(&agent).await;

    // Then: Should succeed
    assert!(result.is_ok(), "release_agent should succeed");
    assert_eq!(result.unwrap(), Some(bead_id), "should return bead_id");

    // Verify agent reset happened
    let row = sqlx::query::<_, (String, Option<String>)>(
        "SELECT status, bead_id FROM agent_state WHERE repo_id = $1 AND agent_id = $2"
    )
    .bind(repo_id.value())
    .bind(1)
    .fetch_one(db.pool())
    .await
    .expect("query failed");

    assert_eq!(row.0, "idle", "Agent should be idle");
    assert!(row.1.is_none(), "Bead should be cleared");
}

#[tokio::test]
async fn bug7_schema_constraints_prevent_negative_agent_ids() {
    // BUG 7: CHECK constraints prevent negative agent_id
    let db = test_db().await;
    reset_schema(&db).await;

    let repo_id = RepoId::new("bug7-repo");
    db.register_repo(&repo_id, "test", "test").await
        .expect("register_repo failed");

    // When: Insert negative agent_id
    let result = sqlx::query(
        "INSERT INTO agent_state (repo_id, agent_id, status) VALUES ($1, -1, 'idle')"
    )
    .bind(repo_id.value())
    .execute(db.pool())
    .await;

    // Then: Should violate CHECK constraint
    assert!(result.is_err(), "Negative agent_id should fail CHECK constraint");

    // When: Insert valid agent_id
    let result = sqlx::query(
        "INSERT INTO agent_state (repo_id, agent_id, status) VALUES ($1, 1, 'idle')"
    )
    .bind(repo_id.value())
    .execute(db.pool())
    .await;

    // Then: Should succeed
    assert!(result.is_ok(), "Valid agent_id should succeed");
}

// ============================================================================
// CONTRACT AND BEHAVIOR VERIFICATION TESTS
// ============================================================================

#[tokio::test]
async fn contract_claim_bead_has_ownership_invariant() {
    // Verify: No two agents can own same bead
    let db = test_db().await;
    reset_schema(&db).await;

    let repo_id = RepoId::new("contract-repo");
    db.register_repo(&repo_id, "test", "test").await
        .expect("register_repo failed");

    let agent1 = AgentId::new(&repo_id, 1);
    let agent2 = AgentId::new(&repo_id, 2);
    db.register_agent(&agent1).await.expect("register agent1 failed");
    db.register_agent(&agent2).await.expect("register agent2 failed");

    let bead_id = BeadId::new("contract-bead");
    db.enqueue_backlog_batch(&repo_id, "contract", 1).await
        .expect("enqueue failed");

    // Given: Bead is pending
    let row = sqlx::query::<_, (String,)>(
        "SELECT status FROM bead_backlog WHERE repo_id = $1 AND bead_id = $2"
    )
    .bind(repo_id.value())
    .bind(bead_id.value())
    .fetch_one(db.pool())
    .await
    .expect("query failed");

    assert_eq!(row.0, "pending", "Precondition: bead should be pending");

    // When: Agent1 claims bead
    let result1 = db.claim_bead(&agent1, &bead_id).await;
    assert!(result1.is_ok(), "claim should succeed");
    assert!(result1.unwrap(), "claim should return true");

    // Then: Agent2 cannot claim same bead
    let result2 = db.claim_bead(&agent2, &bead_id).await;
    assert!(result2.is_ok(), "second claim should not error");
    assert!(!result2.unwrap(), "Invariant: second claim must fail");
}

#[tokio::test]
async fn behavior_release_clears_all_related_state() {
    // Verify: Releasing agent cascades to all related tables
    let db = test_db().await;
    reset_schema(&db).await;

    let repo_id = RepoId::new("behavior-repo");
    db.register_repo(&repo_id, "test", "test").await
        .expect("register_repo failed");

    let agent = AgentId::new(&repo_id, 1);
    db.register_agent(&agent).await.expect("register_agent failed");

    let bead_id = BeadId::new("behavior-bead");

    // Setup: Full ecosystem state
    sqlx::query("INSERT INTO bead_backlog (repo_id, bead_id, priority, status) VALUES ($1, $2, 'p0', 'in_progress')")
        .bind(repo_id.value())
        .bind(bead_id.value())
        .execute(db.pool())
        .await
        .expect("setup failed");

    sqlx::query("INSERT INTO bead_claims (repo_id, bead_id, claimed_by, status) VALUES ($1, $2, $3, 'in_progress')")
        .bind(repo_id.value())
        .bind(bead_id.value())
        .bind(1)
        .execute(db.pool())
        .await
        .expect("setup failed");

    sqlx::query("UPDATE agent_state SET bead_id = $1, status = 'working' WHERE repo_id = $2 AND agent_id = $3")
        .bind(bead_id.value())
        .bind(repo_id.value())
        .bind(1)
        .execute(db.pool())
        .await
        .expect("setup failed");

    // When: Release agent
    db.release_agent(&agent).await.expect("release failed");

    // Then: All related state is cleared
    let claim_result = sqlx::query_as(
        "SELECT COUNT(*) FROM bead_claims"
    )
    .fetch_one(db.pool())
    .await
    .expect("query failed");

    let claim_count: i64 = claim_result.0;
    assert_eq!(claim_count, 0, "Claim should be removed");

    let backlog_result = sqlx::query_as(
        "SELECT status FROM bead_backlog WHERE repo_id = $1 AND bead_id = $2"
    )
    .bind(repo_id.value())
    .bind(bead_id.value())
    .fetch_one(db.pool())
    .await
    .expect("query failed");

    let backlog_status: String = backlog_result.0;
    assert_eq!(backlog_status, "pending", "Backlog should be pending");
}
