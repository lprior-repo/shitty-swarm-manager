// BDD-style tests for Agent Lifecycle behaviors
// Following Martin Fowler's approach: clear domain language, GWT structure,
// focus on business behaviors rather than implementation details.

use super::*;
use crate::db::{test_db, setup_schema, reset_runtime_tables, unique_bead};
use crate::types::{AgentId, AgentStatus, RepoId, BeadId, Stage, StageResult, SwarmStatus};
use sqlx::PgPoolOptions;

mod agent_lifecycle {

    mod when_registering {

        mod given_an_unregistered_agent {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_agent_is_created_with_idle_status() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);

                // When
                let was_new = db.register_agent(&agent_id).await
                    .unwrap_or_else(|e| panic!("Failed to register agent: {}", e));

                // Then
                assert!(was_new, "Agent should be registered as new");
                let state = db.get_agent_state(&agent_id).await
                    .unwrap_or_else(|e| panic!("Failed to get agent state: {}", e));
                assert!(state.is_some(), "Agent state should exist");
                let state = state.unwrap();
                assert_eq!(state.status, AgentStatus::Idle, "New agent should be idle");
                assert_eq!(state.agent_id.number(), 1, "Agent ID should match");
            }

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_duplicate_registration_returns_false_and_preserves_state() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                db.register_agent(&agent_id).await
                    .unwrap_or_else(|e| panic!("Initial registration failed: {}", e));

                // When
                let was_new = db.register_agent(&agent_id).await
                    .unwrap_or_else(|e| panic!("Second registration failed: {}", e));

                // Then
                assert!(!was_new, "Duplicate registration should return false");
                let state = db.get_agent_state(&agent_id).await
                    .unwrap_or_else(|e| panic!("Failed to get state: {}", e))
                    .expect("State should exist");
                assert_eq!(state.status, AgentStatus::Idle, "Status should be unchanged");
            }
        }

        mod given_multiple_agents {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_all_agents_are_registered_with_unique_ids() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let count = 12;

                // When
                db.seed_idle_agents(count).await
                    .unwrap_or_else(|e| panic!("seed_idle_agents failed: {}", e));

                // Then
                for n in 1..=count {
                    let agent_id = AgentId::new(RepoId::new("local"), n);
                    let state = db.get_agent_state(&agent_id).await
                        .unwrap_or_else(|e| panic!("Failed to get agent {} state: {}", n, e))
                        .unwrap_or_else(|| panic!("Agent {} should exist", n));
                    assert_eq!(state.status, AgentStatus::Idle, "Agent {} should be idle", n);
                }
            }
        }
    }

    mod when_claiming_work {

        mod given_an_idle_agent_and_pending_beads {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_agent_claims_earliest_pending_bead_and_transitions_to_working() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("early-bead"));
                let later_bead = BeadId::new(unique_bead("late-bead"));

                db.seed_idle_agents(1).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(&bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert early bead failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(&later_bead.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert late bead failed: {}", e));

                // When
                let claimed = db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));

                // Then
                assert_eq!(claimed.as_ref().map(BeadId::value), Some(bead_id.value()),
                    "Should claim earliest bead");
                let state = db.get_agent_state(&agent_id).await
                    .unwrap_or_else(|e| panic!("get state failed: {}", e))
                    .expect("state should exist");
                assert_eq!(state.status, AgentStatus::Working, "Agent should be working");
                assert_eq!(state.bead_id.as_ref().map(BeadId::value), Some(bead_id.value().to_string()),
                    "Agent should have bead assigned");
                assert_eq!(state.current_stage, Some(Stage::RustContract),
                    "Agent should start at rust-contract stage");
            }
        }

        mod given_no_pending_beads {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_agent_receives_none_and_remains_idle() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);

                db.seed_idle_agents(1).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                // When
                let claimed = db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));

                // Then
                assert!(claimed.is_none(), "Should have no beads to claim");
                let state = db.get_agent_state(&agent_id).await
                    .unwrap_or_else(|e| panic!("get state failed: {}", e))
                    .expect("state should exist");
                assert_eq!(state.status, AgentStatus::Idle, "Agent should remain idle");
                assert!(state.bead_id.is_none(), "Agent should have no bead assigned");
            }
        }
    }

    mod when_executing_stages {

        mod given_a_claimed_bead_at_initial_stage {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_successful_stage_advances_to_next_stage() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("stage-advancement"));

                db.seed_idle_agents(1).await.unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert bead failed: {}", e));
                db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));
                db.record_stage_started(&agent_id, &bead_id, Stage::RustContract, 1).await
                    .unwrap_or_else(|e| panic!("stage start failed: {}", e));

                // When
                db.record_stage_complete(&agent_id, &bead_id, Stage::RustContract, 1,
                    StageResult::Passed, 150).await
                    .unwrap_or_else(|e| panic!("stage complete failed: {}", e));

                // Then
                let state = db.get_agent_state(&agent_id).await
                    .unwrap_or_else(|e| panic!("get state failed: {}", e))
                    .expect("state should exist");
                assert_eq!(state.current_stage, Some(Stage::Implement),
                    "Should advance to implement stage");
                assert_eq!(state.status, AgentStatus::Working,
                    "Agent should still be working");
            }

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_failed_stage_sets_agent_to_waiting_and_increments_attempt() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("stage-failure"));

                db.seed_idle_agents(1).await.unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert bead failed: {}", e));
                db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));
                db.record_stage_started(&agent_id, &bead_id, Stage::Implement, 1).await
                    .unwrap_or_else(|e| panic!("stage start failed: {}", e));

                // When
                db.record_stage_complete(&agent_id, &bead_id, Stage::Implement, 1,
                    StageResult::Failed("tests failed".to_string()), 200).await
                    .unwrap_or_else(|e| panic!("stage complete failed: {}", e));

                // Then
                let state = db.get_agent_state(&agent_id).await
                    .unwrap_or_else(|e| panic!("get state failed: {}", e))
                    .expect("state should exist");
                assert_eq!(state.status, AgentStatus::Waiting,
                    "Agent should be waiting for feedback");
                assert_eq!(state.implementation_attempt, 1,
                    "Should increment attempt count");
                assert_eq!(state.current_stage, Some(Stage::Implement),
                    "Should return to implement stage");
                assert_eq!(state.feedback.as_deref(), Some("tests failed"),
                    "Should store failure feedback");
            }
        }

        mod given_final_stage_completion {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_agent_and_bead_are_finalized() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("final-stage"));

                db.seed_idle_agents(1).await.unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert bead failed: {}", e));
                db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));
                db.record_stage_started(&agent_id, &bead_id, Stage::RedQueen, 1).await
                    .unwrap_or_else(|e| panic!("stage start failed: {}", e));

                // When
                db.record_stage_complete(&agent_id, &bead_id, Stage::RedQueen, 1,
                    StageResult::Passed, 100).await
                    .unwrap_or_else(|e| panic!("stage complete failed: {}", e));

                // Then
                let state = db.get_agent_state(&agent_id).await
                    .unwrap_or_else(|e| panic!("get state failed: {}", e))
                    .expect("state should exist");
                assert_eq!(state.status, AgentStatus::Done,
                    "Agent should be done");
                assert_eq!(state.current_stage, Some(Stage::Done),
                    "Stage should be done");

                let claim_status: Option<String> = sqlx::query_scalar(
                    "SELECT status FROM bead_claims WHERE bead_id = $1")
                    .bind(bead_id.value()).fetch_optional(db.pool()).await
                    .unwrap_or_else(|e| panic!("query failed: {}", e));
                assert_eq!(claim_status.as_deref(), Some("completed"),
                    "Bead claim should be completed");
            }
        }
    }

    mod when_releasing {

        mod given_an_agent_with_claimed_bead {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_agent_resets_to_idle_and_bead_returns_to_backlog() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("release-test"));

                db.seed_idle_agents(1).await.unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert bead failed: {}", e));
                db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));

                // When
                let released = db.release_agent(&agent_id).await
                    .unwrap_or_else(|e| panic!("release failed: {}", e));

                // Then
                assert_eq!(released.as_ref().map(BeadId::value), Some(bead_id.value()),
                    "Should return released bead");
                let state = db.get_agent_state(&agent_id).await
                    .unwrap_or_else(|e| panic!("get state failed: {}", e))
                    .expect("state should exist");
                assert_eq!(state.status, AgentStatus::Idle,
                    "Agent should be idle");
                assert!(state.bead_id.is_none(), "Agent should have no bead");
                assert_eq!(state.implementation_attempt, 0,
                    "Attempts should reset");

                let backlog_status: Option<String> = sqlx::query_scalar(
                    "SELECT status FROM bead_backlog WHERE bead_id = $1")
                    .bind(bead_id.value()).fetch_optional(db.pool()).await
                    .unwrap_or_else(|e| panic!("query failed: {}", e));
                assert_eq!(backlog_status.as_deref(), Some("pending"),
                    "Bead should be pending in backlog");

                let claim_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM bead_claims WHERE bead_id = $1")
                    .bind(bead_id.value()).fetch_one(db.pool()).await
                    .unwrap_or_else(|e| panic!("query failed: {}", e));
                assert_eq!(claim_count, 0, "Bead claim should be deleted");
            }
        }
    }

    mod when_exceeding_max_attempts {

        mod given_an_agent_at_max_implementation_attempts {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_bead_is_marked_blocked_and_agent_errors() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("max-attempts"));

                db.seed_idle_agents(1).await.unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert bead failed: {}", e));
                db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));

                // Simulate 3 attempts
                for i in 1..=3 {
                    db.record_stage_started(&agent_id, &bead_id, Stage::Implement, i).await
                        .unwrap_or_else(|e| panic!("stage start {} failed: {}", i, e));
                    db.record_stage_complete(&agent_id, &bead_id, Stage::Implement, i,
                        StageResult::Failed("attempt failed".to_string()), 100).await
                        .unwrap_or_else(|e| panic!("stage complete {} failed: {}", i, e));
                }

                // When
                db.mark_bead_blocked(&agent_id, &bead_id, "Max attempts (3) exceeded").await
                    .unwrap_or_else(|e| panic!("mark_bead_blocked failed: {}", e));

                // Then
                let state = db.get_agent_state(&agent_id).await
                    .unwrap_or_else(|e| panic!("get state failed: {}", e))
                    .expect("state should exist");
                assert_eq!(state.status, AgentStatus::Error,
                    "Agent should be in error state");
                assert_eq!(state.feedback.as_deref(), Some("Max attempts (3) exceeded"),
                    "Should store block reason");

                let claim_status: Option<String> = sqlx::query_scalar(
                    "SELECT status FROM bead_claims WHERE bead_id = $1")
                    .bind(bead_id.value()).fetch_optional(db.pool()).await
                    .unwrap_or_else(|e| panic!("query failed: {}", e));
                assert_eq!(claim_status.as_deref(), Some("blocked"),
                    "Bead claim should be blocked");

                let backlog_status: Option<String> = sqlx::query_scalar(
                    "SELECT status FROM bead_backlog WHERE bead_id = $1")
                    .bind(bead_id.value()).fetch_optional(db.pool()).await
                    .unwrap_or_else(|e| panic!("query failed: {}", e));
                assert_eq!(backlog_status.as_deref(), Some("blocked"),
                    "Bead backlog entry should be blocked");
            }
        }
    }
}
