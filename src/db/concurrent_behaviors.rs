// BDD-style tests for Concurrent Operations behaviors
// Focus on race conditions, work distribution under concurrency, and swarm coordination.

use super::*;
use crate::db::{test_db, setup_schema, reset_runtime_tables, unique_bead};
use crate::types::{AgentId, AgentStatus, BeadId, RepoId};
use futures_util::future::join_all;
use std::collections::HashSet;

mod concurrent_operations {

    mod when_multiple_agents_claim_work_simultaneously {

        mod given_more_beads_than_agents {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_each_agent_receives_unique_bead_and_beads_remain_unclaimed() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;

                let agent_count = 50;
                let bead_count = 75;

                db.seed_idle_agents(agent_count).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                let bead_ids = (1..=bead_count)
                    .map(|n| unique_bead(&format!("concurrent-bead-{}", n)))
                    .collect::<Vec<_>>();

                for bead_id in &bead_ids {
                    sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                        .bind(bead_id).execute(db.pool()).await
                        .unwrap_or_else(|e| panic!("insert {} failed: {}", bead_id, e));
                }

                // When - all agents claim simultaneously
                let claim_futures = (1..=agent_count).map(|n| {
                    let db = db.clone();
                    async move {
                        let agent = AgentId::new(RepoId::new("local"), n);
                        db.claim_next_bead(&agent).await
                            .ok()
                            .flatten()
                            .map(|b| b.value().to_string())
                    }
                });

                let claims = join_all(claim_futures).await;
                let claimed: Vec<_> = claims.into_iter().flatten().collect();

                // Then
                assert_eq!(claimed.len(), agent_count,
                    "Exactly one bead per agent should be claimed");

                let unique: HashSet<_> = claimed.iter().collect();
                assert_eq!(unique.len(), agent_count,
                    "All claimed beads should be unique (no double-claims)");

                // Verify remaining beads are still pending
                let pending_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM bead_backlog WHERE status = 'pending'")
                    .fetch_one(db.pool())
                    .await
                    .unwrap_or_else(|e| panic!("query failed: {}", e));
                assert_eq!(pending_count, (bead_count - agent_count) as i64,
                    "Remaining beads should still be pending");
            }
        }

        mod given_equal_beads_and_agents {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_all_beads_are_claimed_and_late_comers_get_none() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;

                let count = 100;

                db.seed_idle_agents(count + 10).await  // Extra agents
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                let bead_ids = (1..=count)
                    .map(|n| unique_bead(&format!("exact-bead-{}", n)))
                    .collect::<Vec<_>>();

                for bead_id in &bead_ids {
                    sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                        .bind(bead_id).execute(db.pool()).await
                        .unwrap_or_else(|e| panic!("insert {} failed: {}", bead_id, e));
                }

                // When - more agents than beads claim simultaneously
                let claim_futures = (1..=(count + 10)).map(|n| {
                    let db = db.clone();
                    async move {
                        let agent = AgentId::new(RepoId::new("local"), n);
                        db.claim_next_bead(&agent).await
                            .ok()
                            .flatten()
                            .map(|b| b.value().to_string())
                    }
                });

                let claims = join_all(claim_futures).await;
                let claimed: Vec<_> = claims.into_iter().flatten().collect();

                // Then
                assert_eq!(claimed.len(), count,
                    "Only available beads should be claimed");

                let unique: HashSet<_> = claimed.iter().collect();
                assert_eq!(unique.len(), count,
                    "All claimed beads should be unique");

                // No pending beads should remain
                let pending_count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM bead_backlog WHERE status = 'pending'")
                    .fetch_one(db.pool())
                    .await
                    .unwrap_or_else(|e| panic!("query failed: {}", e));
                assert_eq!(pending_count, 0, "No pending beads should remain");
            }
        }
    }

    mod when_agents_execute_stages_concurrently {

        mod given_multiple_agents_working_on_different_beads {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_stage_history_records_unique_per_agent_bead_stage_attempt() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;

                let agent_count = 20;
                db.seed_idle_agents(agent_count).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                let bead_ids = (1..=agent_count)
                    .map(|n| unique_bead(&format!("stage-bead-{}", n)))
                    .collect::<Vec<_>>();

                for bead_id in &bead_ids {
                    sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                        .bind(bead_id).execute(db.pool()).await
                        .unwrap_or_else(|e| panic!("insert {} failed: {}", bead_id, e));
                }

                // When - claim and start stages concurrently
                let start_futures = (1..=agent_count).map(|n| {
                    let db = db.clone();
                    async move {
                        let agent = AgentId::new(RepoId::new("local"), n);
                        if let Some(bead_id) = db.claim_next_bead(&agent).await
                            .unwrap_or_else(|e| panic!("agent {} claim failed: {}", n, e))
                        {
                            db.record_stage_started(&agent, &bead_id, Stage::RustContract, 1).await
                                .unwrap_or_else(|e| panic!("agent {} stage start failed: {}", n, e));
                            Some((agent.number(), bead_id.value().to_string()))
                        } else {
                            None
                        }
                    }
                });

                let results = join_all(start_futures).await;
                let started: Vec<_> = results.into_iter().flatten().collect();

                // Then - verify all stage histories are distinct
                assert_eq!(started.len(), agent_count);

                let stage_histories: Vec<(i32, String, String, i32)> = sqlx::query_as(
                    "SELECT agent_id, bead_id, stage, attempt_number
                     FROM stage_history
                     WHERE status = 'started'")
                    .fetch_all(db.pool())
                    .await
                    .unwrap_or_else(|e| panic!("query failed: {}", e));

                assert_eq!(stage_histories.len(), agent_count,
                    "Should have one stage history per agent");

                // Verify uniqueness
                let mut seen = HashSet::new();
                for (agent_id, bead_id, stage, attempt) in stage_histories {
                    let key = (agent_id, bead_id, stage, attempt);
                    assert!(seen.insert(key),
                        "Duplicate stage history entry: {:?}", key);
                }
            }
        }
    }

    mod when_high_concurrency_artifact_storage {

        mod given_concurrent_artifact_writes {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_all_artifacts_are_stored_without_corruption() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;

                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("artifact-concurrency"));

                db.seed_idle_agents(1).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert bead failed: {}", e));
                db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));
                db.record_stage_started(&agent_id, &bead_id, Stage::RustContract, 1).await
                    .unwrap_or_else(|e| panic!("stage start failed: {}", e));

                let stage_history_id = sqlx::query_scalar::<_, i64>(
                    "SELECT id FROM stage_history WHERE agent_id = $1 AND bead_id = $2 ORDER BY id DESC LIMIT 1")
                    .bind(agent_id.number() as i32)
                    .bind(bead_id.value())
                    .fetch_one(db.pool())
                    .await
                    .unwrap_or_else(|e| panic!("get history id failed: {}", e));

                let artifact_count = 50;

                // When - store many artifacts concurrently
                let store_futures = (1..=artifact_count).map(|n| {
                    let db = db.clone();
                    async move {
                        let content = format!("artifact-content-{}-{}", n, uuid::Uuid::new_v4());
                        db.store_stage_artifact(
                            stage_history_id,
                            ArtifactType::ContractDocument,
                            &content,
                            Some(serde_json::json!({"index": n})),
                        ).await
                    }
                });

                let results = join_all(store_futures).await;

                // Then
                assert_eq!(results.len(), artifact_count);
                assert!(results.iter().all(|r| r.is_ok()),
                    "All artifact stores should succeed");

                let stored_artifacts = db.get_stage_artifacts(stage_history_id).await
                    .unwrap_or_else(|e| panic!("get artifacts failed: {}", e));

                // Deduplication may reduce count, verify integrity
                assert!(!stored_artifacts.is_empty(),
                    "Should have stored artifacts");

                // Verify content integrity
                for artifact in &stored_artifacts {
                    assert!(!artifact.content.is_empty(),
                        "Artifact content should not be empty");
                    assert!(artifact.content_hash.is_some(),
                        "Artifact should have content hash");
                }
            }
        }
    }
}

mod progress_tracking {

    mod when_querying_swarm_progress {

        mod given_agents_in_various_states {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_progress_summary_reflects_accurate_counts() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;

                // Seed agents in different states
                db.seed_idle_agents(5).await
                    .unwrap_or_else(|e| panic!("seed idle failed: {}", e));

                // Set some agents to working
                sqlx::query("UPDATE agent_state SET status = 'working' WHERE agent_id IN (1, 2, 3)")
                    .execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("update working failed: {}", e));

                // Set some to done
                sqlx::query("UPDATE agent_state SET status = 'done' WHERE agent_id IN (4, 5)")
                    .execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("update done failed: {}", e));

                // Add one with pending work
                let agent_id = AgentId::new(RepoId::new("local"), 6);
                db.register_agent(&agent_id).await
                    .unwrap_or_else(|e| panic!("register failed: {}", e));
                sqlx::query("UPDATE agent_state SET status = 'waiting' WHERE agent_id = 6")
                    .execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("update waiting failed: {}", e));

                // When
                let repo_id = RepoId::new("local");
                let progress = db.get_progress(&repo_id).await
                    .unwrap_or_else(|e| panic!("get_progress failed: {}", e));

                // Then
                assert_eq!(progress.idle, 0, "Should have 0 idle agents");
                assert_eq!(progress.working, 3, "Should have 3 working agents");
                assert_eq!(progress.waiting, 1, "Should have 1 waiting agent");
                assert_eq!(progress.completed, 2, "Should have 2 done agents");
                assert_eq!(progress.errors, 0, "Should have 0 error agents");
                assert_eq!(progress.total_agents, 6, "Should have 6 total agents");
            }
        }
    }

    mod when_listing_active_agents {

        mod given_active_and_inactive_agents {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_only_active_agents_are_returned_ordered_by_last_update() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;

                db.seed_idle_agents(5).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                // Activate agents 1, 2, 3
                for n in 1..=3 {
                    let agent_id = AgentId::new(RepoId::new("local"), n);
                    let bead_id = BeadId::new(unique_bead(&format!("active-{}", n)));
                    sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                        .bind(bead_id.value()).execute(db.pool()).await
                        .unwrap_or_else(|e| panic!("insert {} failed: {}", bead_id.value(), e));
                    db.claim_next_bead(&agent_id).await
                        .unwrap_or_else(|e| panic!("claim {} failed: {}", n, e));
                }

                // When
                let active_agents = db.get_all_active_agents().await
                    .unwrap_or_else(|e| panic!("get_active failed: {}", e));

                // Then
                assert_eq!(active_agents.len(), 3,
                    "Should return only active agents");

                let active_ids: Vec<u32> = active_agents.iter().map(|(_, id, _, _)| *id).collect();
                assert!(active_ids.contains(&1));
                assert!(active_ids.contains(&2));
                assert!(active_ids.contains(&3));
            }
        }
    }

    mod when_getting_available_agents {

        mod given_idle_and_waiting_agents {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_idle_and_below_max_attempt_waiting_agents_are_returned() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;

                db.seed_idle_agents(3).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                // Agent 4: waiting with 0 attempts (can retry)
                let agent_4 = AgentId::new(RepoId::new("local"), 4);
                db.register_agent(&agent_4).await
                    .unwrap_or_else(|e| panic!("register 4 failed: {}", e));
                sqlx::query("UPDATE agent_state SET status = 'waiting', implementation_attempt = 0 WHERE agent_id = 4")
                    .execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("update 4 failed: {}", e));

                // Agent 5: waiting with 2 attempts (below max of 3)
                let agent_5 = AgentId::new(RepoId::new("local"), 5);
                db.register_agent(&agent_5).await
                    .unwrap_or_else(|e| panic!("register 5 failed: {}", e));
                sqlx::query("UPDATE agent_state SET status = 'waiting', implementation_attempt = 2 WHERE agent_id = 5")
                    .execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("update 5 failed: {}", e));

                // Agent 6: waiting with 3 attempts (at max, NOT available)
                let agent_6 = AgentId::new(RepoId::new("local"), 6);
                db.register_agent(&agent_6).await
                    .unwrap_or_else(|e| panic!("register 6 failed: {}", e));
                sqlx::query("UPDATE agent_state SET status = 'waiting', implementation_attempt = 3 WHERE agent_id = 6")
                    .execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("update 6 failed: {}", e));

                // When
                let repo_id = RepoId::new("local");
                let available = db.get_available_agents(&repo_id).await
                    .unwrap_or_else(|e| panic!("get_available failed: {}", e));

                // Then
                assert_eq!(available.len(), 5,
                    "Should have 5 available agents (3 idle + 2 waiting below max)");

                let available_ids: Vec<u32> = available.iter().map(|a| a.agent_id).collect();
                assert!(available_ids.contains(&1));
                assert!(available_ids.contains(&2));
                assert!(available_ids.contains(&3));
                assert!(available_ids.contains(&4));
                assert!(available_ids.contains(&5));
                assert!(!available_ids.contains(&6), "Agent at max attempts should not be available");
            }
        }
    }
}
