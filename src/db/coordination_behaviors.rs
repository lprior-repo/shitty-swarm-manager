// BDD-style tests for Agent Coordination behaviors
// Focus on message passing, artifact storage, and agent-to-agent communication.

use super::*;
use crate::db::{test_db, setup_schema, reset_runtime_tables, unique_bead};
use crate::types::{AgentId, ArtifactType, BeadId, MessageType, RepoId, Stage, StageResult};
use sqlx::PgPoolOptions;

mod agent_coordination {

    mod when_sending_messages {

        mod given_two_agents {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_message_is_delivered_to_recipient_unread() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let from_agent = AgentId::new(RepoId::new("local"), 1);
                let to_agent = AgentId::new(RepoId::new("local"), 2);
                let bead_id = BeadId::new(unique_bead("msg-bead"));

                db.seed_idle_agents(2).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                // When
                let message_id = db.send_agent_message(
                    &from_agent,
                    Some(&to_agent),
                    Some(&bead_id),
                    MessageType::QaFailed,
                    ("QA found issues", "3 tests failed, 2 warnings"),
                    None,
                ).await.unwrap_or_else(|e| panic!("send failed: {}", e));

                // Then
                let unread = db.get_unread_messages(&to_agent, Some(&bead_id)).await
                    .unwrap_or_else(|e| panic!("get_unread failed: {}", e));

                assert_eq!(unread.len(), 1, "Should have one unread message");
                assert_eq!(unread[0].id, message_id, "Message ID should match");
                assert_eq!(unread[0].from_repo_id, from_agent.repo_id().value(),
                    "From repo should match");
                assert_eq!(unread[0].from_agent_id, from_agent.number(),
                    "From agent should match");
                assert_eq!(unread[0].message_type, MessageType::QaFailed,
                    "Message type should match");
                assert_eq!(unread[0].subject, "QA found issues",
                    "Subject should match");
                assert_eq!(unread[0].body, "3 tests failed, 2 warnings",
                    "Body should match");
                assert!(!unread[0].read, "Message should be unread");
                assert!(unread[0].read_at.is_none(), "Read timestamp should be None");
            }

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_metadata_is_stored_with_message() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let from_agent = AgentId::new(RepoId::new("local"), 1);
                let to_agent = AgentId::new(RepoId::new("local"), 2);
                let metadata = serde_json::json!({
                    "stage": "qa-enforcer",
                    "attempt": 2,
                    "test_count": 42,
                    "failures": vec!["test_foo", "test_bar"]
                });

                db.seed_idle_agents(2).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                // When
                let message_id = db.send_agent_message(
                    &from_agent,
                    Some(&to_agent),
                    None,
                    MessageType::QaFailed,
                    ("Metadata test", "Checking metadata storage"),
                    Some(metadata.clone()),
                ).await.unwrap_or_else(|e| panic!("send failed: {}", e));

                // Then
                let unread = db.get_unread_messages(&to_agent, None).await
                    .unwrap_or_else(|e| panic!("get_unread failed: {}", e));

                assert_eq!(unread.len(), 1);
                assert_eq!(unread[0].id, message_id);
                assert_eq!(unread[0].metadata, Some(metadata),
                    "Metadata should be preserved");
            }
        }

        mod given_broadcast_message {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_message_is_visible_to_all_agents_via_global_view() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let from_agent = AgentId::new(RepoId::new("local"), 1);

                db.seed_idle_agents(5).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                // When - broadcast (no specific recipient)
                let message_id = db.send_agent_message(
                    &from_agent,
                    None,  // No specific recipient
                    None,
                    MessageType::Coordination,
                    ("Swarm update", "All agents: pause work"),
                    None,
                ).await.unwrap_or_else(|e| panic!("send failed: {}", e));

                // Then - should appear in global unread view
                let all_unread = db.get_all_unread_messages().await
                    .unwrap_or_else(|e| panic!("get_all_unread failed: {}", e));

                assert!(all_unread.iter().any(|m| m.id == message_id),
                    "Message should appear in global unread view");
            }
        }
    }

    mod when_marking_messages_read {

        mod given_unread_messages {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_messages_are_marked_read_and_timestamped() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let from_agent = AgentId::new(RepoId::new("local"), 1);
                let to_agent = AgentId::new(RepoId::new("local"), 2);
                let bead_id = BeadId::new(unique_bead("read-test"));

                db.seed_idle_agents(2).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                let msg_id_1 = db.send_agent_message(
                    &from_agent, Some(&to_agent), Some(&bead_id),
                    MessageType::QaFailed, ("Fail 1", "details"), None
                ).await.unwrap_or_else(|e| panic!("send 1 failed: {}", e));

                let msg_id_2 = db.send_agent_message(
                    &from_agent, Some(&to_agent), Some(&bead_id),
                    MessageType::ImplementationRetry, ("Retry", "try again"), None
                ).await.unwrap_or_else(|e| panic!("send 2 failed: {}", e));

                // Verify both are unread
                let unread_before = db.get_unread_messages(&to_agent, Some(&bead_id)).await
                    .unwrap_or_else(|e| panic!("get_unread before failed: {}", e));
                assert_eq!(unread_before.len(), 2);

                // When
                db.mark_messages_read(&to_agent, &[msg_id_1, msg_id_2]).await
                    .unwrap_or_else(|e| panic!("mark read failed: {}", e));

                // Then
                let unread_after = db.get_unread_messages(&to_agent, Some(&bead_id)).await
                    .unwrap_or_else(|e| panic!("get_unread after failed: {}", e));
                assert_eq!(unread_after.len(), 0, "All messages should be read");

                // Verify read status in DB
                let read_statuses: Vec<(bool, Option<chrono::DateTime<chrono::Utc>>)> =
                    sqlx::query_as(
                        "SELECT read, read_at FROM agent_messages WHERE id = ANY($1)")
                        .bind(&[msg_id_1, msg_id_2])
                        .fetch_all(db.pool())
                        .await
                        .unwrap_or_else(|e| panic!("query failed: {}", e));

                assert_eq!(read_statuses.len(), 2);
                for (read, read_at) in read_statuses {
                    assert!(read, "Message should be marked read");
                    assert!(read_at.is_some(), "Read timestamp should be set");
                }
            }

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_empty_list_is_handled_gracefully() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);

                db.seed_idle_agents(1).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));

                // When - marking empty list should not error
                let result = db.mark_messages_read(&agent_id, &[]).await;

                // Then
                assert!(result.is_ok(), "Marking empty messages should succeed");
            }
        }
    }

    mod when_storing_artifacts {

        mod given_stage_execution {
            use super::*;

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_artifacts_are_stored_with_content_hash_deduplication() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("artifact-dedup"));

                db.seed_idle_agents(1).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert bead failed: {}", e));
                db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));
                let stage_history_id = db
                    .record_stage_started(&agent_id, &bead_id, Stage::RustContract, 1)
                    .await
                    .unwrap_or_else(|e| panic!("stage start failed: {}", e));

                let content = "contract-document-body";
                let metadata = serde_json::json!({"version": "1.0"});

                // When - store same content twice
                let artifact_id_1 = db.store_stage_artifact(
                    stage_history_id,
                    ArtifactType::ContractDocument,
                    content,
                    Some(metadata.clone()),
                ).await.unwrap_or_else(|e| panic!("store 1 failed: {}", e));

                let artifact_id_2 = db.store_stage_artifact(
                    stage_history_id,
                    ArtifactType::ContractDocument,
                    content,
                    Some(metadata.clone()),
                ).await.unwrap_or_else(|e| panic!("store 2 failed: {}", e));

                // Then - should return same ID (deduplicated)
                assert_eq!(artifact_id_1, artifact_id_2,
                    "Duplicate content should return same artifact ID");

                // Verify only one artifact exists
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM stage_artifacts WHERE stage_history_id = $1")
                    .bind(stage_history_id)
                    .fetch_one(db.pool())
                    .await
                    .unwrap_or_else(|e| panic!("count failed: {}", e));
                assert_eq!(count, 1, "Should have one artifact after deduplication");
            }

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_different_artifact_types_create_separate_entries() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("multi-artifact"));

                db.seed_idle_agents(1).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert bead failed: {}", e));
                db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));
                let stage_history_id = db
                    .record_stage_started(&agent_id, &bead_id, Stage::Implement, 1)
                    .await
                    .unwrap_or_else(|e| panic!("stage start failed: {}", e));

                // When - store multiple artifact types
                let code_id = db.store_stage_artifact(
                    stage_history_id,
                    ArtifactType::ImplementationCode,
                    "fn main() {}",
                    None,
                ).await.unwrap_or_else(|e| panic!("store code failed: {}", e));

                let notes_id = db.store_stage_artifact(
                    stage_history_id,
                    ArtifactType::ImplementationNotes,
                    "implementation notes",
                    None,
                ).await.unwrap_or_else(|e| panic!("store notes failed: {}", e));

                // Then - should have separate entries
                assert_ne!(code_id, notes_id,
                    "Different artifact types should have different IDs");

                let artifacts = db.get_stage_artifacts(stage_history_id).await
                    .unwrap_or_else(|e| panic!("get artifacts failed: {}", e));
                assert_eq!(artifacts.len(), 2, "Should have two artifacts");
            }

            #[tokio::test]
            #[ignore = "requires DATABASE_URL or SWARM_TEST_DATABASE_URL"]
            async fn then_artifacts_are_retrievable_by_bead_and_type() {
                // Given
                let db = test_db().await;
                setup_schema(&db).await;
                reset_runtime_tables(&db).await;
                let agent_id = AgentId::new(RepoId::new("local"), 1);
                let bead_id = BeadId::new(unique_bead("artifact-query"));

                db.seed_idle_agents(1).await
                    .unwrap_or_else(|e| panic!("seed failed: {}", e));
                sqlx::query("INSERT INTO bead_backlog (bead_id, priority, status) VALUES ($1, 'p0', 'pending')")
                    .bind(bead_id.value()).execute(db.pool()).await
                    .unwrap_or_else(|e| panic!("insert bead failed: {}", e));
                db.claim_next_bead(&agent_id).await
                    .unwrap_or_else(|e| panic!("claim failed: {}", e));
                let stage_history_id = db
                    .record_stage_started(&agent_id, &bead_id, Stage::RustContract, 1)
                    .await
                    .unwrap_or_else(|e| panic!("stage start failed: {}", e));

                db.store_stage_artifact(
                    stage_history_id,
                    ArtifactType::ContractDocument,
                    "contract content",
                    None,
                ).await.unwrap_or_else(|e| panic!("store failed: {}", e));

                // When
                let contract_artifacts = db.get_bead_artifacts_by_type(
                    &bead_id,
                    ArtifactType::ContractDocument,
                ).await.unwrap_or_else(|e| panic!("query failed: {}", e));

                // Then
                assert_eq!(contract_artifacts.len(), 1,
                    "Should find one contract artifact");
                assert_eq!(contract_artifacts[0].artifact_type, ArtifactType::ContractDocument);
                assert_eq!(contract_artifacts[0].content, "contract content");

                // Query different type should return empty
                let test_artifacts = db.get_bead_artifacts_by_type(
                    &bead_id,
                    ArtifactType::TestOutput,
                ).await.unwrap_or_else(|e| panic!("query failed: {}", e));
                assert_eq!(test_artifacts.len(), 0,
                    "Should find no test output artifacts");
            }
        }
    }
}
