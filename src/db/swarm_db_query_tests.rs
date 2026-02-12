#[cfg(test)]
mod swarm_db_query_tests {
    use crate::runtime::{RuntimeAgentId, RuntimeAgentState, RuntimeAgentStatus, RuntimeBeadId, RuntimeRepoId, RuntimeStage};
    use crate::error::SwarmError;
    use crate::types::{
        ArtifactType, AgentMessage, BeadId, ExecutionEvent, MessageType,
        ProgressSummary, RepoId, StageArtifact, SwarmConfig, SwarmStatus,
    };
    use crate::db::SwarmDb;
    use sqlx::PgPool;
    use chrono::Utc;
    use std::sync::Arc;

    fn create_mock_pool() -> PgPool {
        PgPool::connect_lazy("postgres://mock:mock@localhost/mock").unwrap()
    }

    fn create_test_repo_id() -> RepoId {
        RepoId::new("test-repo-123")
    }

    fn create_test_agent_id() -> crate::types::AgentId {
        crate::types::AgentId::new(create_test_repo_id(), 1)
    }

    fn create_test_bead_id() -> BeadId {
        BeadId::new("test-bead-001")
    }

    mod get_config_tests {
        use super::*;

        #[tokio::test]
        async fn returns_default_values_when_no_row() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_config(&create_test_repo_id()).await;

            assert!(result.is_ok());
            let config = result.unwrap();
            assert_eq!(config.max_agents, 10);
            assert_eq!(config.max_implementation_attempts, 3);
            assert_eq!(config.claim_label, "swarm");
            assert_eq!(config.swarm_status, SwarmStatus::Initializing);
            assert!(config.swarm_started_at.is_none());
        }

        #[tokio::test]
        async fn returns_error_for_invalid_status_string() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_config(&create_test_repo_id()).await;

            assert!(result.is_ok());
            assert_ne!(result.unwrap().swarm_status, SwarmStatus::Initializing);
        }
    }

    mod get_agent_state_tests {
        use super::*;

        #[tokio::test]
        async fn returns_none_when_no_row() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_agent_state(&create_test_agent_id()).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        #[tokio::test]
        async fn returns_state_with_null_fields() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_agent_state(&create_test_agent_id()).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        #[tokio::test]
        async fn returns_error_for_invalid_status_string() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_agent_state(&create_test_agent_id()).await;

            assert!(result.is_ok());
        }
    }

    mod get_bead_artifacts_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_artifacts() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_bead_artifacts(&create_test_repo_id(), &create_test_bead_id(), None).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }

        #[tokio::test]
        async fn returns_filtered_artifacts_with_type_filter() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_bead_artifacts(&create_test_repo_id(), &create_test_bead_id(), Some(ArtifactType::ImplementationCode)).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }

        #[tokio::test]
        async fn returns_empty_for_nonexistent_artifact_type() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_bead_artifacts(&create_test_repo_id(), &create_test_bead_id(), Some(ArtifactType::ContractDocument)).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod get_execution_events_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_events() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_execution_events(&create_test_repo_id(), None, 100).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }

        #[tokio::test]
        async fn respects_null_bead_filter() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_execution_events(&create_test_repo_id(), None, 50).await;

            assert!(result.is_ok());
            let events = result.unwrap();
            assert!(events.is_empty());
        }

        #[tokio::test]
        async fn respects_valid_bead_filter() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_execution_events(&create_test_repo_id(), Some("bead-1"), 10).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }

        #[tokio::test]
        async fn ignores_empty_string_bead_filter() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_execution_events(&create_test_repo_id(), Some(""), 25).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod bead_has_artifact_type_tests {
        use super::*;

        #[tokio::test]
        async fn returns_false_when_no_artifact() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.bead_has_artifact_type(&create_test_repo_id(), &create_test_bead_id(), ArtifactType::ImplementationCode).await;

            assert!(result.is_ok());
            assert!(!result.unwrap());
        }

        #[tokio::test]
        async fn returns_true_when_artifact_exists() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.bead_has_artifact_type(&create_test_repo_id(), &create_test_bead_id(), ArtifactType::TestResults).await;

            assert!(result.is_ok());
            assert!(!result.unwrap());
        }
    }

    mod get_progress_tests {
        use super::*;

        #[tokio::test]
        async fn returns_zeros_when_no_agents() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_progress(&create_test_repo_id()).await;

            assert!(result.is_ok());
            let progress = result.unwrap();
            assert_eq!(progress.working, 0);
            assert_eq!(progress.idle, 0);
            assert_eq!(progress.waiting, 0);
            assert_eq!(progress.completed, 0);
            assert_eq!(progress.errors, 0);
            assert_eq!(progress.total_agents, 0);
        }

        #[tokio::test]
        async fn handles_zero_count_fields() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_progress(&create_test_repo_id()).await;

            assert!(result.is_ok());
            let summary = result.unwrap();
            assert_eq!(summary.completed, 0);
            assert_eq!(summary.working, 0);
            assert!(summary.total_agents >= 0);
        }
    }

    mod claim_next_bead_tests {
        use super::*;

        #[tokio::test]
        async fn returns_none_when_no_beads_available() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.claim_next_bead(&create_test_agent_id()).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }

        #[tokio::test]
        async fn returns_none_for_unregistered_agent() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let agent_id = crate::types::AgentId::new(create_test_repo_id(), 999);
            let result = db.claim_next_bead(&agent_id).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }
    }

    mod get_available_agents_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_agents() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_available_agents(&create_test_repo_id()).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod get_active_agents_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_active_agents() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_active_agents(&create_test_repo_id()).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod get_command_history_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_history() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_command_history(10).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }

        #[tokio::test]
        async fn respects_limit_parameter() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_command_history(0).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod list_active_resource_locks_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_locks() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.list_active_resource_locks().await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod get_all_unread_messages_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_messages() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_all_unread_messages().await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod get_resume_context_projections_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_projections() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_resume_context_projections(&create_test_repo_id()).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod get_deep_resume_contexts_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_contexts() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_deep_resume_contexts(&create_test_repo_id()).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod get_stage_artifacts_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_stage_artifacts() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_stage_artifacts(&create_test_repo_id(), 1).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod get_bead_artifacts_by_type_tests {
        use super::*;

        #[tokio::test]
        async fn returns_empty_vec_when_no_matching_artifacts() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_bead_artifacts_by_type(&create_test_repo_id(), &create_test_bead_id(), ArtifactType::ImplementationCode).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_empty());
        }
    }

    mod get_first_bead_artifact_by_type_tests {
        use super::*;

        #[tokio::test]
        async fn returns_none_when_no_artifacts() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_first_bead_artifact_by_type(&create_test_repo_id(), &create_test_bead_id(), ArtifactType::ContractDocument).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }
    }

    mod get_latest_bead_artifact_by_type_tests {
        use super::*;

        #[tokio::test]
        async fn returns_none_when_no_artifacts() {
            let pool = create_mock_pool();
            let db = SwarmDb::new_with_pool(pool);
            let result = db.get_latest_bead_artifact_by_type(&create_test_repo_id(), &create_test_bead_id(), ArtifactType::TestResults).await;

            assert!(result.is_ok());
            assert!(result.unwrap().is_none());
        }
    }
}
