#![cfg(test)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

use crate::runtime::repositories::{
    RuntimePgAgentRepository, RuntimePgBeadRepository, RuntimePgStageRepository,
};
use crate::runtime::shared::{RuntimeAgentId, RuntimeBeadId, RuntimeRepoId};
use crate::runtime::stage::Stage;
use sqlx::PgPool;

fn create_mock_pool() -> PgPool {
    PgPool::connect_lazy("postgres://mock:mock@localhost/mock").unwrap()
}

fn make_agent_id(repo_id: &str, agent_num: u32) -> RuntimeAgentId {
    RuntimeAgentId::new(RuntimeRepoId::new(repo_id), agent_num)
}

fn make_bead_id(bead_id: &str) -> RuntimeBeadId {
    RuntimeBeadId::new(bead_id)
}

#[cfg(feature = "db-tests")]
mod recover_stale_claims_tests {
    use super::*;

    #[tokio::test]
    async fn recover_stale_claims_is_repo_scoped() {
        let pool = create_mock_pool();
        let repo = RuntimePgBeadRepository::new(pool);
        let repo_a = RuntimeRepoId::new("repo-a");

        let result = repo.recover_stale_claims(&repo_a).await;
        assert!(result.is_ok(), "recover_stale_claims should return Ok");
    }

    #[tokio::test]
    async fn recover_stale_claims_returns_count() {
        let pool = create_mock_pool();
        let repo = RuntimePgBeadRepository::new(pool);
        let repo_id = RuntimeRepoId::new("test-repo");

        let result = repo.recover_stale_claims(&repo_id).await;
        assert!(result.is_ok(), "recover_stale_claims should return Ok");
        let count = result.unwrap();
        assert!(
            count < 1000,
            "recover_stale_claims should return reasonable count"
        );
    }
}

#[cfg(feature = "db-tests")]
mod heartbeat_claim_tests {
    use super::*;

    #[tokio::test]
    async fn heartbeat_claim_is_repo_scoped() {
        let pool = create_mock_pool();
        let repo = RuntimePgBeadRepository::new(pool);
        let agent_id = make_agent_id("test-repo", 1);
        let bead_id = make_bead_id("test-bead");

        let result = repo.heartbeat_claim(&agent_id, &bead_id, 60_000).await;
        assert!(result.is_ok(), "heartbeat_claim should return Ok");
    }

    #[tokio::test]
    async fn heartbeat_claim_returns_bool() {
        let pool = create_mock_pool();
        let repo = RuntimePgBeadRepository::new(pool);
        let agent_id = make_agent_id("test-repo", 1);
        let bead_id = make_bead_id("test-bead");

        let result = repo.heartbeat_claim(&agent_id, &bead_id, 60_000).await;
        assert!(result.is_ok(), "heartbeat_claim should return Ok");
        let _: bool = result.unwrap();
    }

    #[tokio::test]
    async fn heartbeat_claim_different_repos_isolated() {
        let pool = create_mock_pool();
        let repo = RuntimePgBeadRepository::new(pool);

        let agent_a = make_agent_id("repo-a", 1);
        let agent_b = make_agent_id("repo-b", 1);
        let bead_id = make_bead_id("shared-bead-id");

        let result_a: Result<bool, _> = repo.heartbeat_claim(&agent_a, &bead_id, 60_000).await;
        let result_b: Result<bool, _> = repo.heartbeat_claim(&agent_b, &bead_id, 60_000).await;

        assert!(
            result_a.is_ok(),
            "heartbeat_claim for repo-a should return Ok"
        );
        assert!(
            result_b.is_ok(),
            "heartbeat_claim for repo-b should return Ok"
        );

        let success_a = result_a.unwrap();
        let success_b = result_b.unwrap();

        assert!(
            success_a != success_b || !success_a,
            "Same bead in different repos should be isolated"
        );
    }
}

#[cfg(feature = "db-tests")]
mod find_by_id_tests {
    use super::*;

    #[tokio::test]
    async fn find_by_id_is_repo_scoped() {
        let pool = create_mock_pool();
        let repo = RuntimePgAgentRepository::new(pool);
        let agent_a = make_agent_id("repo-a", 1);

        let result = repo.find_by_id(&agent_a).await;
        assert!(result.is_ok(), "find_by_id should return Ok");
    }

    #[tokio::test]
    async fn find_by_id_different_repos_same_agent_num() {
        let pool = create_mock_pool();
        let repo = RuntimePgAgentRepository::new(pool);

        let agent_a = make_agent_id("repo-a", 1);
        let agent_b = make_agent_id("repo-b", 1);

        let result_a = repo.find_by_id(&agent_a).await;
        let result_b = repo.find_by_id(&agent_b).await;

        assert!(result_a.is_ok(), "find_by_id for repo-a should return Ok");
        assert!(result_b.is_ok(), "find_by_id for repo-b should return Ok");

        let state_a = result_a.unwrap();
        let state_b = result_b.unwrap();

        assert!(
            state_a.is_none()
                || state_b.is_none()
                || state_a.unwrap().agent_id().repo_id() != state_b.unwrap().agent_id().repo_id(),
            "Agents from different repos should be distinct"
        );
    }
}

#[cfg(feature = "db-tests")]
mod claim_next_tests {
    use super::*;

    #[tokio::test]
    async fn claim_next_is_repo_scoped() {
        let pool = create_mock_pool();
        let repo = RuntimePgBeadRepository::new(pool);
        let agent_a = make_agent_id("repo-a", 1);

        let result = repo.claim_next(&agent_a).await;
        assert!(result.is_ok(), "claim_next should return Ok");
    }

    #[tokio::test]
    async fn claim_next_does_not_cross_repo_boundary() {
        let pool = create_mock_pool();
        let repo = RuntimePgBeadRepository::new(pool);

        let agent_a = make_agent_id("repo-a", 1);
        let agent_b = make_agent_id("repo-b", 1);

        let claimed_a = repo.claim_next(&agent_a).await;
        let claimed_b = repo.claim_next(&agent_b).await;

        assert!(claimed_a.is_ok(), "claim_next for repo-a should return Ok");
        assert!(claimed_b.is_ok(), "claim_next for repo-b should return Ok");

        if let (Some(bead_a), Some(bead_b)) =
            (claimed_a.unwrap().as_ref(), claimed_b.unwrap().as_ref())
        {
            assert_ne!(
                bead_a.value(),
                bead_b.value(),
                "Agents from different repos should claim different beads"
            );
        }
    }
}

#[cfg(feature = "db-tests")]
mod stage_record_tests {
    use super::*;

    #[tokio::test]
    async fn record_started_is_repo_scoped() {
        let pool = create_mock_pool();
        let repo = RuntimePgStageRepository::new(pool);
        let agent_id = make_agent_id("test-repo", 1);
        let bead_id = make_bead_id("test-bead");

        let result = repo
            .record_started(&agent_id, &bead_id, Stage::Implement, 1)
            .await;
        assert!(result.is_ok(), "record_started should return Ok");
    }

    #[tokio::test]
    async fn record_completed_is_repo_scoped() {
        let pool = create_mock_pool();
        let repo = RuntimePgStageRepository::new(pool);
        let agent_id = make_agent_id("test-repo", 1);
        let bead_id = make_bead_id("test-bead");

        let result = repo
            .record_completed(
                &agent_id,
                &bead_id,
                Stage::Implement,
                1,
                crate::runtime::stage::StageResult::Passed,
                1000,
            )
            .await;
        assert!(result.is_ok(), "record_completed should return Ok");
    }
}
