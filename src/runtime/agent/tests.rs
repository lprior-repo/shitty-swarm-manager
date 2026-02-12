#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

#[cfg(test)]
mod bdd_tests {
    use crate::runtime::agent::{AgentState, AgentStatus};
    use crate::runtime::shared::{RuntimeAgentId, RuntimeBeadId, RuntimeRepoId};
    use crate::runtime::stage::Stage;

    fn given_an_idle_agent() -> AgentState {
        let repo_id = RuntimeRepoId::new("test-repo");
        let agent_id = RuntimeAgentId::new(repo_id, 1);
        AgentState::new(agent_id, None, None, AgentStatus::Idle, 0)
    }

    fn given_a_working_agent_with_bead() -> AgentState {
        let repo_id = RuntimeRepoId::new("test-repo");
        let agent_id = RuntimeAgentId::new(repo_id, 1);
        let bead_id = RuntimeBeadId::new("bead-123");
        AgentState::new(
            agent_id,
            Some(bead_id),
            Some(Stage::Implement),
            AgentStatus::Working,
            1,
        )
    }

    #[test]
    fn when_agent_is_idle_then_it_has_no_bead() {
        let agent = given_an_idle_agent();

        assert!(agent.bead_id().is_none());
        assert!(agent.current_stage().is_none());
        assert!(!agent.is_working());
    }

    #[test]
    fn when_agent_is_working_then_it_must_have_bead_and_stage() {
        let agent = given_a_working_agent_with_bead();

        assert!(agent.bead_id().is_some());
        assert!(agent.current_stage().is_some());
        assert!(agent.is_working());
    }

    #[test]
    fn when_agent_is_working_without_bead_then_invariant_violation() {
        let repo_id = RuntimeRepoId::new("test-repo");
        let agent_id = RuntimeAgentId::new(repo_id, 1);
        let invalid_agent = AgentState::new(
            agent_id,
            None,
            Some(Stage::Implement),
            AgentStatus::Working,
            1,
        );

        let result = invalid_agent.validate_invariants();

        assert!(result.is_err());
    }

    #[test]
    fn when_agent_can_retry_then_attempts_remaining() {
        let agent = given_a_working_agent_with_bead();

        assert!(agent.can_retry(3));
        assert!(!agent.can_retry(1));
    }

    #[test]
    fn when_agent_is_done_then_it_is_terminal() {
        assert!(AgentStatus::Done.is_terminal());
        assert!(!AgentStatus::Working.is_terminal());
        assert!(!AgentStatus::Idle.is_terminal());
    }

    #[test]
    fn when_agent_is_working_or_waiting_then_it_is_active() {
        assert!(AgentStatus::Working.is_active());
        assert!(AgentStatus::Waiting.is_active());
        assert!(AgentStatus::Error.is_active());
        assert!(!AgentStatus::Idle.is_active());
        assert!(!AgentStatus::Done.is_active());
    }
}
