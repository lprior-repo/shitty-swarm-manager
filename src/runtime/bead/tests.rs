#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

#[cfg(test)]
mod bdd_tests {
    use crate::runtime::bead::{BeadExecution, BeadExecutionStatus};
    use crate::runtime::stage::{Stage, StageResult, StageTransition};

    fn given_an_active_bead_at(stage: Stage, attempt: u32) -> BeadExecution {
        BeadExecution::new(stage, attempt, 3, BeadExecutionStatus::Active).expect("valid bead")
    }

    fn given_a_passed_result() -> StageResult {
        StageResult::Passed
    }

    fn given_a_failed_result() -> StageResult {
        StageResult::Failed("test failure".to_string())
    }

    fn given_a_started_result() -> StageResult {
        StageResult::Started
    }

    #[test]
    fn when_creating_bead_with_zero_max_attempts_then_invariant_violation() {
        let result = BeadExecution::new(Stage::Implement, 0, 0, BeadExecutionStatus::Active);

        assert!(result.is_err());
    }

    #[test]
    fn when_attempt_exceeds_max_then_invariant_violation() {
        let result = BeadExecution::new(Stage::Implement, 4, 3, BeadExecutionStatus::Active);

        assert!(result.is_err());
    }

    #[test]
    fn when_completed_status_without_done_stage_then_invariant_violation() {
        let result = BeadExecution::new(Stage::Implement, 1, 3, BeadExecutionStatus::Completed);

        assert!(result.is_err());
    }

    #[test]
    fn when_done_stage_without_completed_status_then_invariant_violation() {
        let result = BeadExecution::new(Stage::Done, 1, 3, BeadExecutionStatus::Active);

        assert!(result.is_err());
    }

    #[test]
    fn when_blocked_in_done_stage_then_invariant_violation() {
        let result = BeadExecution::new(Stage::Done, 1, 3, BeadExecutionStatus::Blocked);

        assert!(result.is_err());
    }

    #[test]
    fn when_stage_passes_then_advances() {
        let bead = given_an_active_bead_at(Stage::Implement, 1);
        let decision = bead
            .determine_transition(&given_a_passed_result())
            .expect("decision");

        assert_eq!(
            decision.transition(),
            StageTransition::Advance(Stage::QaEnforcer)
        );
    }

    #[test]
    fn when_stage_fails_and_can_retry_then_retries() {
        let bead = given_an_active_bead_at(Stage::Implement, 1);
        let decision = bead
            .determine_transition(&given_a_failed_result())
            .expect("decision");

        assert_eq!(decision.transition(), StageTransition::Retry);
    }

    #[test]
    fn when_stage_fails_and_attempts_exhausted_then_blocks() {
        let bead = given_an_active_bead_at(Stage::Implement, 3);
        let decision = bead
            .determine_transition(&given_a_failed_result())
            .expect("decision");

        assert_eq!(decision.transition(), StageTransition::Block);
    }

    #[test]
    fn when_started_result_then_transition_rejected() {
        let bead = given_an_active_bead_at(Stage::Implement, 1);
        let result = bead.determine_transition(&given_a_started_result());

        assert!(result.is_err());
    }

    #[test]
    fn when_red_queen_passes_then_completes() {
        let bead = given_an_active_bead_at(Stage::RedQueen, 1);
        let decision = bead
            .determine_transition(&given_a_passed_result())
            .expect("decision");

        assert_eq!(decision.transition(), StageTransition::Complete);
    }
}
