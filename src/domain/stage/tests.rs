#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

#[cfg(test)]
mod bdd_tests {
    use crate::domain::stage::{
        decision_from_stage_dag, passed_stage_transition,
        validate_completion_requires_push_confirmation, Stage, StageResult, StageTransition,
        TransitionDecision, TransitionReason,
    };

    fn given_a_passed_result() -> StageResult {
        StageResult::Passed
    }

    fn given_a_failed_result() -> StageResult {
        StageResult::Failed("needs work".to_string())
    }

    fn given_an_error_result() -> StageResult {
        StageResult::Error("critical failure".to_string())
    }

    fn given_an_started_result() -> StageResult {
        StageResult::Started
    }

    fn when_determine_transition(
        stage: Stage,
        result: &StageResult,
        attempt: u32,
        max_attempts: u32,
    ) -> TransitionDecision {
        let is_success = result.is_success();
        let retry_exhausted = attempt >= max_attempts;
        decision_from_stage_dag(stage, is_success, retry_exhausted)
    }

    #[test]
    fn when_stage_passes_then_advances_to_next_stage() {
        let stages_and_expected_nexts = [
            (Stage::RustContract, Stage::Implement),
            (Stage::Implement, Stage::QaEnforcer),
            (Stage::QaEnforcer, Stage::RedQueen),
        ];

        for (stage, expected_next) in stages_and_expected_nexts {
            let decision = when_determine_transition(stage, &given_a_passed_result(), 1, 3);
            assert_eq!(
                decision.transition(),
                StageTransition::Advance(expected_next)
            );
            assert_eq!(decision.reason(), TransitionReason::StagePassedAdvance);
        }
    }

    #[test]
    fn when_red_queen_passes_then_completes() {
        let decision = when_determine_transition(Stage::RedQueen, &given_a_passed_result(), 1, 3);

        assert_eq!(decision.transition(), StageTransition::Complete);
        assert_eq!(decision.reason(), TransitionReason::RedQueenPassedComplete);
    }

    #[test]
    fn when_done_passes_then_no_op() {
        let decision = when_determine_transition(Stage::Done, &given_a_passed_result(), 1, 3);

        assert_eq!(decision.transition(), StageTransition::NoOp);
        assert_eq!(decision.reason(), TransitionReason::StagePassedNoNextStage);
    }

    #[test]
    fn when_stage_fails_and_attempts_remain_then_retries() {
        let stages = [
            Stage::RustContract,
            Stage::Implement,
            Stage::QaEnforcer,
            Stage::RedQueen,
        ];

        for stage in stages {
            let decision = when_determine_transition(stage, &given_a_failed_result(), 1, 3);
            assert_eq!(decision.transition(), StageTransition::Retry);
            assert_eq!(decision.reason(), TransitionReason::StageFailedRetry);
        }
    }

    #[test]
    fn when_stage_fails_and_attempts_exhausted_then_blocks() {
        let stages = [
            Stage::RustContract,
            Stage::Implement,
            Stage::QaEnforcer,
            Stage::RedQueen,
        ];

        for stage in stages {
            let decision = when_determine_transition(stage, &given_a_failed_result(), 3, 3);
            assert_eq!(decision.transition(), StageTransition::Block);
            assert_eq!(
                decision.reason(),
                TransitionReason::StageFailedMaxAttemptsReached
            );
        }
    }

    #[test]
    fn when_error_result_then_same_as_failed() {
        let retry_decision =
            when_determine_transition(Stage::Implement, &given_an_error_result(), 1, 3);
        assert_eq!(retry_decision.transition(), StageTransition::Retry);

        let block_decision =
            when_determine_transition(Stage::Implement, &given_an_error_result(), 3, 3);
        assert_eq!(block_decision.transition(), StageTransition::Block);
    }

    #[test]
    fn when_completion_requested_then_requires_push_confirmation() {
        let result =
            validate_completion_requires_push_confirmation(StageTransition::Complete, false);
        assert!(result.is_err());

        let result =
            validate_completion_requires_push_confirmation(StageTransition::Complete, true);
        assert!(result.is_ok());
    }

    #[test]
    fn when_non_completion_transition_then_no_push_required() {
        let result = validate_completion_requires_push_confirmation(StageTransition::Retry, false);
        assert!(result.is_ok());

        let result = validate_completion_requires_push_confirmation(StageTransition::Block, false);
        assert!(result.is_ok());
    }

    #[test]
    fn when_stage_advances_then_follows_dag_order() {
        assert_eq!(Stage::RustContract.next(), Some(Stage::Implement));
        assert_eq!(Stage::Implement.next(), Some(Stage::QaEnforcer));
        assert_eq!(Stage::QaEnforcer.next(), Some(Stage::RedQueen));
        assert_eq!(Stage::RedQueen.next(), Some(Stage::Done));
        assert_eq!(Stage::Done.next(), None);
    }

    #[test]
    fn when_stage_is_done_then_it_is_terminal() {
        assert!(Stage::Done.is_terminal());
        assert!(!Stage::Implement.is_terminal());
    }

    #[test]
    fn when_passed_result_then_is_success() {
        assert!(given_a_passed_result().is_success());
        assert!(!given_a_failed_result().is_success());
        assert!(!given_an_error_result().is_success());
        assert!(!given_an_started_result().is_success());
    }

    #[test]
    fn when_result_has_message_then_returns_it() {
        assert!(given_a_passed_result().message().is_none());
        assert!(given_an_started_result().message().is_none());
        assert_eq!(given_a_failed_result().message(), Some("needs work"));
        assert_eq!(given_an_error_result().message(), Some("critical failure"));
    }
}
