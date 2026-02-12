#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::BeadExecutionStatus;
use crate::domain::shared::RuntimeError;
use crate::domain::stage::{decision_from_stage_dag, Stage, StageResult, TransitionDecision};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadExecution {
    current_stage: Stage,
    implementation_attempt: u32,
    max_implementation_attempts: u32,
    status: BeadExecutionStatus,
}

impl BeadExecution {
    pub fn new(
        current_stage: Stage,
        implementation_attempt: u32,
        max_implementation_attempts: u32,
        status: BeadExecutionStatus,
    ) -> crate::domain::shared::Result<Self> {
        let execution = Self {
            current_stage,
            implementation_attempt,
            max_implementation_attempts,
            status,
        };
        execution.validate_invariants()?;
        Ok(execution)
    }

    #[must_use]
    pub const fn current_stage(&self) -> Stage {
        self.current_stage
    }

    #[must_use]
    pub const fn implementation_attempt(&self) -> u32 {
        self.implementation_attempt
    }

    #[must_use]
    pub const fn max_implementation_attempts(&self) -> u32 {
        self.max_implementation_attempts
    }

    #[must_use]
    pub const fn status(&self) -> BeadExecutionStatus {
        self.status
    }

    pub fn determine_transition(
        &self,
        result: &StageResult,
    ) -> crate::domain::shared::Result<TransitionDecision> {
        self.validate_invariants()?;

        if matches!(result, StageResult::Started) {
            return Err(RuntimeError::InvariantViolation(
                "Stage result Started cannot produce a transition decision".to_string(),
            ));
        }

        let retry_exhausted = self.implementation_attempt >= self.max_implementation_attempts;
        Ok(decision_from_stage_dag(
            self.current_stage,
            result.is_success(),
            retry_exhausted,
        ))
    }

    pub fn validate_invariants(&self) -> crate::domain::shared::Result<()> {
        if self.max_implementation_attempts == 0 {
            return Err(RuntimeError::InvariantViolation(
                "BeadExecution max_implementation_attempts must be greater than zero".to_string(),
            ));
        }

        if self.implementation_attempt > self.max_implementation_attempts {
            return Err(RuntimeError::InvariantViolation(format!(
                "BeadExecution implementation_attempt {} exceeds max_implementation_attempts {}",
                self.implementation_attempt, self.max_implementation_attempts
            )));
        }

        if self.status == BeadExecutionStatus::Completed && self.current_stage != Stage::Done {
            return Err(RuntimeError::InvariantViolation(
                "BeadExecution with Completed status must be in Done stage".to_string(),
            ));
        }

        if self.current_stage == Stage::Done && self.status != BeadExecutionStatus::Completed {
            return Err(RuntimeError::InvariantViolation(
                "BeadExecution in Done stage must have Completed status".to_string(),
            ));
        }

        if self.status == BeadExecutionStatus::Blocked && self.current_stage == Stage::Done {
            return Err(RuntimeError::InvariantViolation(
                "BeadExecution cannot be Blocked in Done stage".to_string(),
            ));
        }

        Ok(())
    }
}
