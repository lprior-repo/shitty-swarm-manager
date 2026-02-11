#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorSyncTerminal {
    PushConfirmed,
    PushUnconfirmed { reason: String },
    LandingErrored { reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrSyncStatus {
    Synchronized,
    RetryScheduled,
    Diverged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrSyncDivergence {
    RetryPending,
    TerminalMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrSyncAction {
    FinalizeTerminalClaim,
    RecordRetryableFailure { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrSyncDecision {
    action: BrSyncAction,
    status: BrSyncStatus,
    divergence: Option<BrSyncDivergence>,
}

impl BrSyncDecision {
    #[must_use]
    pub const fn new(
        action: BrSyncAction,
        status: BrSyncStatus,
        divergence: Option<BrSyncDivergence>,
    ) -> Self {
        Self {
            action,
            status,
            divergence,
        }
    }

    #[must_use]
    pub const fn action(&self) -> &BrSyncAction {
        &self.action
    }

    #[must_use]
    pub const fn status(&self) -> BrSyncStatus {
        self.status
    }

    #[must_use]
    pub const fn divergence(&self) -> Option<BrSyncDivergence> {
        self.divergence
    }
}

#[must_use]
pub fn map_terminal_sync_state(state: CoordinatorSyncTerminal) -> BrSyncDecision {
    match state {
        CoordinatorSyncTerminal::PushConfirmed => BrSyncDecision::new(
            BrSyncAction::FinalizeTerminalClaim,
            BrSyncStatus::Synchronized,
            None,
        ),
        CoordinatorSyncTerminal::PushUnconfirmed { reason } => BrSyncDecision::new(
            BrSyncAction::RecordRetryableFailure { reason },
            BrSyncStatus::RetryScheduled,
            Some(BrSyncDivergence::RetryPending),
        ),
        CoordinatorSyncTerminal::LandingErrored { reason } => BrSyncDecision::new(
            BrSyncAction::RecordRetryableFailure { reason },
            BrSyncStatus::Diverged,
            Some(BrSyncDivergence::TerminalMismatch),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        map_terminal_sync_state, BrSyncAction, BrSyncDivergence, BrSyncStatus,
        CoordinatorSyncTerminal,
    };

    #[test]
    fn push_confirmed_maps_to_synchronized_finalize_action() {
        let decision = map_terminal_sync_state(CoordinatorSyncTerminal::PushConfirmed);

        assert_eq!(decision.status(), BrSyncStatus::Synchronized);
        assert_eq!(decision.action(), &BrSyncAction::FinalizeTerminalClaim);
        assert!(decision.divergence().is_none());
    }

    #[test]
    fn push_unconfirmed_maps_to_diverged_retryable_action() {
        let decision = map_terminal_sync_state(CoordinatorSyncTerminal::PushUnconfirmed {
            reason: "jj git push failed".to_string(),
        });

        assert_eq!(decision.status(), BrSyncStatus::RetryScheduled);
        assert_eq!(
            decision.action(),
            &BrSyncAction::RecordRetryableFailure {
                reason: "jj git push failed".to_string()
            }
        );
        assert_eq!(decision.divergence(), Some(BrSyncDivergence::RetryPending));
    }

    #[test]
    fn landing_error_maps_to_diverged_retryable_action() {
        let decision = map_terminal_sync_state(CoordinatorSyncTerminal::LandingErrored {
            reason: "transport timeout".to_string(),
        });

        assert_eq!(decision.status(), BrSyncStatus::Diverged);
        assert_eq!(
            decision.action(),
            &BrSyncAction::RecordRetryableFailure {
                reason: "transport timeout".to_string()
            }
        );
        assert_eq!(
            decision.divergence(),
            Some(BrSyncDivergence::TerminalMismatch)
        );
    }
}
