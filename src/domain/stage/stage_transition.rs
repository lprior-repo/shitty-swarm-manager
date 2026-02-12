#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::Stage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageTransition {
    Advance(Stage),
    Retry,
    Complete,
    Block,
    NoOp,
}

impl StageTransition {
    #[must_use]
    pub const fn is_no_op(&self) -> bool {
        matches!(self, Self::NoOp)
    }

    #[must_use]
    pub const fn should_advance(&self) -> bool {
        matches!(self, Self::Advance(_))
    }

    #[must_use]
    pub const fn should_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    #[must_use]
    pub const fn should_block(&self) -> bool {
        matches!(self, Self::Block)
    }
}
