#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuntimeError {
    #[error("Repository error: {0}")]
    RepositoryError(String),
    #[error("Domain invariant violation: {0}")]
    InvariantViolation(String),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;
