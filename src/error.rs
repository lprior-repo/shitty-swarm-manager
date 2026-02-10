#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use thiserror::Error;

/// Error code constants for type-safe error handling
pub mod code {
    pub const CLI_ERROR: &str = "CLI_ERROR";
    pub const EXISTS: &str = "EXISTS";
    pub const NOTFOUND: &str = "NOTFOUND";
    pub const INVALID: &str = "INVALID";
    pub const CONFLICT: &str = "CONFLICT";
    pub const BUSY: &str = "BUSY";
    pub const UNAUTHORIZED: &str = "UNAUTHORIZED";
    pub const DEPENDENCY: &str = "DEPENDENCY";
    pub const TIMEOUT: &str = "TIMEOUT";
    pub const INTERNAL: &str = "INTERNAL";
}

#[derive(Error, Debug)]
pub enum SwarmError {
    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("SQLx error: {0}")]
    SqlxError(#[from] sqlx::Error),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Agent error: {0}")]
    AgentError(String),

    #[error("Bead error: {0}")]
    BeadError(String),

    #[error("Stage error: {0}")]
    StageError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl SwarmError {
    /// Returns the protocol error code for this error
    pub fn code(&self) -> &'static str {
        match self {
            SwarmError::ConfigError(_) => code::INVALID,
            SwarmError::DatabaseError(_) => code::INTERNAL,
            SwarmError::SqlxError(_) => code::INTERNAL,
            SwarmError::AgentError(_) => code::CONFLICT,
            SwarmError::BeadError(_) => code::NOTFOUND,
            SwarmError::StageError(_) => code::CONFLICT,
            SwarmError::IoError(_) => code::DEPENDENCY,
            SwarmError::SerializationError(_) => code::INVALID,
            SwarmError::Internal(_) => code::INTERNAL,
        }
    }

    /// Returns the exit code for this error
    pub fn exit_code(&self) -> i32 {
        match self {
            SwarmError::ConfigError(_) => 2,
            SwarmError::DatabaseError(_) => 3,
            SwarmError::SqlxError(_) => 3,
            SwarmError::AgentError(_) => 4,
            SwarmError::BeadError(_) => 5,
            SwarmError::StageError(_) => 6,
            SwarmError::IoError(_) => 7,
            SwarmError::SerializationError(_) => 8,
            SwarmError::Internal(_) => 9,
        }
    }
}

/// Protocol error codes as documented in the CLI
pub const ERROR_CODES: &[(&str, &str, &str)] = &[
    (
        code::CLI_ERROR,
        "Invalid CLI usage",
        "Run 'swarm --help' for valid options",
    ),
    (
        code::EXISTS,
        "Resource already exists",
        "Use different identifier or delete existing resource",
    ),
    (
        code::NOTFOUND,
        "Resource was not found",
        "List resources and verify identifier",
    ),
    (
        code::INVALID,
        "Invalid request payload",
        "Validate JSON syntax and ensure all required fields are present",
    ),
    (
        code::CONFLICT,
        "Conflicting state transition",
        "Run swarm state to inspect current status",
    ),
    (
        code::BUSY,
        "Resource is temporarily locked",
        "Retry after lock TTL expires",
    ),
    (
        code::UNAUTHORIZED,
        "Operation not authorized",
        "Use valid agent identity",
    ),
    (
        code::DEPENDENCY,
        "Missing system dependency",
        "Install required binary and retry",
    ),
    (
        code::TIMEOUT,
        "Operation timed out",
        "Increase timeout and retry",
    ),
    (
        code::INTERNAL,
        "Unexpected internal failure",
        "Inspect logs and retry command",
    ),
];

/// Get error code details (description and fix) for a given error code
pub fn get_error_info(error_code: &str) -> Option<(&'static str, &'static str)> {
    ERROR_CODES
        .iter()
        .find(|(code, _, _)| *code == error_code)
        .map(|(_, desc, fix)| (*desc, *fix))
}

pub type Result<T> = std::result::Result<T, SwarmError>;
