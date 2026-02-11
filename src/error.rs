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
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ConfigError(_) | Self::SerializationError(_) => code::INVALID,
            Self::DatabaseError(_) | Self::SqlxError(_) | Self::Internal(_) => code::INTERNAL,
            Self::AgentError(_) | Self::StageError(_) => code::CONFLICT,
            Self::BeadError(_) => code::NOTFOUND,
            Self::IoError(_) => code::DEPENDENCY,
        }
    }

    /// Returns the exit code for this error
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        match self {
            Self::ConfigError(_) => 2,
            Self::DatabaseError(_) | Self::SqlxError(_) => 3,
            Self::AgentError(_) => 4,
            Self::BeadError(_) => 5,
            Self::StageError(_) => 6,
            Self::IoError(_) => 7,
            Self::SerializationError(_) => 8,
            Self::Internal(_) => 9,
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

pub type Result<T> = std::result::Result<T, SwarmError>;
