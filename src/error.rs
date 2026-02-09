use thiserror::Error;

#[derive(Error, Debug)]
pub enum SwarmError {
    #[error("Database error: {0}")]
    DatabaseError(String),

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
}

pub type Result<T> = std::result::Result<T, SwarmError>;
