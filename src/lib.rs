#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

pub mod contracts;
pub mod db;
pub mod error;
pub mod gate_cache;
pub mod prompts;
pub mod protocol_envelope;
pub mod skill_execution;
pub mod skill_execution_parsing;
pub mod skill_prompts;
pub mod stage_executor_content;
pub mod stage_executors;
pub mod types;

pub use contracts::*;
pub use db::SwarmDb;
pub use error::{code, Result, SwarmError, ERROR_CODES};
pub use gate_cache::GateExecutionCache;
pub use types::*;
