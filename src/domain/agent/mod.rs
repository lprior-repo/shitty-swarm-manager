#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

mod agent_state;

pub use agent_state::{AgentState, AgentStatus};

#[cfg(test)]
mod tests;
