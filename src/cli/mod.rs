#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

mod action;
mod args;
mod commands;
mod parser;

pub use action::CliAction;
pub use args::{ensure_no_unknown_flags, suggest_commands};
pub use commands::{cli_command_to_request, CliCommand};
pub use parser::{parse_cli_args, CliError};

#[cfg(test)]
mod tests;
