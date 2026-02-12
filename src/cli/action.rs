#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::CliCommand;

#[derive(Debug, Clone)]
pub enum CliAction {
    ShowHelp,
    ShowVersion,
    RunProtocol,
    Command(CliCommand),
}
