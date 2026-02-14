#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::parser::CliError;

/// # Errors
/// Returns `CliError::UnknownCommand` if an unknown flag is found.
pub fn ensure_no_unknown_flags(args: &[String], allowed_flags: &[&str]) -> Result<(), CliError> {
    let invalid = args
        .iter()
        .skip(1)
        .find(|arg| {
            arg.starts_with("--")
                && !matches!(arg.as_str(), "--help" | "-h")
                && !allowed_flags.iter().any(|allowed| allowed == &arg.as_str())
        })
        .cloned();

    invalid.map_or(Ok(()), |flag| Err(CliError::UnknownCommand { cmd: flag }))
}

#[must_use]
pub fn suggest_commands(typo: &str) -> Vec<String> {
    const VALID_COMMANDS: &[&str] = &[
        "doctor",
        "help",
        "status",
        "next",
        "claim-next",
        "assign",
        "run-once",
        "qa",
        "resume",
        "resume-context",
        "artifacts",
        "agent",
        "init",
        "register",
        "release",
        "monitor",
        "init-db",
        "init-local-db",
        "bootstrap",
        "spawn-prompts",
        "prompt",
        "smoke",
        "batch",
        "state",
        "history",
        "lock",
        "unlock",
        "agents",
        "broadcast",
        "load-profile",
    ];

    VALID_COMMANDS
        .iter()
        .map(|cmd| (cmd, strsim::levenshtein(typo, cmd)))
        .filter(|(_, dist)| *dist <= 3)
        .min_by_key(|(_, dist)| *dist)
        .map(|(cmd, _)| vec![cmd.to_string()])
        .unwrap_or_default()
}
