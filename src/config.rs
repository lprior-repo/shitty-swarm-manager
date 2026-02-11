#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use std::path::PathBuf;
use swarm::{Result, SwarmError};

#[derive(Debug)]
pub struct Config {
    pub stage_commands: StageCommands,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageCommands {
    pub rust_contract: String,
    pub implement: String,
    pub qa_enforcer: String,
    pub red_queen: String,
}

pub async fn load_config(path: Option<PathBuf>, claude_mode: bool) -> Result<Config> {
    let config_path = path.unwrap_or_else(|| PathBuf::from(".swarm/config.toml"));
    if !config_path.exists() {
        return Ok(Config {
            stage_commands: StageCommands::for_mode(claude_mode),
        });
    }

    let content = tokio::fs::read_to_string(&config_path)
        .await
        .map_err(|e| SwarmError::ConfigError(format!("Failed to read config: {e}")))?;

    let (_database_url, stage_commands) = parse_config_content(&content);
    let adjusted = if claude_mode {
        stage_commands.with_claude_fallbacks()
    } else {
        stage_commands
    };

    Ok(Config {
        stage_commands: adjusted,
    })
}

pub fn parse_config_content(content: &str) -> (Option<String>, StageCommands) {
    let mut database_url = None;
    let mut stage_commands = StageCommands::default();

    for line in content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        if let Some(value) = parse_key_value(line, "database_url") {
            database_url = Some(expand_env_vars(value));
        }
        if let Some(value) = parse_key_value(line, "rust_contract_cmd") {
            stage_commands.rust_contract = value.to_string();
        }
        if let Some(value) = parse_key_value(line, "implement_cmd") {
            stage_commands.implement = value.to_string();
        }
        if let Some(value) = parse_key_value(line, "qa_enforcer_cmd") {
            stage_commands.qa_enforcer = value.to_string();
        }
        if let Some(value) = parse_key_value(line, "red_queen_cmd") {
            stage_commands.red_queen = value.to_string();
        }
    }

    (database_url, stage_commands)
}

fn expand_env_vars(input: &str) -> String {
    let mut result = input.to_string();
    while let Some(start) = result.find("${") {
        if let Some(end) = result[start..].find('}') {
            let var_part = &result[start + 2..start + end];
            let (var_name, default) = var_part.split_once(":-").unwrap_or((var_part, ""));
            let value = std::env::var(var_name).unwrap_or_else(|_| default.to_string());
            result.replace_range(start..=(start + end), &value);
        } else {
            break;
        }
    }
    result
}

pub fn parse_key_value<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_once('=')
        .and_then(|(lhs, rhs)| (lhs.trim() == key).then_some(rhs.trim().trim_matches('"')))
}

impl Default for StageCommands {
    fn default() -> Self {
        Self {
            rust_contract: "br show {bead_id}".to_string(),
            implement: "jj status".to_string(),
            qa_enforcer: "moon run :quick".to_string(),
            red_queen: "moon run :test".to_string(),
        }
    }
}

impl StageCommands {
    pub fn for_mode(claude_mode: bool) -> Self {
        if claude_mode {
            Self {
                rust_contract: "echo rust-contract {bead_id}".to_string(),
                implement: "echo implement {bead_id}".to_string(),
                qa_enforcer: "moon run :quick".to_string(),
                red_queen: "moon run :test".to_string(),
            }
        } else {
            Self::default()
        }
    }

    pub fn with_claude_fallbacks(self) -> Self {
        let defaults = Self::for_mode(true);
        let base = Self::default();
        Self {
            rust_contract: if self.rust_contract == base.rust_contract {
                defaults.rust_contract
            } else {
                self.rust_contract
            },
            implement: if self.implement == base.implement {
                defaults.implement
            } else {
                self.implement
            },
            qa_enforcer: self.qa_enforcer,
            red_queen: self.red_queen,
        }
    }
}

#[allow(dead_code)]
pub fn default_database_url_for_cli() -> String {
    database_url_candidates_for_cli()
        .into_iter()
        .next()
        .unwrap_or_else(computed_default_database_url)
}

pub fn database_url_candidates_for_cli() -> Vec<String> {
    let mut candidates = Vec::new();

    // 1. Environment variable wins so local shell config works immediately.
    push_unique(&mut candidates, non_empty_env_var("DATABASE_URL"));

    // 2. Project config comes next.
    if let Ok(content) = std::fs::read_to_string(".swarm/config.toml") {
        let (database_url, _) = parse_config_content(&content);
        push_unique(
            &mut candidates,
            database_url.and_then(|url| {
                let trimmed = url.trim().to_string();
                (!trimmed.is_empty()).then_some(trimmed)
            }),
        );
    }

    // 3. Finally, computed defaults from SWARM_DB_* values.
    push_unique(&mut candidates, Some(computed_default_database_url()));

    candidates
}

fn push_unique(target: &mut Vec<String>, value: Option<String>) {
    if let Some(candidate) = value {
        if !target.iter().any(|existing| existing == &candidate) {
            target.push(candidate);
        }
    }
}

fn non_empty_env_var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn computed_default_database_url() -> String {
    let user =
        std::env::var("SWARM_DB_USER").unwrap_or_else(|_| "shitty_swarm_manager".to_string());
    let pass =
        std::env::var("SWARM_DB_PASSWORD").unwrap_or_else(|_| "shitty_swarm_manager".to_string());
    let host = std::env::var("SWARM_DB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("SWARM_DB_PORT").unwrap_or_else(|_| "5437".to_string());
    let db =
        std::env::var("SWARM_DB_NAME").unwrap_or_else(|_| "shitty_swarm_manager_db".to_string());
    format!("postgres://{user}:{pass}@{host}:{port}/{db}")
}

#[cfg(test)]
mod tests {
    use super::{parse_config_content, parse_key_value, StageCommands};

    #[test]
    fn parse_reads_stage_commands_and_database_url() {
        let content = r#"database_url = "postgresql://x"
rust_contract_cmd = "echo contract"
implement_cmd = "echo implement"
qa_enforcer_cmd = "echo qa"
red_queen_cmd = "echo rq""#;
        let (database_url, commands) = parse_config_content(content);
        assert_eq!(database_url, Some("postgresql://x".to_string()));
        assert_eq!(commands.rust_contract, "echo contract");
        assert_eq!(commands.implement, "echo implement");
        assert_eq!(commands.qa_enforcer, "echo qa");
        assert_eq!(commands.red_queen, "echo rq");
    }

    #[test]
    fn parse_key_value_handles_spaces_and_mismatch() {
        assert_eq!(
            parse_key_value("database_url = \"postgres://u:p@h/db?x=y\"", "database_url"),
            Some("postgres://u:p@h/db?x=y")
        );
        assert_eq!(parse_key_value("other = \"x\"", "database_url"), None);
    }

    #[test]
    fn claude_fallbacks_keep_quality_gates() {
        let commands = StageCommands::default().with_claude_fallbacks();
        assert_eq!(commands.rust_contract, "echo rust-contract {bead_id}");
        assert_eq!(commands.implement, "echo implement {bead_id}");
        assert_eq!(commands.qa_enforcer, "moon run :quick");
    }
}
