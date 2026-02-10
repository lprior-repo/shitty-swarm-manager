use std::path::PathBuf;
use swarm::{Result, SwarmError};

#[derive(Debug)]
pub struct Config {
    pub database_url: String,
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
            database_url: default_database_url(),
            stage_commands: StageCommands::for_mode(claude_mode),
        });
    }

    let content = tokio::fs::read_to_string(&config_path)
        .await
        .map_err(|e| SwarmError::ConfigError(format!("Failed to read config: {}", e)))?;

    let (database_url, stage_commands) = parse_config_content(&content);
    let adjusted = if claude_mode {
        stage_commands.with_claude_fallbacks()
    } else {
        stage_commands
    };

    Ok(Config {
        database_url: database_url
            .filter(|url| !url.is_empty())
            .unwrap_or_else(default_database_url),
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
            database_url = Some(value.to_string());
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

fn default_database_url() -> String {
    std::env::var("DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            "postgresql://shitty_swarm_manager:shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db".to_string()
        })
}

pub fn default_database_url_for_cli() -> String {
    default_database_url()
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
