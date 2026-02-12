use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub stage_commands: Vec<String>,
}

impl Config {
    #[must_use]
    pub const fn new(stage_commands: Vec<String>) -> Self {
        Self { stage_commands }
    }
}

#[must_use]
pub fn database_url_candidates_for_cli() -> Vec<String> {
    let mut candidates = Vec::new();

    if let Ok(explicit) = env::var("DATABASE_URL") {
        let trimmed = explicit.trim();
        if !trimmed.is_empty() {
            candidates.push(trimmed.to_string());
        }
    }

    candidates.push("postgres://postgres:postgres@localhost:5432/swarm".to_string());
    candidates.push("postgres://localhost:5432/swarm".to_string());

    candidates
}

#[must_use]
pub fn load_config() -> Config {
    Config::new(vec![
        "rust-contract".to_string(),
        "implement".to_string(),
        "qa-enforcer".to_string(),
        "red-queen".to_string(),
    ])
}
