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

    let mut push_unique = |value: String| {
        if !candidates.iter().any(|existing| existing == &value) {
            candidates.push(value);
        }
    };

    if let Ok(explicit) = env::var("DATABASE_URL") {
        let trimmed = explicit.trim();
        if !trimmed.is_empty() {
            push_unique(trimmed.to_string());
        }
    }

    if let Ok(test_url) = env::var("SWARM_TEST_DATABASE_URL") {
        let trimmed = test_url.trim();
        if !trimmed.is_empty() {
            push_unique(trimmed.to_string());
        }
    }

    push_unique(
        "postgres://shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db".to_string(),
    );
    push_unique("postgres://localhost:5437/shitty_swarm_manager_db".to_string());
    push_unique("postgres://postgres:postgres@localhost:5432/swarm".to_string());
    push_unique("postgres://localhost:5432/swarm".to_string());

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
