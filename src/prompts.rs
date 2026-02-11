use std::path::{Path, PathBuf};

use tokio::fs;

use crate::error::{Result, SwarmError};

/// Fallback embedded prompt template used when repository template loading is bypassed.
pub const AGENT_PROMPT_TEMPLATE: &str = include_str!("../.agents/agent_prompt.md");

fn replace_agent_placeholders(template: &str, agent_id: u32) -> String {
    let id = agent_id.to_string();
    template.replace("#{N}", &id).replace("{N}", &id)
}

#[must_use]
pub fn canonical_agent_prompt_path(repo_root: &Path) -> PathBuf {
    repo_root.join(".agents").join("agent_prompt.md")
}

/// Loads the on-disk agent prompt template from `.agents/agent_prompt.md`.
///
/// # Errors
///
/// Returns an error when the prompt template cannot be read.
pub async fn load_agent_prompt_template(repo_root: &Path) -> Result<String> {
    let path = canonical_agent_prompt_path(repo_root);
    fs::read_to_string(path).await.map_err(SwarmError::from)
}

/// Expands per-agent placeholders in the prompt template and returns the final prompt text.
///
/// # Errors
///
/// Returns an error when the prompt template cannot be read.
pub async fn get_agent_prompt(repo_root: &Path, agent_id: u32) -> Result<String> {
    let template = load_agent_prompt_template(repo_root).await?;
    Ok(replace_agent_placeholders(&template, agent_id))
}
