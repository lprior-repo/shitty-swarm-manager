use std::path::{Path, PathBuf};

use tokio::fs;

use crate::error::{Result, SwarmError};

pub const AGENT_PROMPT_TEMPLATE: &str = include_str!("../.agents/agent_prompt.md");

fn replace_agent_placeholders(template: &str, agent_id: u32) -> String {
    let id = agent_id.to_string();
    template.replace("#{N}", &id).replace("{N}", &id)
}

#[must_use]
pub fn canonical_agent_prompt_path(repo_root: &Path) -> PathBuf {
    repo_root.join(".agents").join("agent_prompt.md")
}

pub async fn load_agent_prompt_template(repo_root: &Path) -> Result<String> {
    let path = canonical_agent_prompt_path(repo_root);
    fs::read_to_string(path).await.map_err(SwarmError::from)
}

#[must_use]
pub async fn get_agent_prompt(repo_root: &Path, agent_id: u32) -> Result<String> {
    let template = load_agent_prompt_template(repo_root).await?;
    Ok(replace_agent_placeholders(&template, agent_id))
}
