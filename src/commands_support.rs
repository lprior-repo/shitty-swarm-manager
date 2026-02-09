use crate::cli::{MonitorView, OutputFormat};
use crate::monitor::render_monitor_view;
use serde_json::json;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use swarm::{AgentId, RepoId, Result, SwarmDb, SwarmError};
use tokio::process::Command;

pub async fn database_url_from_pass(entry: &str) -> Result<String> {
    let output = Command::new("pass")
        .args(["show", entry])
        .output()
        .await
        .map_err(SwarmError::IoError)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(SwarmError::ConfigError(format!(
            "Failed to read pass entry '{}': {}",
            entry,
            if stderr.is_empty() {
                "unknown error"
            } else {
                &stderr
            }
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let from_key = stdout.lines().find_map(|line| {
        line.trim()
            .strip_prefix("connection_url:")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    });

    let from_first_line = stdout
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("postgresql://"))
        .map(str::to_string);

    from_key.or(from_first_line).ok_or_else(|| {
        SwarmError::ConfigError(format!(
            "Pass entry '{}' does not contain a PostgreSQL URL (expected connection_url: ...)",
            entry
        ))
    })
}

pub fn register_agents_recursive<'a>(
    db: &'a SwarmDb,
    repo_id: RepoId,
    next: u32,
    count: u32,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if next > count {
            Ok(())
        } else {
            db.register_agent(&AgentId::new(repo_id.clone(), next))
                .await?;
            register_agents_recursive(db, repo_id, next.saturating_add(1), count).await
        }
    })
}

pub fn write_prompts_recursive<'a>(
    template_text: &'a str,
    out_dir: &'a PathBuf,
    next: u32,
    count: u32,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if next > count {
            Ok(())
        } else {
            let path = out_dir.join(format!("agent_{:02}.md", next));
            tokio::fs::write(&path, template_text.replace("{N}", &next.to_string()))
                .await
                .map_err(SwarmError::IoError)?;
            write_prompts_recursive(template_text, out_dir, next.saturating_add(1), count).await
        }
    })
}

pub fn collect_progress_rows<'a>(
    db: &'a SwarmDb,
    repos: Vec<(RepoId, String)>,
    idx: usize,
    acc: Vec<serde_json::Value>,
) -> Pin<Box<dyn Future<Output = Result<Vec<serde_json::Value>>> + Send + 'a>> {
    Box::pin(async move {
        match repos.get(idx) {
            None => Ok(acc),
            Some((repo_id, name)) => match db.get_progress(repo_id).await {
                Ok(progress) => {
                    let mut next_acc = acc;
                    next_acc.push(json!({
                        "repo": repo_id.value(),
                        "name": name,
                        "working": progress.working,
                        "idle": progress.idle,
                        "done": progress.completed,
                        "errors": progress.errors,
                    }));
                    collect_progress_rows(db, repos, idx + 1, next_acc).await
                }
                Err(_) => collect_progress_rows(db, repos, idx + 1, acc).await,
            },
        }
    })
}

pub fn watch_monitor_recursive<'a>(
    db: &'a SwarmDb,
    view: &'a MonitorView,
    output: &'a OutputFormat,
    watch_ms: u64,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if *output == OutputFormat::Text {
            print!("\x1B[2J\x1B[1;1H");
        }
        render_monitor_view(db, view, output).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(watch_ms)).await;
        watch_monitor_recursive(db, view, output, watch_ms).await
    })
}
