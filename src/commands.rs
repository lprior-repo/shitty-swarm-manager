use crate::agent_runtime::{run_agent, run_smoke_once};
use crate::cli::{Cli, Commands, OutputFormat};
use crate::config::load_config;
use crate::load_profile::run_load_profile;
use crate::monitor::render_monitor_view;
use crate::output::emit_output;
use clap::Parser;
use serde_json::json;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use swarm::{AgentId, RepoId, Result, SwarmDb, SwarmError};

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    dispatch(cli).await
}

async fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Init => {
            emit_output(
                &cli.output,
                "init",
                json!({"message": "Swarm CLI ready", "hint": "Run `swarm init-db`"}),
            );
            Ok(())
        }
        Commands::Register { count } => register_command(&cli, count).await,
        Commands::Agent { id } => agent_command(&cli, id).await,
        Commands::Status { all } => status_command(&cli, all).await,
        Commands::Ps { all: _ } => ps_command(&cli).await,
        Commands::Dashboard { refresh } => {
            emit_output(&cli.output, "dashboard", json!({"refresh_ms": refresh}));
            Ok(())
        }
        Commands::Release { agent_id } => release_command(&cli, agent_id).await,
        Commands::InitDb {
            url,
            schema,
            seed_agents,
        } => init_db_command(&cli.output, url, schema, seed_agents).await,
        Commands::Monitor { ref view, watch_ms } => {
            monitor_command(&cli, view.clone(), watch_ms).await
        }
        Commands::SpawnPrompts {
            template,
            out_dir,
            count,
        } => spawn_prompts_command(&cli.output, template, out_dir, count).await,
        Commands::Smoke { id } => smoke_command(&cli, id).await,
        Commands::LoadProfile {
            agents,
            rounds,
            timeout_ms,
        } => load_profile_command(&cli, agents, rounds, timeout_ms).await,
    }
}

async fn register_command(cli: &Cli, count: u32) -> Result<()> {
    let config = load_config(cli.config.clone(), cli.claude_mode).await?;
    let db = SwarmDb::new(&config.database_url).await?;
    let repo_id = RepoId::from_current_dir()
        .ok_or_else(|| SwarmError::ConfigError("Not in a git repo".to_string()))?;
    let repo_name = std::env::current_dir()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
        .unwrap_or_else(|| "unknown".to_string());
    let repo_path = std::env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| ".".to_string());

    db.register_repo(&repo_id, &repo_name, &repo_path).await?;
    register_agents_recursive(&db, repo_id.clone(), 1, count).await?;
    emit_output(
        &cli.output,
        "register",
        json!({"repo": repo_id.value(), "count": count}),
    );
    Ok(())
}

async fn agent_command(cli: &Cli, id: u32) -> Result<()> {
    let config = load_config(cli.config.clone(), cli.claude_mode).await?;
    let db = SwarmDb::new(&config.database_url).await?;
    let repo_id = RepoId::from_current_dir()
        .ok_or_else(|| SwarmError::ConfigError("Not in a git repo".to_string()))?;
    let agent_id = AgentId::new(repo_id, id);
    run_agent(&db, &agent_id, &config.stage_commands).await?;
    emit_output(
        &cli.output,
        "agent",
        json!({"agent_id": id, "status": "completed"}),
    );
    Ok(())
}

async fn smoke_command(cli: &Cli, id: u32) -> Result<()> {
    let config = load_config(cli.config.clone(), cli.claude_mode).await?;
    let db = SwarmDb::new(&config.database_url).await?;
    run_smoke_once(&db, &AgentId::new(RepoId::new("local"), id)).await?;
    emit_output(
        &cli.output,
        "smoke",
        json!({"agent_id": id, "status": "completed"}),
    );
    Ok(())
}

async fn load_profile_command(cli: &Cli, agents: u32, rounds: u32, timeout_ms: u64) -> Result<()> {
    let config = load_config(cli.config.clone(), cli.claude_mode).await?;
    let db = SwarmDb::new(&config.database_url).await?;
    run_load_profile(&db, agents, rounds, timeout_ms, &cli.output).await
}

async fn status_command(cli: &Cli, all: bool) -> Result<()> {
    let config = load_config(cli.config.clone(), cli.claude_mode).await?;
    let db = SwarmDb::new(&config.database_url).await?;
    if all {
        let repos = db.list_repos().await?;
        let rows = collect_progress_rows(&db, repos, 0, Vec::new()).await?;
        emit_output(&cli.output, "status", json!({"all": true, "rows": rows}));
    } else {
        let repo_id = RepoId::from_current_dir()
            .ok_or_else(|| SwarmError::ConfigError("Not in a git repo".to_string()))?;
        let progress = db.get_progress(&repo_id).await?;
        emit_output(
            &cli.output,
            "status",
            json!({"all": false, "repo": repo_id.value(), "working": progress.working, "idle": progress.idle, "done": progress.completed, "errors": progress.errors}),
        );
    }
    Ok(())
}

async fn ps_command(cli: &Cli) -> Result<()> {
    let config = load_config(cli.config.clone(), cli.claude_mode).await?;
    let db = SwarmDb::new(&config.database_url).await?;
    let rows = db
        .get_all_active_agents()
        .await?
        .into_iter()
        .map(|(repo_id, agent_id, bead_id, status)| json!({"repo": repo_id.value(), "agent_id": agent_id, "bead_id": bead_id, "status": status}))
        .collect::<Vec<_>>();
    emit_output(&cli.output, "ps", json!({"rows": rows}));
    Ok(())
}

async fn release_command(cli: &Cli, agent_id: u32) -> Result<()> {
    let config = load_config(cli.config.clone(), cli.claude_mode).await?;
    let _db = SwarmDb::new(&config.database_url).await?;
    let repo_id = RepoId::from_current_dir()
        .ok_or_else(|| SwarmError::ConfigError("Not in a git repo".to_string()))?;
    let agent = AgentId::new(repo_id, agent_id);
    emit_output(
        &cli.output,
        "release",
        json!({"agent": agent.to_string(), "status": "not_implemented"}),
    );
    Ok(())
}

async fn init_db_command(
    output: &OutputFormat,
    url: String,
    schema: PathBuf,
    seed_agents: u32,
) -> Result<()> {
    let db = SwarmDb::new(&url).await?;
    let schema_sql = tokio::fs::read_to_string(&schema).await.map_err(|e| {
        SwarmError::ConfigError(format!("Failed to read schema {}: {}", schema.display(), e))
    })?;
    db.initialize_schema_from_sql(&schema_sql).await?;
    db.seed_idle_agents(seed_agents).await?;
    emit_output(
        output,
        "init-db",
        json!({"database_url": url, "schema": schema.display().to_string(), "seed_agents": seed_agents}),
    );
    Ok(())
}

async fn monitor_command(cli: &Cli, view: crate::cli::MonitorView, watch_ms: u64) -> Result<()> {
    let config = load_config(cli.config.clone(), cli.claude_mode).await?;
    let db = SwarmDb::new(&config.database_url).await?;
    if watch_ms == 0 {
        render_monitor_view(&db, &view, &cli.output).await
    } else {
        watch_monitor_recursive(&db, &view, &cli.output, watch_ms).await
    }
}

async fn spawn_prompts_command(
    output: &OutputFormat,
    template: PathBuf,
    out_dir: PathBuf,
    count: u32,
) -> Result<()> {
    let template_text = tokio::fs::read_to_string(&template).await.map_err(|e| {
        SwarmError::ConfigError(format!(
            "Failed to read template {}: {}",
            template.display(),
            e
        ))
    })?;
    tokio::fs::create_dir_all(&out_dir)
        .await
        .map_err(SwarmError::IoError)?;
    write_prompts_recursive(&template_text, &out_dir, 1, count).await?;
    emit_output(
        output,
        "spawn-prompts",
        json!({"count": count, "out_dir": out_dir.display().to_string()}),
    );
    Ok(())
}

fn register_agents_recursive<'a>(
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

fn write_prompts_recursive<'a>(
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

fn collect_progress_rows<'a>(
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

fn watch_monitor_recursive<'a>(
    db: &'a SwarmDb,
    view: &'a crate::cli::MonitorView,
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
