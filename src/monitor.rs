use crate::cli::{MonitorView, OutputFormat};
use crate::output::emit_output;
use serde_json::json;
use swarm::{RepoId, Result, SwarmDb};

pub async fn render_monitor_view(
    db: &SwarmDb,
    view: &MonitorView,
    output: &OutputFormat,
) -> Result<()> {
    match view {
        MonitorView::Active => render_active(db, output).await,
        MonitorView::Progress => render_progress(db, output).await,
        MonitorView::Failures => render_failures(db, output).await,
        MonitorView::Messages => render_messages(db, output).await,
    }
}

async fn render_active(db: &SwarmDb, output: &OutputFormat) -> Result<()> {
    let agents = db.get_all_active_agents().await?;
    if *output == OutputFormat::Json {
        let rows = agents
            .into_iter()
            .map(|(repo, agent_id, bead_id, status)| {
                json!({ "repo": repo.value(), "agent_id": agent_id, "bead_id": bead_id, "status": status })
            })
            .collect::<Vec<_>>();
        emit_output(output, "monitor", json!({"view":"active", "rows": rows}));
    } else {
        println!("Active Agents\n");
        agents
            .iter()
            .map(|(_, agent_id, bead_id, status)| {
                format!(
                    "{:<8} {:<24} {:<10}",
                    agent_id,
                    bead_id.clone().unwrap_or_else(|| "-".to_string()),
                    status
                )
            })
            .for_each(|line| println!("{}", line));
    }
    Ok(())
}

async fn render_progress(db: &SwarmDb, output: &OutputFormat) -> Result<()> {
    let progress = db.get_progress(&RepoId::new("local")).await?;
    if *output == OutputFormat::Json {
        emit_output(
            output,
            "monitor",
            json!({
                "view":"progress",
                "total": progress.total_agents,
                "working": progress.working,
                "idle": progress.idle,
                "waiting": progress.waiting,
                "done": progress.completed,
                "errors": progress.errors,
            }),
        );
    } else {
        println!(
            "Swarm Progress\nTotal: {}\nWorking: {}\nIdle: {}\nWaiting: {}\nDone: {}\nErrors: {}",
            progress.total_agents,
            progress.working,
            progress.idle,
            progress.waiting,
            progress.completed,
            progress.errors
        );
    }
    Ok(())
}

async fn render_failures(db: &SwarmDb, output: &OutputFormat) -> Result<()> {
    let failures = db.get_feedback_required().await?;
    if *output == OutputFormat::Json {
        let rows = failures
            .into_iter()
            .map(
                |(bead_id, agent_id, stage, attempt, feedback, completed_at)| {
                    json!({
                        "bead_id": bead_id,
                        "agent_id": agent_id,
                        "stage": stage,
                        "attempt": attempt,
                        "feedback": feedback,
                        "completed_at": completed_at,
                    })
                },
            )
            .collect::<Vec<_>>();
        emit_output(output, "monitor", json!({"view":"failures", "rows": rows}));
    } else {
        println!("Failures Requiring Feedback");
        failures
            .iter()
            .map(|(bead, agent, stage, attempt, _, _)| {
                format!("{:<24} {:<8} {:<14} {:<8}", bead, agent, stage, attempt)
            })
            .for_each(|line| println!("{}", line));
    }
    Ok(())
}

async fn render_messages(db: &SwarmDb, output: &OutputFormat) -> Result<()> {
    let messages = db.get_all_unread_messages().await?;

    if *output == OutputFormat::Json {
        let rows = messages
            .into_iter()
            .map(|message| {
                json!({
                    "id": message.id,
                    "from_agent_id": message.from_agent_id,
                    "to_agent_id": message.to_agent_id,
                    "bead_id": message.bead_id.map(|b| b.value().to_string()),
                    "message_type": message.message_type.as_str(),
                    "subject": message.subject,
                    "created_at": message.created_at,
                    "read": message.read,
                })
            })
            .collect::<Vec<_>>();
        emit_output(output, "monitor", json!({"view":"messages", "rows": rows}));
    } else {
        println!("Unread Messages");
        messages
            .iter()
            .map(|message| {
                format!(
                    "{:<8} {:<8} {:<20} {:<18} {}",
                    message.id,
                    message.from_agent_id,
                    message
                        .to_agent_id
                        .map_or_else(|| "-".to_string(), |value| value.to_string()),
                    message.message_type.as_str(),
                    message.subject
                )
            })
            .for_each(|line| println!("{}", line));
    }

    Ok(())
}
