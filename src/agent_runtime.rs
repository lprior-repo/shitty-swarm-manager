use crate::config::StageCommands;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tokio::process::Command;
use tracing::{error, info, warn};

use swarm::{AgentId, BeadId, Result, Stage, StageResult, SwarmDb, SwarmError};

pub async fn run_agent(
    db: &SwarmDb,
    agent_id: &AgentId,
    stage_commands: &StageCommands,
) -> Result<()> {
    run_agent_recursive(db, agent_id, stage_commands).await
}

pub async fn run_smoke_once(db: &SwarmDb, agent_id: &AgentId) -> Result<()> {
    let maybe_bead = db.claim_next_bead(agent_id).await?;
    match maybe_bead {
        Some(bead_id) => {
            println!("Running smoke pipeline for bead {}", bead_id);
            let stages = [
                Stage::RustContract,
                Stage::Implement,
                Stage::QaEnforcer,
                Stage::RedQueen,
            ];
            run_smoke_stages_recursive(db, agent_id, &bead_id, &stages, 0).await?;
            println!("Smoke pipeline completed for bead {}", bead_id);
            Ok(())
        }
        None => {
            println!("No pending p0 beads available for smoke run.");
            Ok(())
        }
    }
}

pub async fn execute_stage(
    stage: Stage,
    bead_id: &BeadId,
    agent_id: &AgentId,
    commands: &StageCommands,
) -> StageResult {
    let command = render_stage_command(
        stage_command_template(stage, commands),
        bead_id.value(),
        agent_id.number(),
    );
    info!("Executing stage command [{}]: {}", stage, command);
    match run_shell_command(&command).await {
        Ok(output) if output.status_success => StageResult::Passed,
        Ok(output) => StageResult::Failed(output.message),
        Err(err) => StageResult::Error(format!("Stage command error: {}", err)),
    }
}

pub fn render_stage_command(template: &str, bead_id: &str, agent_id: u32) -> String {
    let safe_bead_id = shell_escape(bead_id);
    let safe_agent_id = shell_escape(&agent_id.to_string());
    template
        .replace("{bead_id}", &safe_bead_id)
        .replace("{agent_id}", &safe_agent_id)
}

fn shell_escape(value: &str) -> String {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/')
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

pub fn stage_command_template(stage: Stage, commands: &StageCommands) -> &str {
    match stage {
        Stage::RustContract => commands.rust_contract.as_str(),
        Stage::Implement => commands.implement.as_str(),
        Stage::QaEnforcer => commands.qa_enforcer.as_str(),
        Stage::RedQueen => commands.red_queen.as_str(),
        Stage::Done => "true",
    }
}

pub struct CommandOutput {
    pub status_success: bool,
    pub message: String,
}

pub async fn run_shell_command(command: &str) -> Result<CommandOutput> {
    let output = Command::new("bash")
        .arg("-lc")
        .arg(command)
        .output()
        .await
        .map_err(SwarmError::IoError)?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let message = if output.status.success() {
        stdout
    } else if stderr.is_empty() {
        stdout
    } else if stdout.is_empty() {
        stderr
    } else {
        format!("{}\n{}", stdout, stderr)
    };

    Ok(CommandOutput {
        status_success: output.status.success(),
        message,
    })
}

fn run_agent_recursive<'a>(
    db: &'a SwarmDb,
    agent_id: &'a AgentId,
    stage_commands: &'a StageCommands,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match db.get_agent_state(agent_id).await? {
            None => {
                error!("Agent {} not registered", agent_id);
                Ok(())
            }
            Some(state) => match state.status {
                swarm::AgentStatus::Idle => match db.claim_next_bead(agent_id).await? {
                    Some(bead_id) => {
                        info!("Agent {} claimed bead {}", agent_id, bead_id);
                        run_agent_recursive(db, agent_id, stage_commands).await
                    }
                    None => {
                        info!("Agent {} found no available beads", agent_id);
                        Ok(())
                    }
                },
                swarm::AgentStatus::Done => {
                    info!("Agent {} completed work", agent_id);
                    Ok(())
                }
                swarm::AgentStatus::Working | swarm::AgentStatus::Waiting => {
                    process_work_state(db, agent_id, stage_commands, state).await
                }
                _ => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    run_agent_recursive(db, agent_id, stage_commands).await
                }
            },
        }
    })
}

async fn process_work_state(
    db: &SwarmDb,
    agent_id: &AgentId,
    stage_commands: &StageCommands,
    state: swarm::AgentState,
) -> Result<()> {
    if state.implementation_attempt >= 3 {
        return match state.bead_id {
            Some(bead_id) => {
                let reason = "Max implementation attempts (3) exceeded";
                db.mark_bead_blocked(agent_id, &bead_id, reason).await?;
                warn!("Agent {} blocked bead {}: {}", agent_id, bead_id, reason);
                Ok(())
            }
            None => Ok(()),
        };
    }

    if let (Some(stage), Some(bead_id)) = (state.current_stage, state.bead_id) {
        let attempt = state.implementation_attempt.saturating_add(1);
        let started = Instant::now();
        db.record_stage_started(agent_id, &bead_id, stage, attempt)
            .await?;
        let result = execute_stage(stage, &bead_id, agent_id, stage_commands).await;
        db.record_stage_complete(
            agent_id,
            &bead_id,
            stage,
            attempt,
            result,
            started.elapsed().as_millis() as u64,
        )
        .await?;
    }

    run_agent_recursive(db, agent_id, stage_commands).await
}

fn run_smoke_stages_recursive<'a>(
    db: &'a SwarmDb,
    agent_id: &'a AgentId,
    bead_id: &'a BeadId,
    stages: &'a [Stage],
    idx: usize,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        stages
            .get(idx)
            .copied()
            .map_or_else(
                || Box::pin(async { Ok(()) }) as Pin<Box<dyn Future<Output = Result<()>> + Send>>,
                |stage| {
                    Box::pin(async move {
                        let started = Instant::now();
                        db.record_stage_started(agent_id, bead_id, stage, 1).await?;
                        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
                        db.record_stage_complete(
                            agent_id,
                            bead_id,
                            stage,
                            1,
                            StageResult::Passed,
                            started.elapsed().as_millis() as u64,
                        )
                        .await?;
                        run_smoke_stages_recursive(db, agent_id, bead_id, stages, idx + 1).await
                    })
                },
            )
            .await
    })
}

#[cfg(test)]
mod tests {
    use super::{execute_stage, render_stage_command, run_shell_command, stage_command_template};
    use crate::config::StageCommands;
    use swarm::{AgentId, BeadId, RepoId, Stage, StageResult};

    #[tokio::test]
    async fn shell_command_failure_captures_both_streams() {
        let output = run_shell_command("echo out; echo err >&2; exit 1").await;
        assert!(output.is_ok());
        if let Ok(out) = output {
            assert!(!out.status_success);
            assert!(out.message.contains("out") && out.message.contains("err"));
        }
    }

    #[tokio::test]
    async fn execute_stage_maps_success_and_failure() {
        let ok = StageCommands::for_mode(true);
        let passed = execute_stage(
            Stage::Implement,
            &BeadId::new("b1"),
            &AgentId::new(RepoId::new("local"), 1),
            &ok,
        )
        .await;
        assert_eq!(passed, StageResult::Passed);

        let fail = StageCommands {
            implement: "echo fail >&2; exit 9".to_string(),
            ..ok
        };
        let failed = execute_stage(
            Stage::Implement,
            &BeadId::new("b2"),
            &AgentId::new(RepoId::new("local"), 2),
            &fail,
        )
        .await;
        assert!(matches!(failed, StageResult::Failed(_)));
    }

    #[test]
    fn template_and_render_work() {
        let commands = StageCommands::default();
        assert_eq!(stage_command_template(Stage::Done, &commands), "true");
        assert_eq!(
            render_stage_command("echo {bead_id}:{agent_id}", "bead-1", 7),
            "echo bead-1:7"
        );
    }

    #[test]
    fn adversarial_payload_is_shell_escaped() {
        let rendered = render_stage_command("echo {bead_id}", "bead-1; rm -rf /tmp/evil", 7);
        assert!(rendered.contains("'bead-1; rm -rf /tmp/evil'"));
    }

    #[tokio::test]
    async fn adversarial_payload_does_not_execute_extra_commands() {
        let commands = StageCommands {
            rust_contract: "true".to_string(),
            implement: "printf %s {bead_id}".to_string(),
            qa_enforcer: "true".to_string(),
            red_queen: "true".to_string(),
        };
        let result = execute_stage(
            Stage::Implement,
            &BeadId::new("x; false"),
            &AgentId::new(RepoId::new("local"), 1),
            &commands,
        )
        .await;

        assert_eq!(result, StageResult::Passed);
    }
}
