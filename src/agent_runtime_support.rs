#![allow(clippy::literal_string_with_formatting_args)]

use crate::config::StageCommands;
use serde::Serialize;
use serde_json::json;
use swarm::{
    ArtifactType, BeadId, MessageType, Result, Stage, StageArtifact, StageResult, SwarmError,
};
use tokio::process::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum LandingSagaState {
    Pending,
    Committing,
    Syncing,
    Fetching,
    Pushing,
    Confirmed,
    Failed,
}

impl LandingSagaState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Committing => "committing",
            Self::Syncing => "syncing",
            Self::Fetching => "fetching",
            Self::Pushing => "pushing",
            Self::Confirmed => "confirmed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LandingStepOutcome {
    pub state: LandingSagaState,
    pub step: &'static str,
    pub command: Option<&'static str>,
    pub status_success: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LandingSagaOutcome {
    pub bead_id: String,
    pub initial_state: LandingSagaState,
    pub terminal_state: LandingSagaState,
    pub push_confirmed: bool,
    pub steps: Vec<LandingStepOutcome>,
}

impl LandingSagaOutcome {
    #[must_use]
    pub fn failure_summary(&self) -> String {
        self.steps
            .last()
            .filter(|last| !last.status_success)
            .map_or_else(
                || "landing saga ended before push confirmation".to_string(),
                |last| format!("{} failed: {}", last.step, last.message.trim()),
            )
    }

    #[must_use]
    pub fn persistence_payload(&self) -> serde_json::Value {
        json!({
            "initial_state": self.initial_state.as_str(),
            "terminal_state": self.terminal_state.as_str(),
            "push_confirmed": self.push_confirmed,
            "steps": self.steps.iter().map(|step| {
                json!({
                    "state": step.state.as_str(),
                    "step": step.step,
                    "command": step.command,
                    "status_success": step.status_success,
                    "message": step.message,
                })
            }).collect::<Vec<_>>(),
        })
    }
}

const LANDING_SAGA_STEPS: [(LandingSagaState, &str, Option<&str>); 5] = [
    (LandingSagaState::Committing, "commit_changes", None),
    (
        LandingSagaState::Syncing,
        "sync_beads",
        Some("br sync --flush-only"),
    ),
    (
        LandingSagaState::Fetching,
        "fetch_remote",
        Some("jj git fetch"),
    ),
    (
        LandingSagaState::Pushing,
        "push_commits",
        Some("jj git push"),
    ),
    (
        LandingSagaState::Confirmed,
        "finalize_workspace",
        Some("zjj done"),
    ),
];

pub const fn stage_primary_artifact(stage: Stage, result: &StageResult) -> ArtifactType {
    match (stage, result.is_success()) {
        (Stage::RustContract, _) => ArtifactType::ContractDocument,
        (Stage::Implement, _) => ArtifactType::ImplementationCode,
        (Stage::QaEnforcer, true) => ArtifactType::TestOutput,
        (Stage::QaEnforcer, false) => ArtifactType::FailureDetails,
        (Stage::RedQueen, true) => ArtifactType::QualityGateReport,
        (Stage::RedQueen, false) => ArtifactType::AdversarialReport,
        (Stage::Done, _) => ArtifactType::StageLog,
    }
}

pub const fn stage_success_message_type(stage: Stage) -> Option<MessageType> {
    match stage {
        Stage::RustContract => Some(MessageType::ContractReady),
        Stage::Implement => Some(MessageType::ImplementationReady),
        Stage::QaEnforcer => Some(MessageType::QaComplete),
        Stage::RedQueen => Some(MessageType::StageComplete),
        Stage::Done => None,
    }
}

pub const fn stage_failure_message_type(stage: Stage) -> Option<MessageType> {
    match stage {
        Stage::QaEnforcer => Some(MessageType::QaFailed),
        Stage::RedQueen => Some(MessageType::RedQueenFailed),
        Stage::RustContract | Stage::Implement => Some(MessageType::StageFailed),
        Stage::Done => None,
    }
}

const fn preferred_message_artifact_types(
    stage: Stage,
    is_success: bool,
) -> &'static [ArtifactType] {
    match (stage, is_success) {
        (Stage::RustContract, _) => &[ArtifactType::ContractDocument],
        (Stage::Implement, _) => &[ArtifactType::ImplementationCode],
        (Stage::QaEnforcer, true) => &[ArtifactType::TestOutput, ArtifactType::TestResults],
        (Stage::QaEnforcer, false) => &[ArtifactType::FailureDetails, ArtifactType::TestOutput],
        (Stage::RedQueen, true) => &[ArtifactType::QualityGateReport],
        (Stage::RedQueen, false) => &[ArtifactType::AdversarialReport],
        (Stage::Done, _) => &[ArtifactType::StageLog],
    }
}

pub fn build_full_message_body(
    stage: Stage,
    status: &str,
    bead_id: &BeadId,
    result_message: &str,
    artifacts: &[StageArtifact],
    is_success: bool,
) -> String {
    let preferred = preferred_message_artifact_types(stage, is_success);
    let preferred_content = preferred.iter().find_map(|wanted_type| {
        artifacts
            .iter()
            .find(|artifact| artifact.artifact_type == *wanted_type && !artifact.content.is_empty())
            .map(|artifact| artifact.content.as_str())
    });

    preferred_content.map_or_else(
        || {
            if result_message.is_empty() {
                format!("{} {} for bead {}", stage.as_str(), status, bead_id.value())
            } else {
                result_message.to_string()
            }
        },
        ToOwned::to_owned,
    )
}

pub async fn execute_stage(
    stage: Stage,
    bead_id: &BeadId,
    agent_id: &swarm::AgentId,
    commands: &StageCommands,
) -> StageResult {
    let command = render_stage_command(
        stage_command_template(stage, commands),
        bead_id.value(),
        agent_id.number(),
    );
    match run_shell_command(&command).await {
        Ok(output) if output.status_success => StageResult::Passed,
        Ok(output) => StageResult::Failed(output.message),
        Err(err) => StageResult::Error(format!("Stage command error: {err}")),
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

pub const fn stage_command_template(stage: Stage, commands: &StageCommands) -> &str {
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
    let message = match (stdout.is_empty(), stderr.is_empty()) {
        (true, _) => stderr,
        (_, true) => stdout,
        _ => format!("{stdout}\n{stderr}"),
    };

    Ok(CommandOutput {
        status_success: output.status.success(),
        message,
    })
}

pub async fn create_workspace(agent_id: u32, bead_id: &str) -> Result<()> {
    let cmd = format!("zjj add agent-{agent_id}-{bead_id}");
    let out = run_shell_command(&cmd).await?;
    if !out.status_success {
        tracing::warn!(
            "Workspace creation might have failed or already exists: {}",
            out.message
        );
    }
    Ok(())
}

pub async fn execute_landing_saga(bead_id: &str) -> Result<LandingSagaOutcome> {
    run_landing_saga_with_runner(bead_id, |command| async move {
        run_shell_command(command.as_str()).await
    })
    .await
}

pub async fn run_landing_saga_with_runner<R, Fut>(
    bead_id: &str,
    mut run: R,
) -> Result<LandingSagaOutcome>
where
    R: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<CommandOutput>>,
{
    let mut push_confirmed = false;
    let mut terminal_state = LandingSagaState::Pending;
    let mut steps = Vec::with_capacity(LANDING_SAGA_STEPS.len());

    for (state, step, command) in LANDING_SAGA_STEPS {
        let output = match command {
            Some(actual_command) => run(actual_command.to_string()).await?,
            None => CommandOutput {
                status_success: true,
                message: "Commit state already persisted before landing".to_string(),
            },
        };

        if step == "push_commits" {
            push_confirmed = output.status_success;
        }

        let status_success = output.status_success;
        terminal_state = if status_success {
            state
        } else {
            LandingSagaState::Failed
        };
        steps.push(LandingStepOutcome {
            state,
            step,
            command,
            status_success,
            message: output.message,
        });

        if !status_success {
            break;
        }
    }

    Ok(LandingSagaOutcome {
        bead_id: bead_id.to_string(),
        initial_state: LandingSagaState::Pending,
        terminal_state,
        push_confirmed,
        steps,
    })
}

pub async fn finalize_workspace(_bead_id: &str) -> Result<()> {
    // br sync --flush-only
    let _ = run_shell_command("br sync --flush-only").await;
    // jj git push
    let _ = run_shell_command("jj git push").await;
    // zjj done
    let _ = run_shell_command("zjj done").await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        execute_stage, render_stage_command, run_landing_saga_with_runner, run_shell_command,
        stage_command_template, CommandOutput, LandingSagaState,
    };
    use crate::config::StageCommands;
    use swarm::Result as SwarmResult;
    use swarm::{AgentId, BeadId, RepoId, Stage, StageResult};

    fn pop_output(outputs: &mut Vec<CommandOutput>) -> SwarmResult<CommandOutput> {
        outputs.pop().map_or_else(
            || {
                Err(swarm::Error::Internal(
                    "test output queue was exhausted".to_string(),
                ))
            },
            Ok,
        )
    }

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

    #[tokio::test]
    async fn landing_saga_confirms_push_before_completion() {
        let mut outputs = vec![
            CommandOutput {
                status_success: true,
                message: "workspace done".to_string(),
            },
            CommandOutput {
                status_success: true,
                message: "push ok".to_string(),
            },
            CommandOutput {
                status_success: true,
                message: "fetch ok".to_string(),
            },
            CommandOutput {
                status_success: true,
                message: "sync ok".to_string(),
            },
        ];

        let outcome = run_landing_saga_with_runner("swm-qso", move |_command| {
            let next = pop_output(&mut outputs);
            async move { next }
        })
        .await;

        assert!(outcome.is_ok());
        if let Ok(result) = outcome {
            assert!(result.push_confirmed);
            assert_eq!(result.initial_state, LandingSagaState::Pending);
            assert_eq!(result.terminal_state, LandingSagaState::Confirmed);
            assert_eq!(result.steps.len(), 5);
            assert_eq!(result.steps[3].step, "push_commits");
            assert_eq!(result.steps[4].state, LandingSagaState::Confirmed);
        }
    }

    #[tokio::test]
    async fn landing_saga_stops_when_fetch_fails_and_marks_failed_terminal_state() {
        let mut outputs = vec![
            CommandOutput {
                status_success: true,
                message: "should not run".to_string(),
            },
            CommandOutput {
                status_success: false,
                message: "fetch failed".to_string(),
            },
            CommandOutput {
                status_success: true,
                message: "sync ok".to_string(),
            },
        ];

        let outcome = run_landing_saga_with_runner("swm-qso", move |_command| {
            let next = pop_output(&mut outputs);
            async move { next }
        })
        .await;

        assert!(outcome.is_ok());
        if let Ok(result) = outcome {
            assert!(!result.push_confirmed);
            assert_eq!(result.terminal_state, LandingSagaState::Failed);
            assert_eq!(result.steps.len(), 3);
            assert_eq!(result.steps[2].state, LandingSagaState::Fetching);
            assert!(!result.steps[2].status_success);
        }
    }

    #[tokio::test]
    async fn landing_saga_persistence_payload_exposes_state_machine_contract() {
        let mut outputs = vec![
            CommandOutput {
                status_success: true,
                message: "workspace done".to_string(),
            },
            CommandOutput {
                status_success: true,
                message: "push ok".to_string(),
            },
            CommandOutput {
                status_success: true,
                message: "fetch ok".to_string(),
            },
            CommandOutput {
                status_success: true,
                message: "sync ok".to_string(),
            },
        ];

        let outcome = run_landing_saga_with_runner("swm-qso", move |_command| {
            let next = pop_output(&mut outputs);
            async move { next }
        })
        .await;

        assert!(outcome.is_ok());
        if let Ok(result) = outcome {
            let payload = result.persistence_payload();
            assert_eq!(payload["initial_state"], serde_json::Value::from("pending"));
            assert_eq!(
                payload["terminal_state"],
                serde_json::Value::from("confirmed")
            );
            assert_eq!(payload["push_confirmed"], serde_json::Value::from(true));
            assert_eq!(
                payload["steps"][0]["state"],
                serde_json::Value::from("committing")
            );
            assert_eq!(
                payload["steps"][4]["state"],
                serde_json::Value::from("confirmed")
            );
        }
    }
}
