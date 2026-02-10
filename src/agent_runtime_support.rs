#![allow(clippy::literal_string_with_formatting_args)]

use crate::config::StageCommands;
use swarm::{
    ArtifactType, BeadId, MessageType, Result, Stage, StageArtifact, StageResult, SwarmError,
};
use tokio::process::Command;

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
