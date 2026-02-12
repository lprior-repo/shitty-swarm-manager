use crate::error::{Result, SwarmError};
use crate::gate_cache::GateExecutionCache;
use crate::skill_execution::SkillOutput;
use crate::types::ArtifactType;
use crate::{AgentId, BeadId, SwarmDb};
use tokio::process::Command;

use super::output_mapping::failure_output;

pub(super) async fn run_moon_task(
    task: &str,
    cache: Option<&GateExecutionCache>,
) -> Result<SkillOutput> {
    if let Some(cache) = cache {
        if let Some((_success, exit_code, stdout, stderr)) = cache.get(task).await {
            return Ok(SkillOutput::from_shell_output(&stdout, stderr, exit_code));
        }
    }

    let output = Command::new("moon")
        .args(["run", task])
        .output()
        .await
        .map_err(SwarmError::IoError)?;

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let exit_code = output.status.code();
    let success = exit_code.is_none_or(|code| code == 0);

    if let Some(cache) = cache {
        cache
            .put(
                task.to_string(),
                success,
                exit_code,
                stdout.clone(),
                stderr.clone(),
            )
            .await
            .ok();
    }

    Ok(SkillOutput::from_shell_output(&stdout, stderr, exit_code))
}

/// Execute the qa-enforcer stage.
///
/// This stage runs the fast quality gate and persists parsed test metadata.
pub(super) async fn execute_qa_stage(
    bead_id: &BeadId,
    agent_id: &AgentId,
    db: &SwarmDb,
    cache: Option<&GateExecutionCache>,
) -> Result<SkillOutput> {
    if !db
        .bead_has_artifact_type(
            agent_id.repo_id(),
            bead_id,
            ArtifactType::ImplementationCode,
        )
        .await?
    {
        return Ok(failure_output(
            "No implementation artifact found for QA stage".to_string(),
        ));
    }

    let mut output = run_moon_task(":quick", cache).await?;
    output.extract_qa_artifacts();

    if output.success {
        tracing::info!("Agent {} qa-enforcer passed for bead {}", agent_id, bead_id);
    } else {
        tracing::warn!(
            "Agent {} qa-enforcer failed for bead {}: {}",
            agent_id,
            bead_id,
            output.feedback
        );
    }

    Ok(output)
}

/// Execute the red-queen stage.
///
/// This stage runs the deeper test gate and records adversarial findings.
pub(super) async fn execute_red_queen_stage(
    bead_id: &BeadId,
    agent_id: &AgentId,
    db: &SwarmDb,
    cache: Option<&GateExecutionCache>,
) -> Result<SkillOutput> {
    if !db
        .bead_has_artifact_type(agent_id.repo_id(), bead_id, ArtifactType::TestResults)
        .await?
    {
        return Ok(failure_output(
            "No QA test_results artifact found for red-queen stage".to_string(),
        ));
    }

    let mut output = run_moon_task(":test", cache).await?;
    output.extract_red_queen_artifacts();

    if output.success {
        tracing::info!("Agent {} red-queen passed for bead {}", agent_id, bead_id);
    } else {
        tracing::warn!(
            "Agent {} red-queen failed for bead {}: {}",
            agent_id,
            bead_id,
            output.feedback
        );
    }

    Ok(output)
}
