//! Stage execution implementations using functional Rust patterns.
//!
//! This module provides zero-panic, zero-unwrap implementations for each
//! pipeline stage, replacing shell commands with proper Rust code.

use crate::error::{Result, SwarmError};
use crate::skill_execution::{store_skill_artifacts, SkillOutput};
use crate::stage_executor_content::{contract_document_and_artifacts, implementation_scaffold};
use crate::types::{ArtifactType, Stage, StageArtifact};
use crate::{AgentId, BeadId, SwarmDb};
use std::collections::HashMap;
use tokio::process::Command;

/// Execute a stage and return the result.
///
/// This is the main entry point for stage execution, replacing shell commands
/// with proper Rust implementations.
pub async fn execute_stage_rust(
    db: &SwarmDb,
    stage: Stage,
    bead_id: &BeadId,
    agent_id: &AgentId,
    stage_history_id: i64,
) -> crate::types::StageResult {
    if stage == Stage::Done {
        return crate::types::StageResult::Passed;
    }

    let stage_output = match stage {
        Stage::RustContract => execute_rust_contract_stage(bead_id, agent_id).await,
        Stage::Implement => execute_implement_stage(bead_id, agent_id, db).await,
        Stage::QaEnforcer => execute_qa_stage(bead_id, agent_id, db).await,
        Stage::RedQueen => execute_red_queen_stage(bead_id, agent_id, db).await,
        Stage::Done => Ok(success_output(
            "Done stage does not produce artifacts".to_string(),
        )),
    };

    match stage_output {
        Ok(output) => {
            let result = output_to_stage_result(&output);
            if let Err(err) = store_skill_artifacts(db, stage_history_id, stage, &output).await {
                return crate::types::StageResult::Error(format!(
                    "Failed to store stage artifacts: {}",
                    err
                ));
            }
            result
        }
        Err(err) => {
            let output = error_output(err.to_string());
            if let Err(store_err) =
                store_skill_artifacts(db, stage_history_id, stage, &output).await
            {
                return crate::types::StageResult::Error(format!(
                    "Stage execution failed ({}) and artifact storage failed ({})",
                    err, store_err
                ));
            }
            crate::types::StageResult::Error(err.to_string())
        }
    }
}

fn output_to_stage_result(output: &SkillOutput) -> crate::types::StageResult {
    if output.success {
        crate::types::StageResult::Passed
    } else {
        let message = if output.feedback.trim().is_empty() {
            if output.full_log.trim().is_empty() {
                "Stage failed".to_string()
            } else {
                output.full_log.clone()
            }
        } else {
            output.feedback.clone()
        };
        crate::types::StageResult::Failed(message)
    }
}

fn success_output(full_log: String) -> SkillOutput {
    SkillOutput {
        full_log,
        success: true,
        exit_code: Some(0),
        artifacts: HashMap::new(),
        feedback: String::new(),
        contract_document: None,
        implementation_code: None,
        modified_files: None,
        test_results: None,
        adversarial_report: None,
    }
}

fn failure_output(full_log: String) -> SkillOutput {
    SkillOutput {
        full_log: full_log.clone(),
        success: false,
        exit_code: Some(1),
        artifacts: HashMap::new(),
        feedback: full_log,
        contract_document: None,
        implementation_code: None,
        modified_files: None,
        test_results: None,
        adversarial_report: None,
    }
}

fn error_output(message: String) -> SkillOutput {
    SkillOutput {
        full_log: message.clone(),
        success: false,
        exit_code: Some(1),
        artifacts: HashMap::new(),
        feedback: message,
        contract_document: None,
        implementation_code: None,
        modified_files: None,
        test_results: None,
        adversarial_report: None,
    }
}

async fn run_moon_task(task: &str) -> Result<SkillOutput> {
    let output = Command::new("moon")
        .args(["run", task])
        .output()
        .await
        .map_err(SwarmError::IoError)?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    Ok(SkillOutput::from_shell_output(
        stdout,
        stderr,
        output.status.code(),
    ))
}

/// Execute the rust-contract stage.
///
/// This stage generates a contract document that follows a
/// behavior-first, acceptance-criteria-driven style.
async fn execute_rust_contract_stage(bead_id: &BeadId, agent_id: &AgentId) -> Result<SkillOutput> {
    let (contract_document, artifacts) = contract_document_and_artifacts(bead_id);

    tracing::info!("Agent {} generated contract for bead {}", agent_id, bead_id);

    Ok(SkillOutput {
        full_log: contract_document.clone(),
        success: true,
        exit_code: Some(0),
        artifacts,
        feedback: String::new(),
        contract_document: Some(contract_document),
        implementation_code: None,
        modified_files: None,
        test_results: None,
        adversarial_report: None,
    })
}

/// Execute the implement stage.
///
/// This stage composes implementation artifacts from the contract context.
async fn execute_implement_stage(
    bead_id: &BeadId,
    agent_id: &AgentId,
    db: &SwarmDb,
) -> Result<SkillOutput> {
    let contract_artifacts: Vec<StageArtifact> = db
        .get_bead_artifacts_by_type(bead_id, ArtifactType::ContractDocument)
        .await?;

    if contract_artifacts.is_empty() {
        return Ok(failure_output(
            "Missing contract artifact; cannot generate implementation context".to_string(),
        ));
    }

    let contract_context = match contract_artifacts.first() {
        Some(artifact) => artifact.content.as_str(),
        None => {
            return Ok(failure_output(
                "Contract artifact lookup returned no first artifact".to_string(),
            ));
        }
    };

    let implementation_code = implementation_scaffold(bead_id, contract_context);

    tracing::info!(
        "Agent {} generated implementation artifact for bead {}",
        agent_id,
        bead_id
    );

    Ok(SkillOutput {
        full_log: implementation_code.clone(),
        success: true,
        exit_code: Some(0),
        artifacts: HashMap::new(),
        feedback: String::new(),
        contract_document: None,
        implementation_code: Some(implementation_code),
        modified_files: Some(Vec::new()),
        test_results: None,
        adversarial_report: None,
    })
}

/// Execute the qa-enforcer stage.
///
/// This stage runs the fast quality gate and persists parsed test metadata.
async fn execute_qa_stage(
    bead_id: &BeadId,
    agent_id: &AgentId,
    db: &SwarmDb,
) -> Result<SkillOutput> {
    let impl_artifacts: Vec<StageArtifact> = db
        .get_bead_artifacts_by_type(bead_id, ArtifactType::ImplementationCode)
        .await?;

    if impl_artifacts.is_empty() {
        return Ok(failure_output(
            "No implementation artifact found for QA stage".to_string(),
        ));
    }

    let mut output = run_moon_task(":quick").await?;
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
async fn execute_red_queen_stage(
    bead_id: &BeadId,
    agent_id: &AgentId,
    db: &SwarmDb,
) -> Result<SkillOutput> {
    let test_artifacts: Vec<StageArtifact> = db
        .get_bead_artifacts_by_type(bead_id, ArtifactType::TestResults)
        .await?;

    if test_artifacts.is_empty() {
        return Ok(failure_output(
            "No QA test_results artifact found for red-queen stage".to_string(),
        ));
    }

    let mut output = run_moon_task(":test").await?;
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
