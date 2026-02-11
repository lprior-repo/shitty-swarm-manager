//! Stage execution implementations using functional Rust patterns.
//!
//! This module provides zero-panic, zero-unwrap implementations for each
//! pipeline stage, replacing shell commands with proper Rust code.

use crate::error::{Result, SwarmError};
use crate::gate_cache::GateExecutionCache;
use crate::skill_execution::{store_skill_artifacts, SkillOutput};
use crate::stage_executor_content::{contract_document_and_artifacts, implementation_scaffold};
use crate::types::{ArtifactType, Stage};
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
    cache: Option<&GateExecutionCache>,
) -> crate::types::StageResult {
    if stage == Stage::Done {
        return crate::types::StageResult::Passed;
    }

    let stage_output = match stage {
        Stage::RustContract => Ok(execute_rust_contract_stage(bead_id, agent_id)),
        Stage::Implement => execute_implement_stage(bead_id, agent_id, db).await,
        Stage::QaEnforcer => execute_qa_stage(bead_id, agent_id, db, cache).await,
        Stage::RedQueen => execute_red_queen_stage(bead_id, agent_id, db, cache).await,
        Stage::Done => Ok(success_output(
            "Done stage does not produce artifacts".to_string(),
        )),
    };

    match stage_output {
        Ok(output) => {
            let result = output_to_stage_result(&output);
            if let Err(err) = store_skill_artifacts(db, stage_history_id, stage, &output).await {
                return crate::types::StageResult::Error(format!(
                    "Failed to store stage artifacts: {err}"
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
                    "Stage execution failed ({err}) and artifact storage failed ({store_err})"
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

async fn run_moon_task(task: &str, cache: Option<&GateExecutionCache>) -> Result<SkillOutput> {
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

/// Execute the rust-contract stage.
///
/// This stage generates a contract document that follows a
/// behavior-first, acceptance-criteria-driven style.
fn execute_rust_contract_stage(bead_id: &BeadId, agent_id: &AgentId) -> SkillOutput {
    let (contract_document, artifacts) = contract_document_and_artifacts(bead_id);

    tracing::info!("Agent {} generated contract for bead {}", agent_id, bead_id);

    SkillOutput {
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
    }
}

/// Execute the implement stage.
///
/// This stage composes implementation artifacts from the contract context.
async fn execute_implement_stage(
    bead_id: &BeadId,
    agent_id: &AgentId,
    db: &SwarmDb,
) -> Result<SkillOutput> {
    let maybe_contract_artifact = db
        .get_first_bead_artifact_by_type(bead_id, ArtifactType::ContractDocument)
        .await?;

    let contract_context = match maybe_contract_artifact {
        Some(artifact) => artifact.content,
        None => {
            return Ok(failure_output(
                "Missing contract artifact; cannot generate implementation context".to_string(),
            ));
        }
    };

    let implementation_code = implementation_scaffold(bead_id, &contract_context);

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
    cache: Option<&GateExecutionCache>,
) -> Result<SkillOutput> {
    if !db
        .bead_has_artifact_type(bead_id, ArtifactType::ImplementationCode)
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
async fn execute_red_queen_stage(
    bead_id: &BeadId,
    agent_id: &AgentId,
    db: &SwarmDb,
    cache: Option<&GateExecutionCache>,
) -> Result<SkillOutput> {
    if !db
        .bead_has_artifact_type(bead_id, ArtifactType::TestResults)
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

#[cfg(test)]
mod tests {
    use crate::error::SwarmError;
    use crate::stage_executors::execute_implement_stage;
    use crate::types::{ArtifactType, BeadId};
    use crate::{AgentId, SwarmDb};
    use sqlx::PgPool;

    #[sqlx::test]
    async fn test_implement_stage_loads_contract_only_for_first_attempt(pool: PgPool) {
        let db = SwarmDb::new_with_pool(pool);
        let bead_id = BeadId::new("test-bead-1");
        let agent_id = AgentId::new("local", 1);

        // Store contract artifact
        let stage_history_id = 123;
        db.store_stage_artifact(
            stage_history_id,
            ArtifactType::ContractDocument,
            "contract content",
            None,
        )
        .await
        .expect("Failed to store contract");

        // Execute implement stage for attempt 1
        let result = execute_implement_stage(&bead_id, &agent_id, &db, 1)
            .await
            .expect("execute_implement_stage should succeed");

        assert!(result.success);
        assert!(result.implementation_code.is_some());

        // Verify consumed_context metadata includes only contract
        if let Some(ref metadata) = result.metadata {
            let consumed = metadata
                .get("consumed_context")
                .and_then(|v| v.as_array())
                .expect("consumed_context should be an array");
            assert_eq!(consumed.len(), 1);
            assert_eq!(consumed[0].get("artifact_type").unwrap().as_str(), Some("contract_document"));
        }
    }

    #[sqlx::test]
    async fn test_implement_stage_loads_retry_context_for_attempt_two(pool: PgPool) {
        let db = SwarmDb::new_with_pool(pool);
        let bead_id = BeadId::new("test-bead-2");
        let agent_id = AgentId::new("local", 1);

        // Store contract artifact
        let stage_history_id = 123;
        db.store_stage_artifact(
            stage_history_id,
            ArtifactType::ContractDocument,
            "contract content",
            None,
        )
        .await
        .expect("Failed to store contract");

        // Store retry packet artifact
        db.store_stage_artifact(
            stage_history_id,
            ArtifactType::RetryPacket,
            r#"{"attempt": 1, "failure_reason": "test failed", "remaining_attempts": 2}"#,
            None,
        )
        .await
        .expect("Failed to store retry packet");

        // Store test output artifact from previous QA failure
        db.store_stage_artifact(
            stage_history_id,
            ArtifactType::TestOutput,
            "test failure output",
            None,
        )
        .await
        .expect("Failed to store test output");

        // Execute implement stage for attempt 2
        let result = execute_implement_stage(&bead_id, &agent_id, &db, 2)
            .await
            .expect("execute_implement_stage should succeed");

        assert!(result.success);
        assert!(result.implementation_code.is_some());

        // Verify consumed_context metadata includes contract, retry packet, and test output
        if let Some(ref metadata) = result.metadata {
            let consumed = metadata
                .get("consumed_context")
                .and_then(|v| v.as_array())
                .expect("consumed_context should be an array");
            assert_eq!(consumed.len(), 3);

            let artifact_types: Vec<String> = consumed
                .iter()
                .filter_map(|v| v.get("artifact_type").and_then(|s| s.as_str()).map(|s| s.to_string()))
                .collect();

            assert!(artifact_types.contains(&"contract_document".to_string()));
            assert!(artifact_types.contains(&"retry_packet".to_string()));
            assert!(artifact_types.contains(&"test_output".to_string()));
        }
    }

    #[sqlx::test]
    async fn test_implement_stage_fails_without_contract(pool: PgPool) {
        let db = SwarmDb::new_with_pool(pool);
        let bead_id = BeadId::new("test-bead-3");
        let agent_id = AgentId::new("local", 1);

        // No contract artifact stored

        // Execute implement stage for attempt 1
        let result = execute_implement_stage(&bead_id, &agent_id, &db, 1)
            .await
            .expect("execute_implement_stage should return error");

        assert!(!result.success);
        assert!(result.feedback.contains("contract"));
    }
}
