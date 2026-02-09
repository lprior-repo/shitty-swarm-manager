//! Skill execution framework for agent stages.

use crate::error::{Result, SwarmError};
use crate::skill_execution_parsing::{parse_test_results, TestResults};
use crate::types::{ArtifactType, Stage};
use crate::SwarmDb;
use futures_util::future::try_join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillOutput {
    pub full_log: String,
    pub success: bool,
    pub exit_code: Option<i32>,
    pub artifacts: HashMap<String, String>,
    pub feedback: String,
    pub contract_document: Option<String>,
    pub implementation_code: Option<String>,
    pub modified_files: Option<Vec<String>>,
    pub test_results: Option<TestResults>,
    pub adversarial_report: Option<String>,
}

/// Metadata about a skill invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillInvocationMetadata {
    /// Name of the skill that was invoked
    pub skill_name: String,
    /// Arguments passed to the skill
    pub args: Vec<String>,
    /// Duration of the skill execution
    pub duration_ms: u64,
    /// Environment variables
    pub env: HashMap<String, String>,
}

impl SkillOutput {
    /// Create a new skill output from shell command execution.
    pub fn from_shell_output(stdout: String, stderr: String, exit_code: Option<i32>) -> Self {
        let success = exit_code.is_none_or(|code| code == 0);
        let full_log = match (stdout.is_empty(), stderr.is_empty()) {
            (true, _) => stderr,
            (_, true) => stdout.clone(),
            _ => format!("{}\n{}", stdout, stderr),
        };

        let feedback = match success {
            true => String::new(),
            false => full_log.clone(),
        };

        Self {
            full_log,
            success,
            exit_code,
            artifacts: HashMap::new(),
            feedback,
            contract_document: None,
            implementation_code: None,
            modified_files: None,
            test_results: None,
            adversarial_report: None,
        }
    }

    /// Extract artifacts for the rust-contract stage.
    pub fn extract_contract_artifacts(&mut self) {
        // The rust-contract skill produces a comprehensive markdown contract document.
        // For now, we store the full log as the contract document.
        // In production, this would parse the markdown sections.
        if self.success {
            self.contract_document = Some(self.full_log.clone());
            self.artifacts
                .insert("contract_document".to_string(), self.full_log.clone());
        }
    }

    /// Extract artifacts for the implement stage.
    pub fn extract_implementation_artifacts(&mut self) {
        // The functional-rust-generator skill produces Rust code.
        // For now, we store the full log as the implementation.
        // In production, this would parse the actual code blocks.
        if self.success {
            self.implementation_code = Some(self.full_log.clone());
            self.artifacts
                .insert("implementation_code".to_string(), self.full_log.clone());
        }
    }

    /// Extract artifacts for the qa-enforcer stage.
    pub fn extract_qa_artifacts(&mut self) {
        // The qa-enforcer skill produces test execution results.
        // For now, we store the full log as test output.
        // In production, this would parse test results from various test frameworks.
        self.artifacts
            .insert("test_output".to_string(), self.full_log.clone());

        if !self.success {
            self.artifacts
                .insert("failure_details".to_string(), self.full_log.clone());
        }

        // Parse test results if available
        self.test_results = Some(parse_test_results(&self.full_log));
    }

    /// Extract artifacts for the red-queen stage.
    pub fn extract_red_queen_artifacts(&mut self) {
        // The red-queen skill produces adversarial test reports.
        // For now, we store the full log as the report.
        if self.success {
            self.artifacts
                .insert("quality_gate_report".to_string(), self.full_log.clone());
        } else {
            self.adversarial_report = Some(self.full_log.clone());
            self.artifacts
                .insert("adversarial_report".to_string(), self.full_log.clone());
        }
    }
}

/// Store skill artifacts to the database.
pub async fn store_skill_artifacts(
    db: &SwarmDb,
    stage_history_id: i64,
    stage: Stage,
    output: &SkillOutput,
) -> Result<()> {
    let skill_name = match stage {
        Stage::RustContract => "rust-contract",
        Stage::Implement => "functional-rust-generator",
        Stage::QaEnforcer => "qa-enforcer",
        Stage::RedQueen => "red-queen",
        Stage::Done => return Ok(()),
    };

    let mut pending_artifacts: Vec<(ArtifactType, String, Option<serde_json::Value>)> = vec![
        (
            ArtifactType::StageLog,
            output.full_log.clone(),
            Some(serde_json::json!({
                "exit_code": output.exit_code,
                "success": output.success,
            })),
        ),
        (
            ArtifactType::SkillInvocation,
            format!("Skill: {}", skill_name),
            Some(serde_json::json!({
                "skill_name": skill_name,
            })),
        ),
    ];

    match stage {
        Stage::RustContract => {
            if let Some(ref contract) = output.contract_document {
                pending_artifacts.push((ArtifactType::ContractDocument, contract.clone(), None));
            }
        }
        Stage::Implement => {
            if let Some(ref impl_code) = output.implementation_code {
                pending_artifacts.push((ArtifactType::ImplementationCode, impl_code.clone(), None));
            }
            if let Some(ref files) = output.modified_files {
                let files_json = serde_json::to_string(files).map_err(|e| {
                    SwarmError::DatabaseError(format!("Failed to serialize files: {}", e))
                })?;
                pending_artifacts.push((ArtifactType::ModifiedFiles, files_json, None));
            }
        }
        Stage::QaEnforcer => {
            if !output.success {
                pending_artifacts.push((
                    ArtifactType::FailureDetails,
                    output.feedback.clone(),
                    None,
                ));
            }
            if let Some(ref test_results) = output.test_results {
                let results_json = serde_json::to_string(test_results).map_err(|e| {
                    SwarmError::DatabaseError(format!("Failed to serialize test results: {}", e))
                })?;
                pending_artifacts.push((ArtifactType::TestResults, results_json, None));
            }
        }
        Stage::RedQueen => {
            if !output.success {
                if let Some(ref report) = output.adversarial_report {
                    pending_artifacts.push((ArtifactType::AdversarialReport, report.clone(), None));
                }
            } else {
                pending_artifacts.push((
                    ArtifactType::QualityGateReport,
                    output.full_log.clone(),
                    None,
                ));
            }
        }
        Stage::Done => {}
    }

    for (name, value) in &output.artifacts {
        let artifact_type = match ArtifactType::try_from(name.as_str()) {
            Ok(value_type) => value_type,
            Err(_) => continue,
        };

        pending_artifacts.push((artifact_type, value.clone(), None));
    }

    let store_futures =
        pending_artifacts
            .into_iter()
            .map(|(artifact_type, content, metadata)| async move {
                db.store_stage_artifact(stage_history_id, artifact_type, &content, metadata)
                    .await
                    .map(|_| ())
            });

    try_join_all(store_futures).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skill_output_from_success() {
        let output =
            SkillOutput::from_shell_output("All tests passed".to_string(), String::new(), Some(0));

        assert!(output.success);
        assert_eq!(output.full_log, "All tests passed");
        assert_eq!(output.exit_code, Some(0));
        assert!(output.feedback.is_empty());
    }

    #[test]
    fn test_skill_output_from_failure() {
        let output = SkillOutput::from_shell_output(
            "Running tests".to_string(),
            "Test failed".to_string(),
            Some(1),
        );

        assert!(!output.success);
        assert!(output.full_log.contains("Test failed"));
        assert_eq!(output.exit_code, Some(1));
        assert!(!output.feedback.is_empty());
    }
}
