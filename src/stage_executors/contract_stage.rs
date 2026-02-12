use crate::skill_execution::SkillOutput;
use crate::stage_executor_content::contract_document_and_artifacts;
use crate::{AgentId, BeadId};

/// Execute the rust-contract stage.
///
/// This stage generates a contract document that follows a
/// behavior-first, acceptance-criteria-driven style.
pub(super) fn execute_rust_contract_stage(bead_id: &BeadId, agent_id: &AgentId) -> SkillOutput {
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
