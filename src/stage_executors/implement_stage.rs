use crate::error::Result;
use crate::skill_execution::SkillOutput;
use crate::stage_executor_content::implementation_scaffold;
use crate::types::ArtifactType;
use crate::{AgentId, BeadId, SwarmDb};
use serde_json::Value;
use std::collections::HashMap;

use super::output_mapping::failure_output;

/// Execute the implement stage.
///
/// This stage composes implementation artifacts from the contract context.
pub(super) async fn execute_implement_stage(
    bead_id: &BeadId,
    agent_id: &AgentId,
    db: &SwarmDb,
) -> Result<SkillOutput> {
    let maybe_contract_artifact = db
        .get_first_bead_artifact_by_type(
            agent_id.repo_id(),
            bead_id,
            ArtifactType::ContractDocument,
        )
        .await?;

    let contract_context = match maybe_contract_artifact {
        Some(artifact) => artifact.content,
        None => {
            return Ok(failure_output(
                "Missing contract artifact; cannot generate implementation context".to_string(),
            ));
        }
    };

    let previous_attempts = db
        .get_agent_state(agent_id)
        .await?
        .map_or(0, |state| state.implementation_attempt());

    let retry_packet_context = if previous_attempts > 0 {
        match db
            .get_latest_bead_artifact_by_type(
                agent_id.repo_id(),
                bead_id,
                ArtifactType::RetryPacket,
            )
            .await?
        {
            Some(artifact) => Some(format_retry_packet(&artifact.content)),
            None => {
                return Ok(failure_output(
                    "Missing retry packet; cannot resume deterministic implement attempt"
                        .to_string(),
                ));
            }
        }
    } else {
        None
    };

    let failure_details = db
        .get_latest_bead_artifact_by_type(agent_id.repo_id(), bead_id, ArtifactType::FailureDetails)
        .await?
        .map(|artifact| artifact.content);

    let test_output = db
        .get_latest_bead_artifact_by_type(agent_id.repo_id(), bead_id, ArtifactType::TestOutput)
        .await?
        .map(|artifact| artifact.content);

    let test_results = db
        .get_latest_bead_artifact_by_type(agent_id.repo_id(), bead_id, ArtifactType::TestResults)
        .await?
        .map(|artifact| artifact.content);

    let mut context_sections = Vec::new();
    context_sections.push(format!("## Contract Document\n{}", contract_context.trim()));
    append_section(
        &mut context_sections,
        "Retry Packet",
        retry_packet_context.as_deref(),
    );
    append_section(
        &mut context_sections,
        "Failure Details",
        failure_details.as_deref(),
    );
    append_section(
        &mut context_sections,
        "Test Results",
        test_results.as_deref(),
    );
    append_section(&mut context_sections, "Test Output", test_output.as_deref());

    let aggregated_context = context_sections.join("\n\n");
    let implementation_code = implementation_scaffold(bead_id, &aggregated_context);

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

pub(super) fn append_section(sections: &mut Vec<String>, title: &str, content: Option<&str>) {
    if let Some(body) = content {
        let trimmed = body.trim();
        if !trimmed.is_empty() {
            sections.push(format!("## {title}\n{trimmed}"));
        }
    }
}

pub(super) fn format_retry_packet(payload: &str) -> String {
    serde_json::from_str::<Value>(payload).map_or_else(
        |_| payload.to_string(),
        |value| {
            serde_json::to_string_pretty(&value)
                .map_or_else(|_| payload.to_string(), std::convert::identity)
        },
    )
}
