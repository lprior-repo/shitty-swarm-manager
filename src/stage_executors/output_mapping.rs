use crate::skill_execution::SkillOutput;
use std::collections::HashMap;

pub(super) fn output_to_stage_result(output: &SkillOutput) -> crate::types::StageResult {
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

pub(super) fn success_output(full_log: String) -> SkillOutput {
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

pub(super) fn failure_output(full_log: String) -> SkillOutput {
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

pub(super) fn error_output(message: String) -> SkillOutput {
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
