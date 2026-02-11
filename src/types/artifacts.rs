use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ArtifactType {
    ContractDocument,
    Requirements,
    SystemContext,
    Invariants,
    DataFlow,
    ImplementationPlan,
    AcceptanceCriteria,
    ErrorHandling,
    TestScenarios,
    ValidationGates,
    SuccessMetrics,
    ImplementationCode,
    ModifiedFiles,
    ImplementationNotes,
    TestOutput,
    TestResults,
    CoverageReport,
    ValidationReport,
    FailureDetails,
    AdversarialReport,
    RegressionReport,
    QualityGateReport,
    StageLog,
    RetryPacket,
    SkillInvocation,
    ErrorMessage,
    Feedback,
}

impl ArtifactType {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ContractDocument => "contract_document",
            Self::Requirements => "requirements",
            Self::SystemContext => "system_context",
            Self::Invariants => "invariants",
            Self::DataFlow => "data_flow",
            Self::ImplementationPlan => "implementation_plan",
            Self::AcceptanceCriteria => "acceptance_criteria",
            Self::ErrorHandling => "error_handling",
            Self::TestScenarios => "test_scenarios",
            Self::ValidationGates => "validation_gates",
            Self::SuccessMetrics => "success_metrics",
            Self::ImplementationCode => "implementation_code",
            Self::ModifiedFiles => "modified_files",
            Self::ImplementationNotes => "implementation_notes",
            Self::TestOutput => "test_output",
            Self::TestResults => "test_results",
            Self::CoverageReport => "coverage_report",
            Self::ValidationReport => "validation_report",
            Self::FailureDetails => "failure_details",
            Self::AdversarialReport => "adversarial_report",
            Self::RegressionReport => "regression_report",
            Self::QualityGateReport => "quality_gate_report",
            Self::StageLog => "stage_log",
            Self::RetryPacket => "retry_packet",
            Self::SkillInvocation => "skill_invocation",
            Self::ErrorMessage => "error_message",
            Self::Feedback => "feedback",
        }
    }

    pub const ALL_STRINGS: [&'static str; 27] = [
        "contract_document",
        "requirements",
        "system_context",
        "invariants",
        "data_flow",
        "implementation_plan",
        "acceptance_criteria",
        "error_handling",
        "test_scenarios",
        "validation_gates",
        "success_metrics",
        "implementation_code",
        "modified_files",
        "implementation_notes",
        "test_output",
        "test_results",
        "coverage_report",
        "validation_report",
        "failure_details",
        "adversarial_report",
        "regression_report",
        "quality_gate_report",
        "stage_log",
        "retry_packet",
        "skill_invocation",
        "error_message",
        "feedback",
    ];

    #[must_use]
    pub const fn names() -> &'static [&'static str] {
        &Self::ALL_STRINGS
    }
}

impl TryFrom<&str> for ArtifactType {
    type Error = String;

    fn try_from(value: &str) -> std::result::Result<Self, String> {
        match value {
            "contract_document" => Ok(Self::ContractDocument),
            "requirements" => Ok(Self::Requirements),
            "system_context" => Ok(Self::SystemContext),
            "invariants" => Ok(Self::Invariants),
            "data_flow" => Ok(Self::DataFlow),
            "implementation_plan" => Ok(Self::ImplementationPlan),
            "acceptance_criteria" => Ok(Self::AcceptanceCriteria),
            "error_handling" => Ok(Self::ErrorHandling),
            "test_scenarios" => Ok(Self::TestScenarios),
            "validation_gates" => Ok(Self::ValidationGates),
            "success_metrics" => Ok(Self::SuccessMetrics),
            "implementation_code" => Ok(Self::ImplementationCode),
            "modified_files" => Ok(Self::ModifiedFiles),
            "implementation_notes" => Ok(Self::ImplementationNotes),
            "test_output" => Ok(Self::TestOutput),
            "test_results" => Ok(Self::TestResults),
            "coverage_report" => Ok(Self::CoverageReport),
            "validation_report" => Ok(Self::ValidationReport),
            "failure_details" => Ok(Self::FailureDetails),
            "adversarial_report" => Ok(Self::AdversarialReport),
            "regression_report" => Ok(Self::RegressionReport),
            "quality_gate_report" => Ok(Self::QualityGateReport),
            "stage_log" => Ok(Self::StageLog),
            "skill_invocation" => Ok(Self::SkillInvocation),
            "error_message" => Ok(Self::ErrorMessage),
            "feedback" => Ok(Self::Feedback),
            "retry_packet" => Ok(Self::RetryPacket),
            _ => Err(format!("Unknown artifact type: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageArtifact {
    pub id: i64,
    pub stage_history_id: i64,
    pub artifact_type: ArtifactType,
    pub content: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub content_hash: Option<String>,
}
