use super::identifiers::BeadId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MessageType {
    ContractReady,
    ImplementationReady,
    QaComplete,
    QaFailed,
    RedQueenFailed,
    ImplementationRetry,
    ArtifactAvailable,
    StageComplete,
    StageFailed,
    BlockingIssue,
    Coordination,
}

impl MessageType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ContractReady => "contract_ready",
            Self::ImplementationReady => "implementation_ready",
            Self::QaComplete => "qa_complete",
            Self::QaFailed => "qa_failed",
            Self::RedQueenFailed => "red_queen_failed",
            Self::ImplementationRetry => "implementation_retry",
            Self::ArtifactAvailable => "artifact_available",
            Self::StageComplete => "stage_complete",
            Self::StageFailed => "stage_failed",
            Self::BlockingIssue => "blocking_issue",
            Self::Coordination => "coordination",
        }
    }
}

impl TryFrom<&str> for MessageType {
    type Error = String;

    fn try_from(value: &str) -> std::result::Result<Self, String> {
        match value {
            "contract_ready" => Ok(Self::ContractReady),
            "implementation_ready" => Ok(Self::ImplementationReady),
            "qa_complete" => Ok(Self::QaComplete),
            "qa_failed" => Ok(Self::QaFailed),
            "red_queen_failed" => Ok(Self::RedQueenFailed),
            "implementation_retry" => Ok(Self::ImplementationRetry),
            "artifact_available" => Ok(Self::ArtifactAvailable),
            "stage_complete" => Ok(Self::StageComplete),
            "stage_failed" => Ok(Self::StageFailed),
            "blocking_issue" => Ok(Self::BlockingIssue),
            "coordination" => Ok(Self::Coordination),
            _ => Err(format!("Unknown message type: {}", value)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMessage {
    pub id: i64,
    pub from_repo_id: String,
    pub from_agent_id: u32,
    pub to_repo_id: Option<String>,
    pub to_agent_id: Option<u32>,
    pub bead_id: Option<BeadId>,
    pub message_type: MessageType,
    pub subject: String,
    pub body: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
    pub read: bool,
}
