use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventSchemaVersion {
    V1,
}

impl EventSchemaVersion {
    #[must_use]
    pub const fn as_i32(&self) -> i32 {
        match self {
            Self::V1 => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureDiagnostics {
    pub category: String,
    pub retryable: bool,
    pub next_command: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionEvent {
    #[serde(rename = "sequence", alias = "seq")]
    pub seq: i64,
    #[serde(rename = "payload_version", alias = "schema_version")]
    pub schema_version: i32,
    pub event_type: String,
    pub entity_id: String,
    pub bead_id: Option<String>,
    pub agent_id: Option<u32>,
    pub stage: Option<String>,
    pub causation_id: Option<String>,
    pub diagnostics: Option<FailureDiagnostics>,
    pub payload: Option<serde_json::Value>,
    #[serde(rename = "timestamp", alias = "created_at")]
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::{ExecutionEvent, FailureDiagnostics};
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn serializes_execution_event_with_versioned_envelope_fields() -> Result<(), String> {
        let timestamp = Utc
            .with_ymd_and_hms(2026, 2, 11, 5, 30, 0)
            .single()
            .ok_or_else(|| "invalid test timestamp".to_string())?;

        let event = ExecutionEvent {
            seq: 42,
            schema_version: 1,
            event_type: "transition_retry".to_string(),
            entity_id: "bead:swm-1jg.1".to_string(),
            bead_id: Some("swm-1jg.1".to_string()),
            agent_id: Some(7),
            stage: Some("implement".to_string()),
            causation_id: Some("stage-history:99".to_string()),
            diagnostics: Some(FailureDiagnostics {
                category: "test_failure".to_string(),
                retryable: true,
                next_command: "swarm stage --stage implement".to_string(),
                detail: Some("assertion mismatch".to_string()),
            }),
            payload: Some(json!({"transition": "retry"})),
            created_at: timestamp,
        };

        let encoded = serde_json::to_value(event).map_err(|e| e.to_string())?;

        assert_eq!(encoded["sequence"], json!(42));
        assert_eq!(encoded["payload_version"], json!(1));
        assert!(encoded.get("seq").is_none());
        assert!(encoded.get("schema_version").is_none());
        assert!(encoded
            .get("timestamp")
            .is_some_and(serde_json::Value::is_string));
        assert!(encoded.get("created_at").is_none());

        Ok(())
    }

    #[test]
    fn deserializes_execution_event_from_legacy_storage_field_names() -> Result<(), String> {
        let raw = json!({
            "seq": 7,
            "schema_version": 1,
            "event_type": "stage_started",
            "entity_id": "bead:swm-1jg.1",
            "bead_id": "swm-1jg.1",
            "agent_id": 3,
            "stage": "implement",
            "causation_id": "stage-history:5",
            "diagnostics": {
                "category": "stage_failure",
                "retryable": true,
                "next_command": "swarm stage --stage implement",
                "detail": "redacted"
            },
            "payload": {"attempt": 1, "status": "started"},
            "created_at": "2026-02-11T05:30:00Z"
        });

        let decoded: ExecutionEvent = serde_json::from_value(raw).map_err(|e| e.to_string())?;
        assert_eq!(decoded.seq, 7);
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded.event_type, "stage_started");
        assert_eq!(decoded.causation_id.as_deref(), Some("stage-history:5"));

        Ok(())
    }

    #[test]
    fn serializes_failure_diagnostics_with_expected_schema() -> Result<(), String> {
        let diagnostics = FailureDiagnostics {
            category: "timeout".to_string(),
            retryable: true,
            next_command: "swarm stage --stage implement".to_string(),
            detail: Some("operation exceeded budget".to_string()),
        };

        let encoded = serde_json::to_value(diagnostics).map_err(|e| e.to_string())?;

        assert_eq!(encoded["category"], json!("timeout"));
        assert_eq!(encoded["retryable"], json!(true));
        assert_eq!(
            encoded["next_command"],
            json!("swarm stage --stage implement")
        );
        assert_eq!(encoded["detail"], json!("operation exceeded budget"));

        Ok(())
    }
}
