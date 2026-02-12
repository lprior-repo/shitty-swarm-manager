use super::db_resolution;
use super::ProtocolRequest;
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, ProgressSummary, SwarmError};
use serde_json::{json, Value};

pub(super) fn required_string_arg(
    request: &ProtocolRequest,
    key: &str,
) -> std::result::Result<String, Box<ProtocolEnvelope>> {
    request
        .args
        .get(key)
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    format!("Missing required field: {key}"),
                )
                .with_fix(format!(
                    "Add '{key}' field to request. Example: echo '{{\"cmd\":\"agent\",\"{key}\":<value>}}' | swarm"
                ))
                .with_ctx(json!({key: "required"})),
            )
        })
}

pub(super) fn to_protocol_failure(error: SwarmError, rid: Option<String>) -> Box<ProtocolEnvelope> {
    Box::new(
        ProtocolEnvelope::error(rid, error.code().to_string(), error.to_string())
            .with_fix("Check error details and retry with corrected parameters".to_string())
            .with_ctx(json!({"error": error.to_string()})),
    )
}

pub(super) fn dry_flag(request: &ProtocolRequest) -> bool {
    request.dry.is_some_and(|value| value)
}

pub(super) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub(super) async fn minimal_state_for_request(
    request: &ProtocolRequest,
    default_timeout_ms: u64,
    min_timeout_ms: u64,
    max_timeout_ms: u64,
) -> Value {
    let repo_id = db_resolution::repo_id_from_request(request);
    match db_resolution::db_from_request(
        request,
        default_timeout_ms,
        min_timeout_ms,
        max_timeout_ms,
    )
    .await
    {
        Ok(db) => match db.get_progress(&repo_id).await {
            Ok(progress) => minimal_state_from_progress(&progress),
            Err(_) => json!({"total": 0, "active": 0}),
        },
        Err(_) => json!({"total": 0, "active": 0}),
    }
}

pub(super) fn minimal_state_from_progress(progress: &ProgressSummary) -> Value {
    json!({
        "total": progress.total_agents,
        "active": progress.working + progress.waiting + progress.errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map, Value};

    fn request_with_args(args: Map<String, Value>) -> ProtocolRequest {
        ProtocolRequest {
            cmd: "lock".to_string(),
            rid: Some("rid-123".to_string()),
            dry: None,
            args,
        }
    }

    #[test]
    fn given_required_string_present_when_required_string_arg_then_returns_value() {
        let request = request_with_args(Map::from_iter(vec![(
            "resource".to_string(),
            Value::String("repo-1".to_string()),
        )]));

        let result = required_string_arg(&request, "resource");

        assert!(result.is_ok());
        assert_eq!(result.unwrap_or_default(), "repo-1".to_string());
    }

    #[test]
    fn given_required_string_missing_when_required_string_arg_then_returns_invalid_envelope() {
        let request = request_with_args(Map::new());

        let result = required_string_arg(&request, "resource");

        assert!(result.is_err());
        let envelope = result.err().map_or_else(
            || ProtocolEnvelope::error(None, "INTERNAL".to_string(), "missing".to_string()),
            |value| *value,
        );
        let error = envelope.err.map_or_else(
            || crate::protocol_envelope::ProtocolError {
                code: "INTERNAL".to_string(),
                msg: "missing".to_string(),
                ctx: None,
            },
            |value| *value,
        );
        assert_eq!(error.code, code::INVALID.to_string());
        assert!(error.msg.contains("Missing required field: resource"));
        assert_eq!(envelope.fix.unwrap_or_default(), "Add 'resource' field to request. Example: echo '{\"cmd\":\"agent\",\"resource\":<value>}' | swarm".to_string());
        assert_eq!(
            error.ctx.map_or(Value::Null, |value| *value),
            json!({"resource": "required"})
        );
    }

    #[test]
    fn given_internal_error_when_to_protocol_failure_then_maps_error_code_and_context() {
        let envelope = to_protocol_failure(
            SwarmError::Internal("boom".to_string()),
            Some("rid-42".to_string()),
        );

        assert!(!envelope.ok);
        assert_eq!(envelope.rid, Some("rid-42".to_string()));
        let error = envelope.err.map_or_else(
            || crate::protocol_envelope::ProtocolError {
                code: "INTERNAL".to_string(),
                msg: "missing".to_string(),
                ctx: None,
            },
            |value| *value,
        );
        assert_eq!(error.code, code::INTERNAL.to_string());
        assert!(error.msg.contains("Internal error: boom"));
        assert_eq!(
            error.ctx.map_or(Value::Null, |value| *value),
            json!({"error": "Internal error: boom"})
        );
    }

    #[test]
    fn given_dry_flag_variants_when_dry_flag_then_only_some_true_is_true() {
        let request_none = ProtocolRequest {
            cmd: "status".to_string(),
            rid: None,
            dry: None,
            args: Map::new(),
        };
        let request_false = ProtocolRequest {
            cmd: "status".to_string(),
            rid: None,
            dry: Some(false),
            args: Map::new(),
        };
        let request_true = ProtocolRequest {
            cmd: "status".to_string(),
            rid: None,
            dry: Some(true),
            args: Map::new(),
        };

        assert!(!dry_flag(&request_none));
        assert!(!dry_flag(&request_false));
        assert!(dry_flag(&request_true));
    }

    #[test]
    fn given_progress_summary_when_minimal_state_from_progress_then_total_and_active_are_projected()
    {
        let progress = ProgressSummary {
            completed: 4,
            working: 3,
            waiting: 2,
            errors: 1,
            idle: 0,
            total_agents: 10,
        };

        let state = minimal_state_from_progress(&progress);

        assert_eq!(state, json!({"total": 10, "active": 6}));
    }
}
