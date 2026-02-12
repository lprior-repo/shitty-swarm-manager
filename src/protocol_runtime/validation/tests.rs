use super::{validate_request_args, validate_request_null_bytes};
use crate::{code, protocol_envelope::ProtocolEnvelope, ProtocolRequest};
use serde_json::{json, Map, Value};

fn request(cmd: &str, args: Map<String, Value>) -> ProtocolRequest {
    ProtocolRequest {
        cmd: cmd.to_string(),
        rid: Some("rid-val".to_string()),
        dry: None,
        args,
    }
}

#[test]
fn given_unknown_command_when_validate_request_args_then_it_is_passthrough() {
    let request = request(
        "custom-cmd",
        Map::from_iter(vec![(
            "anything".to_string(),
            Value::String("ok".to_string()),
        )]),
    );

    let result = validate_request_args(&request);

    assert!(result.is_ok());
}

#[test]
fn given_known_command_with_only_allowed_and_global_fields_when_validate_request_args_then_ok() {
    let request = request(
        "history",
        Map::from_iter(vec![
            ("limit".to_string(), Value::from(25)),
            (
                "repo_id".to_string(),
                Value::String("repo-main".to_string()),
            ),
            (
                "database_url".to_string(),
                Value::String("postgres://localhost/test".to_string()),
            ),
            ("connect_timeout_ms".to_string(), Value::from(4000)),
        ]),
    );

    let result = validate_request_args(&request);

    assert!(result.is_ok());
}

#[test]
fn given_unknown_fields_when_validate_request_args_then_error_contains_unknown_and_allowed() {
    let request = request(
        "history",
        Map::from_iter(vec![
            ("limit".to_string(), Value::from(10)),
            ("bogus".to_string(), Value::String("x".to_string())),
            ("extra".to_string(), Value::Bool(true)),
        ]),
    );

    let result = validate_request_args(&request);

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
    assert!(error.msg.contains("Unknown field(s) for history"));
    let ctx = error.ctx.map_or(Value::Null, |value| *value);
    assert_eq!(ctx["cmd"], Value::String("history".to_string()));
    assert_eq!(ctx["unknown"], json!(["bogus", "extra"]));
    assert_eq!(
        ctx["allowed"],
        json!(["connect_timeout_ms", "database_url", "limit", "repo_id"])
    );
}

#[test]
fn given_null_byte_in_cmd_when_validate_request_null_bytes_then_returns_cmd_field_error() {
    let request = request("his\0tory", Map::new());

    let result = validate_request_null_bytes(&request);

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
    assert_eq!(error.msg, "Null byte is not allowed in cmd".to_string());
    assert_eq!(
        error.ctx.map_or(Value::Null, |value| *value),
        json!({"field": "cmd"})
    );
}

#[test]
fn given_null_byte_in_nested_array_field_when_validate_request_null_bytes_then_reports_precise_path(
) {
    let bad_rid = format!("bad{}-rid", '\0');
    let request = request(
        "batch",
        Map::from_iter(vec![(
            "ops".to_string(),
            json!([
                {"cmd": "doctor"},
                {"cmd": "history", "meta": {"rid": bad_rid}}
            ]),
        )]),
    );

    let result = validate_request_null_bytes(&request);

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
    assert!(error.msg.contains("ops[1].meta.rid"));
    assert_eq!(
        error.ctx.map_or(Value::Null, |value| *value),
        json!({"field": "ops[1].meta.rid"})
    );
}

#[test]
fn given_clean_request_when_validate_request_null_bytes_then_ok() {
    let request = request(
        "assign",
        Map::from_iter(vec![
            ("bead_id".to_string(), Value::String("bead-123".to_string())),
            ("agent_id".to_string(), Value::String("agent-5".to_string())),
        ]),
    );

    let result = validate_request_null_bytes(&request);

    assert!(result.is_ok());
}
