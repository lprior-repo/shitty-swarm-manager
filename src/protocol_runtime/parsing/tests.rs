use super::{
    bounded_history_limit, json_value_type_name, parse_database_connect_timeout_ms,
    parse_optional_non_negative_u32, parse_optional_non_negative_u64, parse_rid,
    request_connect_timeout_ms,
};
use crate::{
    code, protocol_envelope::ProtocolEnvelope, protocol_runtime::ParseError, ProtocolRequest,
};
use serde_json::{json, Map, Value};

fn request_with_args(args: Map<String, Value>) -> ProtocolRequest {
    ProtocolRequest {
        cmd: "history".to_string(),
        rid: Some("rid-parse".to_string()),
        dry: None,
        args,
    }
}

#[test]
fn given_missing_field_when_parse_optional_non_negative_u64_then_returns_none() {
    let request = request_with_args(Map::new());

    let parsed = parse_optional_non_negative_u64(&request, "watch_ms");

    assert!(parsed.is_ok());
    assert_eq!(parsed.ok().flatten(), None);
}

#[test]
fn given_negative_i64_when_parse_optional_non_negative_u64_then_returns_invalid_value() {
    let request = request_with_args(Map::from_iter(vec![("watch_ms".to_string(), json!(-1))]));

    let parsed = parse_optional_non_negative_u64(&request, "watch_ms");

    assert!(matches!(
        parsed,
        Err(ParseError::InvalidValue { field, value }) if field == "watch_ms" && value == "must be non-negative"
    ));
}

#[test]
fn given_non_numeric_value_when_parse_optional_non_negative_u64_then_returns_invalid_type() {
    let request = request_with_args(Map::from_iter(vec![(
        "watch_ms".to_string(),
        json!("1000"),
    )]));

    let parsed = parse_optional_non_negative_u64(&request, "watch_ms");

    assert!(matches!(
        parsed,
        Err(ParseError::InvalidType { field, expected, got })
            if field == "watch_ms" && expected == "u64" && got == "string"
    ));
}

#[test]
fn given_value_above_u32_max_when_parse_optional_non_negative_u32_then_returns_invalid_value() {
    let request = request_with_args(Map::from_iter(vec![(
        "seed_agents".to_string(),
        json!(u32::MAX as u64 + 1),
    )]));

    let parsed = parse_optional_non_negative_u32(&request, "seed_agents");

    assert!(matches!(
        parsed,
        Err(ParseError::InvalidValue { field, value })
            if field == "seed_agents" && value.contains("exceeds max u32")
    ));
}

#[test]
fn given_requested_limit_when_bounded_history_limit_then_applies_default_and_cap() {
    assert_eq!(bounded_history_limit(None, 100, 500), 100);
    assert_eq!(bounded_history_limit(Some(250), 100, 500), 250);
    assert_eq!(bounded_history_limit(Some(9_999), 100, 500), 500);
}

#[test]
fn given_raw_connect_timeout_when_parse_database_connect_timeout_ms_then_trims_parses_and_clamps() {
    assert_eq!(
        parse_database_connect_timeout_ms(Some("  "), 3_000, 100, 30_000),
        3_000
    );
    assert_eq!(
        parse_database_connect_timeout_ms(Some("abc"), 3_000, 100, 30_000),
        3_000
    );
    assert_eq!(
        parse_database_connect_timeout_ms(Some("50"), 3_000, 100, 30_000),
        100
    );
    assert_eq!(
        parse_database_connect_timeout_ms(Some("45000"), 3_000, 100, 30_000),
        30_000
    );
}

#[test]
fn given_invalid_connect_timeout_arg_when_request_connect_timeout_ms_then_returns_invalid_envelope()
{
    let request = request_with_args(Map::from_iter(vec![(
        "connect_timeout_ms".to_string(),
        json!("slow"),
    )]));

    let result = request_connect_timeout_ms(&request, 3_000, 100, 30_000);

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
    assert_eq!(envelope.rid, Some("rid-parse".to_string()));
    assert_eq!(error.code, code::INVALID.to_string());
    assert!(error
        .msg
        .contains("Invalid type for field connect_timeout_ms"));
}

#[test]
fn given_json_values_when_json_value_type_name_then_returns_expected_labels() {
    assert_eq!(json_value_type_name(&Value::Null), "null");
    assert_eq!(json_value_type_name(&json!(true)), "bool");
    assert_eq!(json_value_type_name(&json!(123)), "number");
    assert_eq!(json_value_type_name(&json!("abc")), "string");
    assert_eq!(json_value_type_name(&json!([1, 2])), "array");
    assert_eq!(json_value_type_name(&json!({"k": 1})), "object");
}

#[test]
fn given_json_lines_when_parse_rid_then_extracts_only_valid_string_rid() {
    assert_eq!(
        parse_rid(r#"{"cmd":"doctor","rid":"abc-123"}"#),
        Some("abc-123".to_string())
    );
    assert_eq!(parse_rid(r#"{"cmd":"doctor","rid":123}"#), None);
    assert_eq!(parse_rid("not-json"), None);
}
