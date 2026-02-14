use super::{ParseError, ProtocolRequest};
use crate::code;
use crate::protocol_envelope::ProtocolEnvelope;
use serde_json::{json, Value};

#[cfg(test)]
pub(super) fn parse_optional_non_negative_u64(
    request: &ProtocolRequest,
    key: &str,
) -> Result<Option<u64>, ParseError> {
    request
        .args
        .get(key)
        .map(|raw| {
            if raw.as_i64().is_some_and(|value| value < 0) {
                return Err(ParseError::InvalidValue {
                    field: key.to_string(),
                    value: "must be non-negative".to_string(),
                });
            }

            raw.as_u64().ok_or_else(|| ParseError::InvalidType {
                field: key.to_string(),
                expected: "u64".to_string(),
                got: json_value_type_name(raw).to_string(),
            })
        })
        .transpose()
}

#[cfg(test)]
pub(super) fn parse_optional_non_negative_i64(
    request: &ProtocolRequest,
    key: &str,
) -> Result<Option<i64>, ParseError> {
    let Some(raw) = request.args.get(key) else {
        return Ok(None);
    };

    if let Some(value) = raw.as_i64() {
        if value < 0 {
            return Err(ParseError::InvalidValue {
                field: key.to_string(),
                value: "must be non-negative".to_string(),
            });
        }
        return Ok(Some(value));
    }

    Err(ParseError::InvalidType {
        field: key.to_string(),
        expected: "i64".to_string(),
        got: json_value_type_name(raw).to_string(),
    })
}

#[cfg(test)]
pub(super) fn parse_optional_non_negative_u32(
    request: &ProtocolRequest,
    key: &str,
) -> Result<Option<u32>, ParseError> {
    request
        .args
        .get(key)
        .map(|raw| {
            if raw.as_i64().is_some_and(|value| value < 0) {
                return Err(ParseError::InvalidValue {
                    field: key.to_string(),
                    value: "must be non-negative".to_string(),
                });
            }

            let value = raw.as_u64().ok_or_else(|| ParseError::InvalidType {
                field: key.to_string(),
                expected: "u32".to_string(),
                got: json_value_type_name(raw).to_string(),
            })?;

            u32::try_from(value).map_err(|_| ParseError::InvalidValue {
                field: key.to_string(),
                value: format!("{value} exceeds max u32"),
            })
        })
        .transpose()
}

pub(super) fn bounded_history_limit(
    requested_limit: Option<i64>,
    default_limit: i64,
    max_limit: i64,
) -> i64 {
    requested_limit.map_or(default_limit, |limit| limit.min(max_limit))
}

pub(super) fn parse_database_connect_timeout_ms(
    raw: Option<&str>,
    default_timeout_ms: u64,
    min_timeout_ms: u64,
    max_timeout_ms: u64,
) -> u64 {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(default_timeout_ms, |value| {
            value.clamp(min_timeout_ms, max_timeout_ms)
        })
}

pub(super) fn request_connect_timeout_ms(
    request: &ProtocolRequest,
    default_timeout_ms: u64,
    min_timeout_ms: u64,
    max_timeout_ms: u64,
) -> std::result::Result<u64, Box<ProtocolEnvelope>> {
    request
        .args
        .get("connect_timeout_ms")
        .map(|raw| parse_connect_timeout_value(raw, min_timeout_ms, max_timeout_ms))
        .transpose()
        .map(|maybe| {
            maybe.unwrap_or_else(|| {
                parse_database_connect_timeout_ms(
                    std::env::var("SWARM_DB_CONNECT_TIMEOUT_MS").ok().as_deref(),
                    default_timeout_ms,
                    min_timeout_ms,
                    max_timeout_ms,
                )
            })
        })
        .map_err(|error| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    error.to_string(),
                )
                .with_fix("Use connect_timeout_ms as an integer between 100 and 30000".to_string())
                .with_ctx(json!({"error": error.to_string()})),
            )
        })
}

fn parse_connect_timeout_value(
    raw: &Value,
    min_timeout_ms: u64,
    max_timeout_ms: u64,
) -> std::result::Result<u64, ParseError> {
    let Some(value) = raw.as_u64() else {
        return Err(ParseError::InvalidType {
            field: "connect_timeout_ms".to_string(),
            expected: "u64".to_string(),
            got: json_value_type_name(raw).to_string(),
        });
    };

    Ok(value.clamp(min_timeout_ms, max_timeout_ms))
}

pub(super) const fn json_value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub(super) fn parse_rid(raw: &str) -> Option<String> {
    serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| value.get("rid").and_then(Value::as_str).map(str::to_string))
}

#[cfg(test)]
mod tests;
