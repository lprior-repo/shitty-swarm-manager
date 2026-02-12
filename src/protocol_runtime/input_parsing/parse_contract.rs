use super::super::ProtocolRequest;
use serde_json::Value;

pub trait ParseInput {
    type Input;

    /// # Errors
    /// Returns parse errors for missing or invalid request fields.
    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("Missing required field: {field}")]
    MissingField { field: String },
    #[error("Invalid type for field {field}: expected {expected}, got {got}")]
    InvalidType {
        field: String,
        expected: String,
        got: String,
    },
    #[error("Invalid value for field {field}: {value}")]
    InvalidValue { field: String, value: String },
    #[error("Parse error: {0}")]
    Custom(String),
}

pub fn json_value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(number) if number.is_u64() => "u64",
        Value::Number(number) if number.is_i64() => "i64",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub fn parse_optional_non_negative_u64(
    request: &ProtocolRequest,
    field: &str,
) -> Result<Option<u64>, ParseError> {
    let Some(raw) = request.args.get(field) else {
        return Ok(None);
    };
    if raw.as_i64().is_some_and(|value| value < 0) {
        return Err(ParseError::InvalidValue {
            field: field.to_string(),
            value: "must be non-negative".to_string(),
        });
    }
    raw.as_u64()
        .map(Some)
        .ok_or_else(|| ParseError::InvalidType {
            field: field.to_string(),
            expected: "u64".to_string(),
            got: json_value_type_name(raw).to_string(),
        })
}

pub fn parse_optional_non_negative_i64(
    request: &ProtocolRequest,
    field: &str,
) -> Result<Option<i64>, ParseError> {
    let Some(raw) = request.args.get(field) else {
        return Ok(None);
    };
    let value = raw.as_i64().ok_or_else(|| ParseError::InvalidType {
        field: field.to_string(),
        expected: "i64".to_string(),
        got: json_value_type_name(raw).to_string(),
    })?;
    if value < 0 {
        return Err(ParseError::InvalidValue {
            field: field.to_string(),
            value: "must be non-negative".to_string(),
        });
    }
    Ok(Some(value))
}

pub fn parse_optional_non_negative_u32(
    request: &ProtocolRequest,
    field: &str,
) -> Result<Option<u32>, ParseError> {
    let Some(raw) = request.args.get(field) else {
        return Ok(None);
    };
    if raw.as_i64().is_some_and(|value| value < 0) {
        return Err(ParseError::InvalidValue {
            field: field.to_string(),
            value: "must be non-negative".to_string(),
        });
    }
    let as_u64 = raw.as_u64().ok_or_else(|| ParseError::InvalidType {
        field: field.to_string(),
        expected: "u32".to_string(),
        got: json_value_type_name(raw).to_string(),
    })?;
    u32::try_from(as_u64)
        .map(Some)
        .map_err(|_| ParseError::InvalidValue {
            field: field.to_string(),
            value: format!("{as_u64} exceeds max u32"),
        })
}
