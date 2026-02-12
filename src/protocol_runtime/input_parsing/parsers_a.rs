use super::super::{ProtocolRequest, MAX_REGISTER_COUNT};
use super::parse_contract::{
    json_value_type_name, parse_optional_non_negative_u32, parse_optional_non_negative_u64,
    ParseError, ParseInput,
};
use serde_json::Value;

impl ParseInput for crate::DoctorInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {
            json: request.args.get("json").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::HelpInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {
            short: request.args.get("short").and_then(Value::as_bool),
            s: request.args.get("s").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::StatusInput {
    type Input = Self;

    fn parse_input(_request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {})
    }
}

impl ParseInput for crate::AgentInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let id_raw = request
            .args
            .get("id")
            .ok_or_else(|| ParseError::MissingField {
                field: "id".to_string(),
            })?;

        let id_as_u64 = id_raw.as_u64().ok_or_else(|| ParseError::InvalidType {
            field: "id".to_string(),
            expected: "u32".to_string(),
            got: json_value_type_name(id_raw).to_string(),
        })?;

        let id = u32::try_from(id_as_u64).map_err(|_| ParseError::InvalidValue {
            field: "id".to_string(),
            value: format!("{id_as_u64} exceeds max u32"),
        })?;

        if id == 0 {
            return Err(ParseError::InvalidValue {
                field: "id".to_string(),
                value: "must be greater than 0".to_string(),
            });
        }

        Ok(Self {
            id,
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::InitInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {
            dry: request.args.get("dry").and_then(Value::as_bool),
            database_url: request
                .args
                .get("database_url")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            schema: request
                .args
                .get("schema")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            seed_agents: request
                .args
                .get("seed_agents")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
        })
    }
}

impl ParseInput for crate::RegisterInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let count = match request.args.get("count") {
            None => None,
            Some(raw) => {
                if raw.as_i64().is_some_and(|value| value <= 0) {
                    return Err(ParseError::InvalidValue {
                        field: "count".to_string(),
                        value: "must be greater than 0".to_string(),
                    });
                }

                let count_as_u64 = raw.as_u64().ok_or_else(|| ParseError::InvalidType {
                    field: "count".to_string(),
                    expected: "u32".to_string(),
                    got: json_value_type_name(raw).to_string(),
                })?;

                let count = u32::try_from(count_as_u64).map_err(|_| ParseError::InvalidValue {
                    field: "count".to_string(),
                    value: format!("{count_as_u64} exceeds max u32"),
                })?;

                if count == 0 {
                    return Err(ParseError::InvalidValue {
                        field: "count".to_string(),
                        value: "must be greater than 0".to_string(),
                    });
                }

                if count > MAX_REGISTER_COUNT {
                    return Err(ParseError::InvalidValue {
                        field: "count".to_string(),
                        value: format!("must be less than or equal to {MAX_REGISTER_COUNT}"),
                    });
                }

                Some(count)
            }
        };

        Ok(Self {
            count,
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::ReleaseInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let agent_id = request
            .args
            .get("agent_id")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| ParseError::MissingField {
                field: "agent_id".to_string(),
            })?;

        Ok(Self {
            agent_id,
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::MonitorInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let watch_ms = parse_optional_non_negative_u64(request, "watch_ms")?;

        Ok(Self {
            view: request
                .args
                .get("view")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            watch_ms,
        })
    }
}

impl ParseInput for crate::InitDbInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let seed_agents = parse_optional_non_negative_u32(request, "seed_agents")?;

        Ok(Self {
            url: request
                .args
                .get("url")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            schema: request
                .args
                .get("schema")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            seed_agents,
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::InitLocalDbInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {
            container_name: request
                .args
                .get("container_name")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            port: request
                .args
                .get("port")
                .and_then(Value::as_u64)
                .map(|value| {
                    u16::try_from(value).map_err(|_| ParseError::InvalidValue {
                        field: "port".to_string(),
                        value: format!("{value} exceeds max u16"),
                    })
                })
                .transpose()?,
            user: request
                .args
                .get("user")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            database: request
                .args
                .get("database")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            schema: request
                .args
                .get("schema")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            seed_agents: request
                .args
                .get("seed_agents")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}
