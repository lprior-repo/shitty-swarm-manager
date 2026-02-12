use super::super::ProtocolRequest;
use super::parse_contract::{
    json_value_type_name, parse_optional_non_negative_i64, ParseError, ParseInput,
};
use serde_json::Value;

impl ParseInput for crate::BootstrapInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::SpawnPromptsInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {
            template: request
                .args
                .get("template")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            out_dir: request
                .args
                .get("out_dir")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
            count: request
                .args
                .get("count")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::PromptInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let id = match request.args.get("id") {
            None => 1,
            Some(raw) => {
                if raw.as_i64().is_some_and(|value| value <= 0) {
                    return Err(ParseError::InvalidValue {
                        field: "id".to_string(),
                        value: "must be greater than 0".to_string(),
                    });
                }

                let id_as_u64 = raw.as_u64().ok_or_else(|| ParseError::InvalidType {
                    field: "id".to_string(),
                    expected: "u32".to_string(),
                    got: json_value_type_name(raw).to_string(),
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

                id
            }
        };

        Ok(Self {
            id,
            skill: request
                .args
                .get("skill")
                .and_then(Value::as_str)
                .map(std::string::ToString::to_string),
        })
    }
}

impl ParseInput for crate::SmokeInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {
            id: request
                .args
                .get("id")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(1),
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::BatchInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let ops = request
            .args
            .get("ops")
            .and_then(Value::as_array)
            .ok_or_else(|| ParseError::MissingField {
                field: "ops".to_string(),
            })?
            .clone();

        Ok(Self {
            ops,
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::StateInput {
    type Input = Self;

    fn parse_input(_request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {})
    }
}

impl ParseInput for crate::HistoryInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let limit = parse_optional_non_negative_i64(request, "limit")?;
        Ok(Self { limit })
    }
}

impl ParseInput for crate::LockInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let resource = request
            .args
            .get("resource")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::MissingField {
                field: "resource".to_string(),
            })?
            .to_string();

        let agent = request
            .args
            .get("agent")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::MissingField {
                field: "agent".to_string(),
            })?
            .to_string();

        let ttl_ms = request
            .args
            .get("ttl_ms")
            .and_then(Value::as_i64)
            .ok_or_else(|| ParseError::MissingField {
                field: "ttl_ms".to_string(),
            })?;

        Ok(Self {
            resource,
            agent,
            ttl_ms,
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::UnlockInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let resource = request
            .args
            .get("resource")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::MissingField {
                field: "resource".to_string(),
            })?
            .to_string();

        let agent = request
            .args
            .get("agent")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::MissingField {
                field: "agent".to_string(),
            })?
            .to_string();

        Ok(Self {
            resource,
            agent,
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::AgentsInput {
    type Input = Self;

    fn parse_input(_request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {})
    }
}

impl ParseInput for crate::BroadcastInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        let msg = request
            .args
            .get("msg")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::MissingField {
                field: "msg".to_string(),
            })?
            .to_string();

        let from = request
            .args
            .get("from")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::MissingField {
                field: "from".to_string(),
            })?
            .to_string();

        Ok(Self {
            msg,
            from,
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}

impl ParseInput for crate::LoadProfileInput {
    type Input = Self;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        Ok(Self {
            agents: request
                .args
                .get("agents")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            rounds: request
                .args
                .get("rounds")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            timeout_ms: request.args.get("timeout_ms").and_then(Value::as_u64),
            dry: request.args.get("dry").and_then(Value::as_bool),
        })
    }
}
