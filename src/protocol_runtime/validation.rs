use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, ProtocolRequest};
use serde_json::{json, Map, Value};

const GLOBAL_ALLOWED_REQUEST_ARGS: &[&str] = &["repo_id", "database_url", "connect_timeout_ms"];

pub(super) fn validate_request_args(
    request: &ProtocolRequest,
) -> std::result::Result<(), Box<ProtocolEnvelope>> {
    let Some(allowed_command_specific) = allowed_command_args(request.cmd.as_str()) else {
        return Ok(());
    };
    let unknown = request
        .args
        .keys()
        .filter(|key| {
            !allowed_command_specific.contains(&key.as_str())
                && !GLOBAL_ALLOWED_REQUEST_ARGS.contains(&key.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();

    if unknown.is_empty() {
        return Ok(());
    }

    let mut allowed = allowed_command_specific
        .iter()
        .map(|key| (*key).to_string())
        .collect::<Vec<_>>();
    allowed.extend(
        GLOBAL_ALLOWED_REQUEST_ARGS
            .iter()
            .map(|key| (*key).to_string()),
    );
    allowed.sort();
    allowed.dedup();

    Err(Box::new(
        ProtocolEnvelope::error(
            request.rid.clone(),
            code::INVALID.to_string(),
            format!(
                "Unknown field(s) for {}: {}",
                request.cmd,
                unknown.join(", ")
            ),
        )
        .with_fix("Remove unknown fields or use documented command arguments".to_string())
        .with_ctx(json!({"cmd": request.cmd, "unknown": unknown, "allowed": allowed})),
    ))
}

pub(super) fn validate_request_null_bytes(
    request: &ProtocolRequest,
) -> std::result::Result<(), Box<ProtocolEnvelope>> {
    if request.cmd.contains('\0') {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "Null byte is not allowed in cmd".to_string(),
            )
            .with_fix("Remove null bytes from request fields".to_string())
            .with_ctx(json!({"field": "cmd"})),
        ));
    }

    if let Some(field) = first_null_byte_field_in_map(&request.args, "") {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                format!("Null byte is not allowed in field {field}"),
            )
            .with_fix("Remove null bytes from request fields".to_string())
            .with_ctx(json!({"field": field})),
        ));
    }

    Ok(())
}

fn first_null_byte_field_in_map(map: &Map<String, Value>, prefix: &str) -> Option<String> {
    map.iter().find_map(|(key, value)| {
        let field = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        first_null_byte_field_in_value(value, &field)
    })
}

fn first_null_byte_field_in_value(value: &Value, field: &str) -> Option<String> {
    match value {
        Value::String(text) => {
            if text.contains('\0') {
                Some(field.to_string())
            } else {
                None
            }
        }
        Value::Array(items) => items.iter().enumerate().find_map(|(index, item)| {
            first_null_byte_field_in_value(item, &format!("{field}[{index}]"))
        }),
        Value::Object(object) => first_null_byte_field_in_map(object, field),
        Value::Null | Value::Bool(_) | Value::Number(_) => None,
    }
}

fn allowed_command_args(cmd: &str) -> Option<&'static [&'static str]> {
    match cmd {
        "?" | "help" => Some(&["short", "s"]),
        "state" | "history" => Some(&["limit"]),
        "doctor" | "status" | "resume" | "agents" => Some(&[]),
        "lock" => Some(&["resource", "agent", "ttl_ms", "dry"]),
        "unlock" => Some(&["resource", "agent", "dry"]),
        "broadcast" => Some(&["msg", "from", "dry"]),
        "monitor" => Some(&["view", "watch_ms"]),
        "register" => Some(&["count", "dry"]),
        "agent" | "run-once" | "smoke" => Some(&["id", "dry"]),
        "next" | "claim-next" | "bootstrap" => Some(&["dry"]),
        "assign" => Some(&["bead_id", "agent_id", "dry"]),
        "qa" => Some(&["target", "id", "dry"]),
        "resume-context" => Some(&["bead_id"]),
        "artifacts" => Some(&["bead_id", "artifact_type"]),
        "release" => Some(&["agent_id", "dry"]),
        "init-db" => Some(&["url", "schema", "seed_agents", "dry"]),
        "init-local-db" => Some(&[
            "container_name",
            "port",
            "user",
            "database",
            "schema",
            "seed_agents",
            "dry",
        ]),
        "spawn-prompts" => Some(&["template", "out_dir", "count", "dry"]),
        "prompt" => Some(&["id", "skill"]),
        "load-profile" => Some(&["agents", "rounds", "timeout_ms", "dry"]),
        "init" => Some(&["dry", "database_url", "schema", "seed_agents"]),
        "batch" => Some(&["ops", "cmds", "dry"]),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
