use crate::cli::OutputFormat;
use serde_json::json;
use swarm::SwarmError;

pub fn emit_output(output: &OutputFormat, command: &str, payload: serde_json::Value) {
    match output {
        OutputFormat::Text => payload
            .get("message")
            .and_then(serde_json::Value::as_str)
            .map_or_else(|| println!("{}", payload), |msg| println!("{}", msg)),
        OutputFormat::Json => println!(
            "{}",
            json!({
                "command": command,
                "status": "ok",
                "payload": payload,
            })
        ),
    }
}

pub fn map_error_to_exit_code(error: &SwarmError) -> i32 {
    match error {
        SwarmError::ConfigError(_) => 2,
        SwarmError::DatabaseError(_) => 3,
        SwarmError::AgentError(_) => 4,
        SwarmError::BeadError(_) => 5,
        SwarmError::StageError(_) => 6,
        SwarmError::IoError(_) => 7,
        SwarmError::SerializationError(_) => 8,
    }
}

#[cfg(test)]
mod tests {
    use super::map_error_to_exit_code;
    use swarm::SwarmError;

    #[test]
    fn exit_code_mapping_is_stable() {
        assert_eq!(
            map_error_to_exit_code(&SwarmError::ConfigError("x".to_string())),
            2
        );
        assert_eq!(
            map_error_to_exit_code(&SwarmError::DatabaseError("x".to_string())),
            3
        );
        assert_eq!(
            map_error_to_exit_code(&SwarmError::AgentError("x".to_string())),
            4
        );
        assert_eq!(
            map_error_to_exit_code(&SwarmError::BeadError("x".to_string())),
            5
        );
        assert_eq!(
            map_error_to_exit_code(&SwarmError::StageError("x".to_string())),
            6
        );
        let serde_err = serde_json::from_str::<serde_json::Value>("]")
            .err()
            .map_or_else(
                || serde_json::Error::io(std::io::Error::other("missing")),
                |e| e,
            );
        assert_eq!(
            map_error_to_exit_code(&SwarmError::SerializationError(serde_err)),
            8
        );
    }
}
