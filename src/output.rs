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

pub fn emit_error(output: &OutputFormat, error: &SwarmError) {
    let exit_code = map_error_to_exit_code(error);
    let (kind, hint) = error_kind_and_hint(error);

    match output {
        OutputFormat::Text => {
            eprintln!("error [{}]: {}", kind, error);
            eprintln!("hint: {}", hint);
        }
        OutputFormat::Json => {
            eprintln!(
                "{}",
                json!({
                    "status": "error",
                    "error": {
                        "kind": kind,
                        "message": error.to_string(),
                        "hint": hint,
                        "exit_code": exit_code,
                    }
                })
            );
        }
    }
}

fn error_kind_and_hint(error: &SwarmError) -> (&'static str, &'static str) {
    match error {
        SwarmError::ConfigError(_) => (
            "config_error",
            "Check CLI flags, config path, and required local files.",
        ),
        SwarmError::DatabaseError(_) => (
            "database_error",
            "Verify DATABASE_URL and confirm Postgres is reachable.",
        ),
        SwarmError::AgentError(_) => (
            "agent_error",
            "Inspect agent state and message queue for the current bead.",
        ),
        SwarmError::BeadError(_) => (
            "bead_error",
            "Confirm bead ID exists and bead status allows this action.",
        ),
        SwarmError::StageError(_) => (
            "stage_error",
            "Check stage artifacts and prior stage outputs for this bead.",
        ),
        SwarmError::IoError(_) => (
            "io_error",
            "Verify filesystem permissions and external command availability.",
        ),
        SwarmError::SerializationError(_) => (
            "serialization_error",
            "Review payload format and schema expectations.",
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
    use super::{error_kind_and_hint, map_error_to_exit_code};
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

    #[test]
    fn error_kind_and_hint_mapping_is_stable() {
        let (kind, hint) = error_kind_and_hint(&SwarmError::ConfigError("x".to_string()));
        assert_eq!(kind, "config_error");
        assert!(hint.contains("config"));
    }
}
