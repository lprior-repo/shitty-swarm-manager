#[cfg(test)]
mod protocol_handler_contract_tests {
    use assert_cmd::Command;
    use serde_json::Value;

    fn assert_protocol_envelope(output: &Value) -> Result<(), String> {
        match (output.get("ok"), output.get("t"), output.get("ms")) {
            (Some(_), Some(timestamp), Some(duration))
                if timestamp.is_number() && duration.is_number() =>
            {
                Ok(())
            }
            _ => Err(format!(
                "missing or invalid protocol envelope fields in response: {output}"
            )),
        }
    }

    fn parse_json_output(assert: &assert_cmd::assert::Assert) -> Result<Value, String> {
        let output = assert.get_output();
        let raw = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let first_line = raw.lines().next().unwrap_or(&raw);
        serde_json::from_str::<Value>(first_line)
            .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))
    }

    fn get_binary_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_BIN_EXE_swarm")).to_path_buf()
    }

    mod handle_lock_tests {
        use super::*;

        #[test]
        fn lock_with_empty_resource_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args([
                    "lock",
                    "--resource",
                    "",
                    "--agent",
                    "agent-1",
                    "--ttl-ms",
                    "30000",
                ])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["msg"].as_str().unwrap_or("").contains("empty"));

            Ok(())
        }

        #[test]
        fn lock_with_whitespace_only_resource_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args([
                    "lock",
                    "--resource",
                    "   ",
                    "--agent",
                    "agent-1",
                    "--ttl-ms",
                    "30000",
                ])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn lock_missing_ttl_ms_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["lock", "--resource", "res-1", "--agent", "agent-1"])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");

            Ok(())
        }

        #[test]
        fn lock_with_invalid_ttl_ms_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args([
                    "lock",
                    "--resource",
                    "res-1",
                    "--agent",
                    "agent-1",
                    "--ttl-ms",
                    "-5",
                ])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");

            Ok(())
        }
    }

    mod handle_monitor_tests {
        use super::*;

        #[test]
        fn monitor_active_view_returns_valid_envelope() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["monitor", "--view", "active", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["view"], "active");

            Ok(())
        }

        #[test]
        fn monitor_progress_view_returns_valid_envelope() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["monitor", "--view", "progress", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["view"], "progress");

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn monitor_failures_view_returns_valid_envelope() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["monitor", "--view", "failures", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["view"], "failures");

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn monitor_events_view_returns_valid_envelope() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["monitor", "--view", "events", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["view"], "events");

            Ok(())
        }

        #[test]
        fn monitor_messages_view_returns_valid_envelope() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["monitor", "--view", "messages", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["view"], "messages");

            Ok(())
        }

        #[test]
        fn monitor_unknown_view_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["monitor", "--view", "unknown_view"])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");

            Ok(())
        }

        #[test]
        fn monitor_default_view_is_active() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["monitor", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["view"], "active");

            Ok(())
        }
    }

    mod handle_init_tests {
        use super::*;

        #[test]
        fn init_dry_run_returns_steps() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["init", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["dry"], true);

            let would_do = data["would_do"]
                .as_array()
                .ok_or("expected would_do array")?;
            assert_eq!(would_do.len(), 3);

            assert_eq!(would_do[0]["action"], "bootstrap");
            assert_eq!(would_do[1]["action"], "init_db");
            assert_eq!(would_do[2]["action"], "register");

            Ok(())
        }

        #[test]
        fn init_with_custom_seed_agents() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["init", "--dry", "--seed-agents", "24"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            let data = output["d"].as_object().ok_or("expected data object")?;
            let would_do = data["would_do"]
                .as_array()
                .ok_or("expected would_do array")?;
            assert_eq!(would_do[2]["target"], 24);

            Ok(())
        }
    }

    mod handle_batch_tests {
        use super::*;

        #[test]
        fn batch_missing_ops_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary).args(["batch"]).assert().failure();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["msg"].as_str().unwrap_or("").contains("Missing"));

            Ok(())
        }

        #[test]
        fn batch_empty_ops_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let input = r#"{"cmd":"batch","ops":[]}"#;
            let assert = Command::new(binary)
                .write_stdin(format!("{input}\n"))
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["msg"].as_str().unwrap_or("").contains("empty"));

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn batch_with_cmds_alias_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let input = r#"{"cmd":"batch","cmds":[{"cmd":"doctor"}]}"#;
            let assert = Command::new(binary)
                .write_stdin(format!("{input}\n"))
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert!(err["fix"].as_str().unwrap_or("").contains("ops"));

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn batch_with_invalid_json_item_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let input = r#"{"cmd":"batch","ops":["not valid json"]}"#;
            let assert = Command::new(binary)
                .write_stdin(format!("{input}\n"))
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["msg"]
                .as_str()
                .unwrap_or("")
                .contains("Invalid batch item"));

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn batch_with_nested_batch_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let input = r#"{"cmd":"batch","ops":[{"cmd":"batch","ops":[{"cmd":"doctor"}]}]}"#;
            let assert = Command::new(binary)
                .write_stdin(format!("{input}\n"))
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["msg"].as_str().unwrap_or("").contains("Nested batch"));

            Ok(())
        }

        #[test]
        fn batch_dry_run_shows_would_do() -> Result<(), String> {
            let binary = get_binary_path();
            let input = r#"{"cmd":"batch","ops":[{"cmd":"doctor"},{"cmd":"status"}],"dry":true}"#;
            let assert = Command::new(binary)
                .write_stdin(format!("{input}\n"))
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["dry"], true);

            let would_do = data["would_do"]
                .as_array()
                .ok_or("expected would_do array")?;
            assert_eq!(would_do.len(), 2);
            assert_eq!(would_do[0]["action"], "execute");
            assert_eq!(would_do[0]["target"], "doctor");
            assert_eq!(would_do[1]["target"], "status");

            Ok(())
        }
    }

    mod handle_history_tests {
        use super::*;

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn history_with_limit_parameter() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["history", "--limit", "50", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn history_default_limit_is_applied() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["history", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["effective_limit"], 100);

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn history_excessive_limit_is_capped() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["history", "--limit", "50000", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["effective_limit"], 10000);

            Ok(())
        }

        #[test]
        fn history_negative_limit_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["history", "--limit", "-10"])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");

            Ok(())
        }
    }

    mod handle_state_tests {
        use super::*;

        #[test]
        fn state_dry_run_returns_valid_envelope() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["state", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            Ok(())
        }

        #[test]
        fn state_with_limit_parameter() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["state", "--dry", "--limit", "10"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            Ok(())
        }
    }

    mod handle_register_tests {
        use super::*;

        #[test]
        fn register_count_zero_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["register", "--count", "0"])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["msg"].as_str().unwrap_or("").contains("greater than 0"));

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn register_count_above_max_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["register", "--count", "200"])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["fix"].as_str().unwrap_or("").contains("100"));

            Ok(())
        }

        #[test]
        #[ignore = "requires database connection with matching schema"]
        fn register_negative_count_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["register", "--count", "-5"])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");

            Ok(())
        }

        #[test]
        fn register_dry_run_shows_would_do() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["register", "--count", "5", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["dry"], true);

            let would_do = data["would_do"]
                .as_array()
                .ok_or("expected would_do array")?;
            assert_eq!(would_do.len(), 2);
            assert_eq!(would_do[0]["action"], "register_repo");
            assert_eq!(would_do[1]["action"], "register_agents");
            assert_eq!(would_do[1]["target"], 5);

            Ok(())
        }
    }

    mod handle_broadcast_tests {
        use super::*;

        #[test]
        fn broadcast_empty_msg_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["broadcast", "--msg", "", "--from", "agent-1"])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["msg"].as_str().unwrap_or("").contains("msg"));

            Ok(())
        }

        #[test]
        fn broadcast_empty_from_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["broadcast", "--msg", "hello", "--from", ""])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["msg"].as_str().unwrap_or("").contains("from"));

            Ok(())
        }

        #[test]
        fn broadcast_whitespace_msg_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["broadcast", "--msg", "   ", "--from", "agent-1"])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");

            Ok(())
        }

        #[test]
        fn broadcast_dry_run_shows_would_do() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args([
                    "broadcast",
                    "--msg",
                    "hello world",
                    "--from",
                    "agent-1",
                    "--dry",
                ])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["dry"], true);

            let would_do = data["would_do"]
                .as_array()
                .ok_or("expected would_do array")?;
            assert_eq!(would_do.len(), 1);
            assert_eq!(would_do[0]["action"], "broadcast");
            assert_eq!(would_do[0]["target"], "hello world");

            Ok(())
        }
    }

    mod handle_resume_context_tests {
        use super::*;

        #[test]
        fn resume_context_empty_bead_filter() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["resume-context", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            Ok(())
        }

        #[test]
        fn resume_context_empty_string_bead_id_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["resume-context", "--bead-id", ""])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");
            assert!(err["msg"].as_str().unwrap_or("").contains("empty"));

            Ok(())
        }

        #[test]
        fn resume_context_whitespace_bead_id_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["resume-context", "--bead-id", "   "])
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");

            Ok(())
        }

        #[test]
        fn resume_context_non_string_bead_id_returns_error() -> Result<(), String> {
            let binary = get_binary_path();
            let input = r#"{"cmd":"resume-context","bead_id":123}"#;
            let assert = Command::new(binary)
                .write_stdin(format!("{input}\n"))
                .assert()
                .failure();

            let output = parse_json_output(&assert)?;
            let err = output["err"].as_object().ok_or("expected err object")?;
            assert_eq!(err["code"], "INVALID");

            Ok(())
        }
    }

    mod handle_init_local_db_tests {
        use super::*;

        #[test]
        fn init_local_db_dry_run_returns_steps() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["init-local-db", "--dry"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            assert_protocol_envelope(&output)?;

            let data = output["d"].as_object().ok_or("expected data object")?;
            assert_eq!(data["dry"], true);

            let would_do = data["would_do"]
                .as_array()
                .ok_or("expected would_do array")?;
            assert_eq!(would_do.len(), 2);
            assert_eq!(would_do[0]["action"], "docker_start_or_run");
            assert_eq!(would_do[1]["action"], "init_db");

            Ok(())
        }

        #[test]
        fn init_local_db_custom_container_name() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args([
                    "init-local-db",
                    "--dry",
                    "--container-name",
                    "custom-db-container",
                ])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            let data = output["d"].as_object().ok_or("expected data object")?;
            let would_do = data["would_do"]
                .as_array()
                .ok_or("expected would_do array")?;
            assert_eq!(would_do[0]["target"], "custom-db-container");

            Ok(())
        }

        #[test]
        fn init_local_db_custom_port() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["init-local-db", "--dry", "--port", "5433"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            Ok(())
        }

        #[test]
        fn init_local_db_custom_user_and_database() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args([
                    "init-local-db",
                    "--dry",
                    "--user",
                    "testuser",
                    "--database",
                    "testdb",
                ])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            Ok(())
        }

        #[test]
        fn init_local_db_with_seed_agents() -> Result<(), String> {
            let binary = get_binary_path();
            let assert = Command::new(binary)
                .args(["init-local-db", "--dry", "--seed-agents", "24"])
                .assert()
                .success();

            let output = parse_json_output(&assert)?;
            Ok(())
        }
    }
}
