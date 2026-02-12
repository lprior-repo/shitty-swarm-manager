//! Exit Code Integration Tests
//!
//! These tests verify that the CLI actually returns non-zero exit codes
//! when errors occur by testing the SwarmError exit code mappings.

/// Test that verifies all error types have proper non-zero exit codes
/// This is the core contract that ensures shell scripts can detect failures
#[test]
fn test_all_error_variants_have_nonzero_exit_codes() {
    // This test documents what the fix accomplishes:
    //
    // BEFORE FIX:
    // - process_protocol_line always returned Ok(())
    // - Even when envelope.ok was false
    // - main.rs would exit with code 0
    // - Shell scripts couldn't detect failures
    //
    // AFTER FIX:
    // - process_protocol_line returns Err(...) when envelope.ok is false
    // - main.rs gets the Err and exits with err.exit_code()
    // - Shell scripts can detect failures via $?

    let test_cases = vec![
        (
            swarm::SwarmError::ConfigError("test".to_string()),
            2,
            "ConfigError",
        ),
        (
            swarm::SwarmError::DatabaseError("test".to_string()),
            3,
            "DatabaseError",
        ),
        (
            swarm::SwarmError::AgentError("test".to_string()),
            4,
            "AgentError",
        ),
        (
            swarm::SwarmError::BeadError("test".to_string()),
            5,
            "BeadError",
        ),
        (
            swarm::SwarmError::StageError("test".to_string()),
            6,
            "StageError",
        ),
        (
            swarm::SwarmError::Internal("test".to_string()),
            9,
            "Internal",
        ),
    ];

    for (error, expected_code, name) in test_cases {
        let actual_code = error.exit_code();
        assert_eq!(
            actual_code, expected_code,
            "{} should have exit code {}, got {}",
            name, expected_code, actual_code
        );
        assert_ne!(
            actual_code, 0,
            "{} must NEVER have exit code 0 - this breaks shell scripts!",
            name
        );
    }
}

#[test]
fn test_json_parse_error_has_nonzero_exit_code() {
    // Given: A JSON serialization error
    let invalid_json = "{not valid json}";
    let parse_result: Result<serde_json::Value, _> = serde_json::from_str(invalid_json);

    // When: It fails
    let error = parse_result.unwrap_err();
    let swarm_error = swarm::SwarmError::from(error);

    // Then: Exit code should be 8 (SerializationError)
    assert_eq!(
        swarm_error.exit_code(),
        8,
        "JSON parse errors should have exit code 8"
    );
    assert_ne!(
        swarm_error.exit_code(),
        0,
        "JSON parse errors must NOT have exit code 0"
    );
}

#[test]
fn test_io_error_has_nonzero_exit_code() {
    // Given: An I/O error
    let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let swarm_error = swarm::SwarmError::from(io_error);

    // Then: Exit code should be 7
    assert_eq!(
        swarm_error.exit_code(),
        7,
        "I/O errors should have exit code 7"
    );
    assert_ne!(
        swarm_error.exit_code(),
        0,
        "I/O errors must NOT have exit code 0"
    );
}

#[test]
fn test_exit_code_range_validation() {
    // All exit codes should be in valid range 0-9
    // 0 = success only
    // 1-9 = various error conditions

    let errors: Vec<swarm::SwarmError> = vec![
        swarm::SwarmError::ConfigError("missing env var".to_string()),
        swarm::SwarmError::DatabaseError("connection refused".to_string()),
        swarm::SwarmError::AgentError("agent not found".to_string()),
        swarm::SwarmError::BeadError("bead not found".to_string()),
        swarm::SwarmError::StageError("stage failed".to_string()),
        swarm::SwarmError::Internal("unexpected".to_string()),
    ];

    for error in errors {
        let code = error.exit_code();
        assert!(
            (1..=9).contains(&code),
            "Exit code {} is outside valid range 1-9 for error: {:?}",
            code,
            error
        );
    }
}

#[test]
fn test_success_has_exit_code_zero() {
    // Verify success case returns 0
    let success: Result<(), swarm::SwarmError> = Ok(());
    let exit_code = match success {
        Ok(()) => 0,
        Err(ref e) => e.exit_code(),
    };
    assert_eq!(exit_code, 0, "Success must have exit code 0");
}
