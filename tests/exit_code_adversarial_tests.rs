//! Exit Code Adversarial Tests
//!
//! These tests verify that ALL error conditions result in non-zero exit codes.
//! They actively try to break the assumption that exit codes work correctly.
//!
//! Martin Fowler style: expressive names, Given-When-Then structure,
//! comprehensive coverage of happy path, error path, and edge cases.

use swarm::{code, SwarmError};

// ============================================================================
// HAPPY PATH TESTS
// ============================================================================

#[test]
fn test_returns_exit_code_zero_for_success() {
    // Given: A successful result
    let result: Result<(), SwarmError> = Ok(());

    // When: We check what exit code would be used
    let exit_code = match result {
        Ok(()) => 0,
        Err(ref e) => e.exit_code(),
    };

    // Then: Exit code should be 0
    assert_eq!(exit_code, 0, "Success should return exit code 0");
}

// ============================================================================
// ERROR PATH TESTS - CLI ERRORS (Exit Code 1)
// ============================================================================

#[test]
fn test_config_error_returns_exit_code_two() {
    // Given: A configuration error
    let error = SwarmError::ConfigError("missing DATABASE_URL".to_string());

    // When: We get the exit code
    let exit_code = error.exit_code();

    // Then: Exit code should be 2
    assert_eq!(exit_code, 2, "ConfigError should return exit code 2");
}

// ============================================================================
// ERROR PATH TESTS - DATABASE ERRORS (Exit Code 3)
// ============================================================================

#[test]
fn test_database_error_returns_exit_code_three() {
    // Given: A database error
    let error = SwarmError::DatabaseError("connection refused".to_string());

    // When: We get the exit code
    let exit_code = error.exit_code();

    // Then: Exit code should be 3
    assert_eq!(exit_code, 3, "DatabaseError should return exit code 3");
}

#[test]
fn test_sqlx_error_returns_exit_code_three() {
    // Given: A SQLx error (simulated)
    let io_error = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "db down");
    let sqlx_error = sqlx::Error::Io(io_error);
    let error = SwarmError::from(sqlx_error);

    // When: We get the exit code
    let exit_code = error.exit_code();

    // Then: Exit code should be 3
    assert_eq!(exit_code, 3, "SqlxError should return exit code 3");
}

// ============================================================================
// ERROR PATH TESTS - AGENT ERRORS (Exit Code 4)
// ============================================================================

#[test]
fn test_agent_error_returns_exit_code_four() {
    // Given: An agent error
    let error = SwarmError::AgentError("agent not found".to_string());

    // When: We get the exit code
    let exit_code = error.exit_code();

    // Then: Exit code should be 4
    assert_eq!(exit_code, 4, "AgentError should return exit code 4");
}

// ============================================================================
// ERROR PATH TESTS - BEAD ERRORS (Exit Code 5)
// ============================================================================

#[test]
fn test_bead_error_returns_exit_code_five() {
    // Given: A bead error
    let error = SwarmError::BeadError("bead not found".to_string());

    // When: We get the exit code
    let exit_code = error.exit_code();

    // Then: Exit code should be 5
    assert_eq!(exit_code, 5, "BeadError should return exit code 5");
}

// ============================================================================
// ERROR PATH TESTS - STAGE ERRORS (Exit Code 6)
// ============================================================================

#[test]
fn test_stage_error_returns_exit_code_six() {
    // Given: A stage error
    let error = SwarmError::StageError("stage execution failed".to_string());

    // When: We get the exit code
    let exit_code = error.exit_code();

    // Then: Exit code should be 6
    assert_eq!(exit_code, 6, "StageError should return exit code 6");
}

// ============================================================================
// ERROR PATH TESTS - I/O ERRORS (Exit Code 7)
// ============================================================================

#[test]
fn test_io_error_returns_exit_code_seven() {
    // Given: An I/O error
    let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let error = SwarmError::from(io_error);

    // When: We get the exit code
    let exit_code = error.exit_code();

    // Then: Exit code should be 7
    assert_eq!(exit_code, 7, "IoError should return exit code 7");
}

// ============================================================================
// ERROR PATH TESTS - SERIALIZATION ERRORS (Exit Code 8)
// ============================================================================

#[test]
fn test_serialization_error_returns_exit_code_eight() {
    // Given: A serialization error
    let json_error = serde_json::from_str::<serde_json::Value>("{invalid json}").unwrap_err();
    let error = SwarmError::from(json_error);

    // When: We get the exit code
    let exit_code = error.exit_code();

    // Then: Exit code should be 8
    assert_eq!(exit_code, 8, "SerializationError should return exit code 8");
}

// ============================================================================
// ERROR PATH TESTS - INTERNAL ERRORS (Exit Code 9)
// ============================================================================

#[test]
fn test_internal_error_returns_exit_code_nine() {
    // Given: An internal error
    let error = SwarmError::Internal("unexpected condition".to_string());

    // When: We get the exit code
    let exit_code = error.exit_code();

    // Then: Exit code should be 9
    assert_eq!(exit_code, 9, "Internal error should return exit code 9");
}

// ============================================================================
// CONTRACT VERIFICATION TESTS - No Exit Code 0 on Error
// ============================================================================

#[test]
fn test_no_error_variant_returns_exit_code_zero() {
    // Given: All SwarmError variants
    let errors = vec![
        SwarmError::ConfigError("test".to_string()),
        SwarmError::DatabaseError("test".to_string()),
        SwarmError::AgentError("test".to_string()),
        SwarmError::BeadError("test".to_string()),
        SwarmError::StageError("test".to_string()),
        SwarmError::Internal("test".to_string()),
    ];

    // When/Then: None should return exit code 0
    for error in errors {
        let exit_code = error.exit_code();
        assert_ne!(
            exit_code, 0,
            "Error {:?} returned exit code 0 - this breaks shell scripts!",
            error
        );
    }
}

// ============================================================================
// CONTRACT VERIFICATION TESTS - Error Code Mapping
// ============================================================================

#[test]
fn test_error_code_mapping_matches_exit_code_semantics() {
    // Given: Error variants and their expected category codes
    let test_cases = vec![
        (SwarmError::ConfigError("test".to_string()), code::INVALID),
        (
            SwarmError::DatabaseError("test".to_string()),
            code::INTERNAL,
        ),
        (SwarmError::AgentError("test".to_string()), code::CONFLICT),
        (SwarmError::BeadError("test".to_string()), code::NOTFOUND),
        (SwarmError::StageError("test".to_string()), code::CONFLICT),
        (SwarmError::Internal("test".to_string()), code::INTERNAL),
    ];

    // When/Then: Each error's code() matches expected category
    for (error, expected_code) in test_cases {
        let actual_code = error.code();
        assert_eq!(
            actual_code, expected_code,
            "Error {:?} has code '{}' but expected '{}'",
            error, actual_code, expected_code
        );
    }
}

// ============================================================================
// ADVERSARIAL REGRESSION TESTS
// ============================================================================

#[test]
fn test_exit_code_consistency_for_same_error_type() {
    // Given: Multiple errors of the same type
    let error1 = SwarmError::DatabaseError("test1".to_string());
    let error2 = SwarmError::DatabaseError("test2".to_string());

    // When: We compare exit codes
    let code1 = error1.exit_code();
    let code2 = error2.exit_code();

    // Then: They should be identical (same error type = same exit code)
    assert_eq!(
        code1, code2,
        "Exit code changed for same error type with different messages - this is a bug!"
    );
}

#[test]
fn test_io_error_variants_all_map_to_seven() {
    // Given: Different I/O error kinds
    let io_errors = vec![
        std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
        std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied"),
        std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused"),
        std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out"),
        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected eof"),
    ];

    // When/Then: All should map to exit code 7
    for io_err in io_errors {
        let error = SwarmError::from(io_err);
        assert_eq!(
            error.exit_code(),
            7,
            "IoError kind {:?} did not map to exit code 7",
            error
        );
    }
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[test]
fn test_empty_error_messages_still_return_nonzero_exit() {
    // Given: Errors with empty messages
    let errors = vec![
        SwarmError::ConfigError("".to_string()),
        SwarmError::DatabaseError("".to_string()),
        SwarmError::Internal("".to_string()),
    ];

    // When/Then: All should still return non-zero
    for error in errors {
        assert_ne!(
            error.exit_code(),
            0,
            "Error with empty message returned exit code 0"
        );
    }
}

#[test]
fn test_very_long_error_messages_preserve_exit_code() {
    // Given: Error with very long message
    let long_message = "x".repeat(10000);
    let error = SwarmError::ConfigError(long_message);

    // When: We get exit code
    let exit_code = error.exit_code();

    // Then: Should still be correct
    assert_eq!(exit_code, 2, "Long error message affected exit code");
}

#[test]
fn test_unicode_in_error_messages_preserve_exit_code() {
    // Given: Error with unicode message
    let error = SwarmError::ConfigError("错误：测试".to_string());

    // When: We get exit code
    let exit_code = error.exit_code();

    // Then: Should still be correct
    assert_eq!(exit_code, 2, "Unicode in error message affected exit code");
}

// ============================================================================
// INTEGRATION-STYLE TESTS - Error Propagation
// ============================================================================

#[test]
fn test_error_display_includes_message() {
    // Given: An error with a specific message
    let error = SwarmError::ConfigError("missing variable".to_string());

    // When: We display it
    let display = format!("{}", error);

    // Then: Message should be present
    assert!(
        display.contains("missing variable"),
        "Error display should include the message"
    );
}

#[test]
fn test_all_error_codes_are_documented() {
    // Given: All error codes in the ERROR_CODES array
    let documented_codes: Vec<&str> = swarm::ERROR_CODES
        .iter()
        .map(|(code, _, _)| *code)
        .collect();

    // When/Then: All variants should have a corresponding documented code
    assert!(
        documented_codes.contains(&code::INVALID),
        "INVALID code should be documented"
    );
    assert!(
        documented_codes.contains(&code::INTERNAL),
        "INTERNAL code should be documented"
    );
    assert!(
        documented_codes.contains(&code::CONFLICT),
        "CONFLICT code should be documented"
    );
    assert!(
        documented_codes.contains(&code::NOTFOUND),
        "NOTFOUND code should be documented"
    );
    assert!(
        documented_codes.contains(&code::DEPENDENCY),
        "DEPENDENCY code should be documented"
    );
}
