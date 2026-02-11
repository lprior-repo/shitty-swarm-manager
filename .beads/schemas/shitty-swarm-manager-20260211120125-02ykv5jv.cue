
package validation

import "list"

// Validation schema for bead: shitty-swarm-manager-20260211120125-02ykv5jv
// Title: cli: Fix exit code always 0 even on errors
//
// This schema validates that implementation is complete.
// Use: cue vet shitty-swarm-manager-20260211120125-02ykv5jv.cue implementation.cue

#BeadImplementation: {
  bead_id: "shitty-swarm-manager-20260211120125-02ykv5jv"
  title: "cli: Fix exit code always 0 even on errors"

  // Contract verification
  contracts_verified: {
    preconditions_checked: bool & true
    postconditions_verified: bool & true
    invariants_maintained: bool & true

    // Specific preconditions that must be verified
    precondition_checks: [
      "CLI command is executed",
    ]

    // Specific postconditions that must be verified
    postcondition_checks: [
      "Process exits with non-zero code on error",
    ]

    // Specific invariants that must be maintained
    invariant_checks: [
      "Exit code 0 only means success",
    ]
  }

  // Test verification
  tests_passing: {
    all_tests_pass: bool & true

    happy_path_tests: [...string] & list.MinItems(2)
    error_path_tests: [...string] & list.MinItems(2)

    // Note: Actual test names provided by implementer, must include all required tests

    // Required happy path tests
    required_happy_tests: [
      "Successful command returns exit code 0",
      "Command with valid input succeeds",
    ]

    // Required error path tests
    required_error_tests: [
      "Command with invalid input returns non-zero exit code",
      "Command with missing args returns non-zero exit code",
    ]
  }

  // Code completion
  code_complete: {
    implementation_exists: string  // Path to implementation file
    tests_exist: string  // Path to test file
    ci_passing: bool & true
    no_unwrap_calls: bool & true  // Rust/functional constraint
    no_panics: bool & true  // Rust constraint
  }

  // Completion criteria
  completion: {
    all_sections_complete: bool & true
    documentation_updated: bool
    beads_closed: bool
    timestamp: string  // ISO8601 completion timestamp
  }
}

// Example implementation proof - create this file to validate completion:
//
// implementation.cue:
// package validation
//
// implementation: #BeadImplementation & {
//   contracts_verified: {
//     preconditions_checked: true
//     postconditions_verified: true
//     invariants_maintained: true
//     precondition_checks: [/* documented checks */]
//     postcondition_checks: [/* documented verifications */]
//     invariant_checks: [/* documented invariants */]
//   }
//   tests_passing: {
//     all_tests_pass: true
//     happy_path_tests: ["test_version_flag_works", "test_version_format", "test_exit_code_zero"]
//     error_path_tests: ["test_invalid_flag_errors", "test_no_flags_normal_behavior"]
//   }
//   code_complete: {
//     implementation_exists: "src/main.rs"
//     tests_exist: "tests/cli_test.rs"
//     ci_passing: true
//     no_unwrap_calls: true
//     no_panics: true
//   }
//   completion: {
//     all_sections_complete: true
//     documentation_updated: true
//     beads_closed: false
//     timestamp: "2026-02-11T12:01:25Z"
//   }
// }