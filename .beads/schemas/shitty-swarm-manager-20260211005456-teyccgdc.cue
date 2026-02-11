
package validation

import "list"

// Validation schema for bead: shitty-swarm-manager-20260211005456-teyccgdc
// Title: diagnostics: emit valid retry next-command guidance
//
// This schema validates that implementation is complete.
// Use: cue vet shitty-swarm-manager-20260211005456-teyccgdc.cue implementation.cue

#BeadImplementation: {
  bead_id: "shitty-swarm-manager-20260211005456-teyccgdc"
  title: "diagnostics: emit valid retry next-command guidance"

  // Contract verification
  contracts_verified: {
    preconditions_checked: bool & true
    postconditions_verified: bool & true
    invariants_maintained: bool & true

    // Specific preconditions that must be verified
    precondition_checks: [
      "Retry diagnostics payload generation path is invoked on failure transitions.",
    ]

    // Specific postconditions that must be verified
    postcondition_checks: [
      "next_command values correspond to supported CLI commands.",
      "Failure envelopes and monitor views expose updated guidance consistently.",
    ]

    // Specific invariants that must be maintained
    invariant_checks: [
      "Diagnostics remain machine-readable and backward-compatible.",
      "No mutation behavior changes from guidance text updates.",
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
      "Given retry transition when diagnostics are produced then next_command references a valid supported command.",
      "Given monitor failures view when retry event exists then rendered next_command is executable.",
    ]

    // Required error path tests
    required_error_tests: [
      "Given malformed failure message when diagnostics are built then next_command still defaults to a valid safe command.",
      "Given unknown transition category when diagnostics are built then category defaults safely without panic.",
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
//     timestamp: "2026-02-11T00:54:56Z"
//   }
// }