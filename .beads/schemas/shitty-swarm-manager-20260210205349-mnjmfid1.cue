
package validation

import "list"

// Validation schema for bead: shitty-swarm-manager-20260210205349-mnjmfid1
// Title: quality: Build BDD contract suite decoupled from internals
//
// This schema validates that implementation is complete.
// Use: cue vet shitty-swarm-manager-20260210205349-mnjmfid1.cue implementation.cue

#BeadImplementation: {
  bead_id: "shitty-swarm-manager-20260210205349-mnjmfid1"
  title: "quality: Build BDD contract suite decoupled from internals"

  // Contract verification
  contracts_verified: {
    preconditions_checked: bool & true
    postconditions_verified: bool & true
    invariants_maintained: bool & true

    // Specific preconditions that must be verified
    precondition_checks: [
      "Scenario fixtures can initialize and reset test database state",
    ]

    // Specific postconditions that must be verified
    postcondition_checks: [
      "Scenarios cover happy, failure-loop, and blocked terminal flows",
      "Assertions rely on protocol responses and read models",
    ]

    // Specific invariants that must be maintained
    invariant_checks: [
      "No BDD assertion inspects private function behavior",
      "Scenario setup is deterministic and isolated",
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
      "Given pending bead, when agent pipeline passes all stages, then progress shows completed bead and done agent",
      "Given completed bead, when querying monitor progress, then aggregate counts reflect completion",
    ]

    // Required error path tests
    required_error_tests: [
      "Given repeated QA failures, when retries exceed max, then bead becomes blocked and agent leaves working state",
      "Given invalid command payload, when protocol executes, then structured INVALID error envelope is returned",
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
//     timestamp: "2026-02-10T20:53:49Z"
//   }
// }