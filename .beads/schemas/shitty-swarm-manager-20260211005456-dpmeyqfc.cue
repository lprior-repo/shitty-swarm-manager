
package validation

import "list"

// Validation schema for bead: shitty-swarm-manager-20260211005456-dpmeyqfc
// Title: resume-context: add deep resume context command
//
// This schema validates that implementation is complete.
// Use: cue vet shitty-swarm-manager-20260211005456-dpmeyqfc.cue implementation.cue

#BeadImplementation: {
  bead_id: "shitty-swarm-manager-20260211005456-dpmeyqfc"
  title: "resume-context: add deep resume context command"

  // Contract verification
  contracts_verified: {
    preconditions_checked: bool & true
    postconditions_verified: bool & true
    invariants_maintained: bool & true

    // Specific preconditions that must be verified
    precondition_checks: [
      "A bead exists with stage history and at least one persisted artifact.",
    ]

    // Specific postconditions that must be verified
    postcondition_checks: [
      "Protocol response includes full context required to continue without filesystem memory.",
      "Response remains machine-readable and stable across retries.",
    ]

    // Specific invariants that must be maintained
    invariant_checks: [
      "Resume payload ordering is deterministic by attempt and timestamp.",
      "No private/internal-only fields are required by consumers.",
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
      "Given a working bead with artifacts when resume-context is requested then full context payload includes attempt timeline and artifact bodies.",
      "Given a waiting bead with feedback when resume-context is requested then latest failure diagnostics are included.",
    ]

    // Required error path tests
    required_error_tests: [
      "Given unknown bead id when resume-context is requested then response returns NOTFOUND with actionable fix.",
      "Given malformed request when resume-context is requested then response returns INVALID without mutating state.",
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