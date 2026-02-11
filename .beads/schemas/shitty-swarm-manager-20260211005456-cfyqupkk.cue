
package validation

import "list"

// Validation schema for bead: shitty-swarm-manager-20260211005456-cfyqupkk
// Title: prompt-governance: unify canonical agent prompt source
//
// This schema validates that implementation is complete.
// Use: cue vet shitty-swarm-manager-20260211005456-cfyqupkk.cue implementation.cue

#BeadImplementation: {
  bead_id: "shitty-swarm-manager-20260211005456-cfyqupkk"
  title: "prompt-governance: unify canonical agent prompt source"

  // Contract verification
  contracts_verified: {
    preconditions_checked: bool & true
    postconditions_verified: bool & true
    invariants_maintained: bool & true

    // Specific preconditions that must be verified
    precondition_checks: [
      "Prompt generation commands are available in protocol runtime.",
    ]

    // Specific postconditions that must be verified
    postcondition_checks: [
      "Default spawn template and prompt command content are semantically aligned.",
      "Documentation identifies canonical prompt path.",
    ]

    // Specific invariants that must be maintained
    invariant_checks: [
      "Prompt placeholders remain stable (`{N}`/`#{N}`).",
      "Generated prompt files preserve machine-readable contract sections.",
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
      "Given default spawn-prompts when generated then prompt content matches canonical source sections.",
      "Given prompt command for an agent id then returned prompt contains expected canonical pipeline sections.",
    ]

    // Required error path tests
    required_error_tests: [
      "Given missing external template path then spawn-prompts returns NOTFOUND without fallback drift.",
      "Given invalid skill prompt request then command returns actionable NOTFOUND guidance.",
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