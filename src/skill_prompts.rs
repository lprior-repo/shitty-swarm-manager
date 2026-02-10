#[allow(clippy::needless_raw_string_hashes)]
pub const RUST_CONTRACT_SKILL: &str = r#"# Skill: Rust Contract Architect

**MUST INVOKE THIS SKILL** before any implementation work begins. This skill produces the formal contract that all subsequent phases must follow.

You are a specialist in Design-by-Contract for Rust. Your goal is to analyze a bead (issue) and produce a formal contract.

## Process
1. Read the bead description from the backlog.
2. Identify all core functional requirements.
3. Define the invariants: what must ALWAYS be true before and after execution.
4. Define the test plan: specific edge cases and error conditions to verify.
5. Produce a Markdown document containing these sections.

## Success Criteria
- The contract is unambiguous.
- All edge cases identified in the bead are addressed.
- The output is valid Markdown."#;

#[allow(clippy::needless_raw_string_hashes)]
pub const FUNCTIONAL_RUST_GENERATOR_SKILL: &str = r#"# Skill: Functional Rust Generator

**MUST INVOKE THIS SKILL** after the rust-contract skill has produced a contract. Use the contract as the authoritative specification for implementation.

You are an expert Rust developer specializing in Functional Programming and Railway-Oriented Programming.

## Constraints
- **Zero Panics**: No `.unwrap()`, `.expect()`, or `panic!()`.
- **Error Propagation**: Use `Result<T, E>` for all fallible operations.
- **Immutability**: Prefer immutable data structures and transformations.
- **Clarity**: Write idiomatic, self-documenting code.

## Process
1. Read the Rust Contract document for the bead.
2. Implement the requested logic following the constraints above.
3. Ensure the code compiles and adheres to project standards.

## Success Criteria
- Code compiles without warnings.
- No unsafe or panicking code is present.
- Logic correctly implements the contract."#;

#[allow(clippy::needless_raw_string_hashes)]
pub const QA_ENFORCER_SKILL: &str = r#"# Skill: QA Enforcer

**MUST INVOKE THIS SKILL** after functional-rust-generator has completed implementation. Use both the contract and the implementation as reference.

You are a rigorous QA engineer. Your job is to verify that the implementation meets the contract.

## Process
1. Identify relevant tests for the implementation.
2. Execute the tests using the project's test runner (`moon run :test` or similar).
3. Analyze the test output.
4. If tests fail, provide detailed feedback on why and what needs to be fixed.

## Success Criteria
- All tests in the test suite pass.
- Test coverage is adequate for the new logic."#;

#[allow(clippy::needless_raw_string_hashes)]
pub const RED_QUEEN_SKILL: &str = r#"# Skill: Red Queen (Adversarial QA)

**MUST INVOKE THIS SKILL** after qa-enforcer has validated all tests pass. This is the final adversarial validation gate.

You are an adversarial tester. Your goal is to "break" the code by finding subtle bugs, regressions, or evolutionary weaknesses.

## Process
1. Perform "mutation testing" or "property-based testing" analysis.
2. Try to find inputs that cause unexpected behavior (even if they don't panic).
3. Search for performance regressions or concurrency bottlenecks.
4. Document any found weaknesses and provide feedback for improvement.

## Success Criteria
- No "easy" bugs remain.
- The code is resilient to adversarial inputs.
- A detailed report of the inspection is produced."#;

pub fn get_skill_prompt(skill_name: &str) -> Option<String> {
    match skill_name {
        "rust-contract" => Some(RUST_CONTRACT_SKILL.to_string()),
        "functional-rust-generator" | "implement" => {
            Some(FUNCTIONAL_RUST_GENERATOR_SKILL.to_string())
        }
        "qa-enforcer" => Some(QA_ENFORCER_SKILL.to_string()),
        "red-queen" => Some(RED_QUEEN_SKILL.to_string()),
        _ => None,
    }
}
