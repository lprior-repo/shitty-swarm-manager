# Contract Specification: CLI Exit Code Contract

## Context
- **Feature**: CLI exit code propagation for error conditions
- **Bead**: swm-kyh - cli: Fix exit code always 0 even on errors
- **Domain terms**:
  - `exit_code`: Process exit status (0 = success, non-zero = error)
  - `SwarmError`: Error type with `exit_code()` method returning i32
  - `CliError`: CLI parsing errors
  - `ProtocolEnvelope`: JSON response wrapper with `ok` field

## Preconditions
- [ ] CLI is invoked with valid or invalid arguments
- [ ] Database may be available or unavailable
- [ ] Configuration may be valid or invalid
- [ ] The system may encounter runtime errors during command execution

## Postconditions
- [ ] Exit code 0 ONLY when operation completes successfully
- [ ] Exit code 1 for CLI parsing errors
- [ ] Exit code 2-9 for specific SwarmError variants (see Error Taxonomy)
- [ ] Error output is written to stderr
- [ ] JSON envelope with `"ok": false` for protocol errors
- [ ] Process terminates with correct exit status for shell scripting

## Invariants
- [ ] Exit code 0 means SUCCESS (never returned on error)
- [ ] All error conditions result in non-zero exit code
- [ ] Exit codes are consistent and predictable (documented mapping)
- [ ] Shell scripts can rely on exit codes for flow control
- [ ] No silent failures (all errors have visible exit codes)

## Error Taxonomy & Exit Code Mapping

| Exit Code | Error Variant | When It Occurs |
|-----------|--------------|----------------|
| 0 | Success | Operation completed successfully |
| 1 | CliError | CLI parsing error, unknown command, missing args |
| 2 | ConfigError | Invalid configuration, env vars, settings |
| 3 | DatabaseError/SqlxError | Database connection/query failures |
| 4 | AgentError | Agent lifecycle errors |
| 5 | BeadError | Bead not found, invalid bead ID |
| 6 | StageError | Stage execution failures |
| 7 | IoError | File system, I/O operations |
| 8 | SerializationError | JSON parsing/serialization errors |
| 9 | Internal | Unexpected internal failures |

## Contract Signatures

```rust
// Main entry point contract
fn main() -> ! // Never returns, always calls std::process::exit(code)

// Error trait contract
impl SwarmError {
    fn exit_code(&self) -> i32; // Returns mapped exit code
    fn code(&self) -> &'static str; // Returns error category code
}

// Protocol processing contract
async fn process_protocol_line(line: &str) -> Result<(), SwarmError>;
async fn run_protocol_loop() -> Result<(), SwarmError>;
```

## Current State Analysis

### Identified Issue
The codebase has proper `exit_code()` mapping on `SwarmError`, but there may be code paths that:
1. Catch errors but don't exit with proper code
2. Return Ok(()) even when operations failed
3. Swallow errors in async contexts

### Areas to Audit
- `protocol_runtime::process_protocol_line` - must propagate all errors
- `protocol_runtime::run_protocol_loop` - must propagate all errors  
- Any `match` statements that have incomplete error arms
- Any `.ok()` or `.unwrap_or()` that discards errors

## Non-goals
- [ ] Do NOT change error message formatting
- [ ] Do NOT add new error variants
- [ ] Do NOT modify exit code mappings
- [ ] Do NOT change JSON output schema
- [ ] Do NOT affect successful operation behavior
