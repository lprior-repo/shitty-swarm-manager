# Martin Fowler Test Plan: CLI Exit Codes (Adversarial)

## Overview
These tests verify that ALL error conditions result in non-zero exit codes. They are designed to be adversarial - actively trying to break the assumption that exit codes work correctly.

## Happy Path Tests

### test_returns_exit_code_zero_on_success
**Given**: A valid command that succeeds (e.g., `swarm --help`)  
**When**: The command completes successfully  
**Then**: Exit code is 0

### test_returns_exit_code_zero_on_protocol_success
**Given**: A valid protocol command with proper inputs  
**When**: Protocol execution succeeds  
**Then**: Exit code is 0 and JSON envelope has `"ok": true`

## Error Path Tests (Adversarial)

### CLI Parsing Errors (Exit Code 1)

#### test_returns_exit_code_one_on_unknown_command
**Given**: An unknown command like `swarm foobar`  
**When**: CLI attempts to parse  
**Then**: Exit code is 1, stderr contains error message

#### test_returns_exit_code_one_on_missing_required_argument
**Given**: Command missing required arg like `swarm agent` (no --id)  
**When**: CLI validates arguments  
**Then**: Exit code is 1

#### test_returns_exit_code_one_on_invalid_argument_type
**Given**: Command with wrong type like `swarm agent --id abc`  
**When**: CLI parses --id as u32  
**Then**: Exit code is 1

#### test_returns_exit_code_one_on_malformed_json_input
**Given**: Invalid JSON piped to protocol mode  
**When**: Protocol attempts to parse  
**Then**: Exit code is 8 (SerializationError)

### Database Errors (Exit Code 3)

#### test_returns_exit_code_three_on_database_connection_failure
**Given**: Invalid DATABASE_URL or unreachable database  
**When**: Command requires database access  
**Then**: Exit code is 3

#### test_returns_exit_code_three_on_database_query_failure
**Given**: Database connected but query fails  
**When**: SQL execution fails  
**Then**: Exit code is 3

### Configuration Errors (Exit Code 2)

#### test_returns_exit_code_two_on_missing_database_url
**Given**: No DATABASE_URL env var set  
**When**: Command requires database  
**Then**: Exit code is 2

#### test_returns_exit_code_two_on_invalid_config_file
**Given**: Corrupted or invalid config  
**When**: Config is loaded  
**Then**: Exit code is 2

### Bead Errors (Exit Code 5)

#### test_returns_exit_code_five_on_nonexistent_bead
**Given**: Request for bead ID that doesn't exist  
**When**: Bead lookup fails  
**Then**: Exit code is 5

#### test_returns_exit_code_five_on_invalid_bead_id_format
**Given**: Malformed bead ID  
**When**: Bead ID validation fails  
**Then**: Exit code is 5

### Agent Errors (Exit Code 4)

#### test_returns_exit_code_four_on_agent_operation_failure
**Given**: Agent operation that cannot complete  
**When**: Agent lifecycle operation fails  
**Then**: Exit code is 4

### Stage Errors (Exit Code 6)

#### test_returns_exit_code_six_on_stage_execution_failure
**Given**: Stage that fails to execute  
**When**: Stage runner encounters error  
**Then**: Exit code is 6

### I/O Errors (Exit Code 7)

#### test_returns_exit_code_seven_on_file_not_found
**Given**: Command references nonexistent file  
**When**: File operation fails  
**Then**: Exit code is 7

#### test_returns_exit_code_seven_on_permission_denied
**Given**: File without read permissions  
**When**: File read attempted  
**Then**: Exit code is 7

### Serialization Errors (Exit Code 8)

#### test_returns_exit_code_eight_on_invalid_json_protocol_input
**Given**: Malformed JSON input to protocol  
**When**: JSON parsing fails  
**Then**: Exit code is 8

### Internal Errors (Exit Code 9)

#### test_returns_exit_code_nine_on_internal_failure
**Given**: Unexpected condition triggering Internal error  
**When**: Internal invariant violated  
**Then**: Exit code is 9

## Edge Case Tests (Boundary Exploration)

### test_exit_code_not_swallowed_by_async_context
**Given**: Error occurring inside async block  
**When**: Async runtime propagates error  
**Then**: Exit code is correct, not default 0

### test_exit_code_not_swallowed_by_map_or
**Given**: Error handled with `.map_or(0, |e| e.exit_code())`  
**When**: Error path taken  
**Then**: Exit code comes from error, not default

### test_exit_code_propagates_through_multiple_error_conversions
**Given**: Error converted through multiple From implementations  
**When**: Final error reported  
**Then**: Original exit code semantics preserved

### test_exit_code_on_empty_stdin_protocol_mode
**Given**: EOF immediately when expecting JSON input  
**When**: Protocol reads empty input  
**Then**: Graceful handling with appropriate exit code

### test_exit_code_on_sigint_during_operation
**Given**: Long-running operation  
**When**: SIGINT received  
**Then**: Clean shutdown with non-zero exit code

## Contract Verification Tests

### test_precondition_exit_code_zero_only_for_success
**Verify**: No code path returns 0 on error

### test_postcondition_all_errors_have_nonzero_exit
**Verify**: Every SwarmError variant maps to non-zero exit code

### test_invariant_exit_code_consistency
**Verify**: Same error condition always produces same exit code

### test_invariant_exit_code_documentation_match
**Verify**: Actual exit codes match documentation in ERROR_CODES

## Given-When-Then Scenarios

### Scenario 1: Shell script relies on exit code
**Given**: A bash script running `swarm status`  
**When**: Database is down  
**Then**:
- Exit code is 3 (not 0)
- Script can detect failure via `$?`
- Script can take corrective action

### Scenario 2: CI pipeline error detection
**Given**: CI running `swarm doctor` as health check  
**When**: Environment is misconfigured  
**Then**:
- Exit code is 2 (ConfigError)
- CI marks build as failed
- Pipeline stops (doesn't continue blindly)

### Scenario 3: Batch operation with partial failure
**Given**: Batch command with multiple operations  
**When**: One operation fails  
**Then**:
- Overall exit code is non-zero
- Failed operation is identified in output
- Caller knows batch didn't fully succeed

### Scenario 4: Protocol mode error propagation
**Given**: JSON input to protocol mode  
**When**: Command execution fails  
**Then**:
- JSON envelope has `"ok": false`
- Process exits with non-zero code
- Both machine (JSON) and human (exit code) can detect error

## Adversarial Regression Tests

### Test: Exit codes must never be zero on error (100 iterations)
```rust
// For each error variant, verify exit_code() != 0
for error in all_error_variants() {
    assert_ne!(error.exit_code(), 0, 
        "Error {:?} has exit code 0 - this breaks shell scripts!", error);
}
```

### Test: Exit code consistency across error conversions
```rust
// Verify that error conversions preserve exit code semantics
let db_error = SwarmError::DatabaseError("test".to_string());
let converted: SwarmError = db_error.into(); // Through any conversion chain
assert_eq!(db_error.exit_code(), converted.exit_code());
```

### Test: Shell integration - set -e compatibility
```bash
#!/bin/bash
set -e
swarm invalid-command  # Should trigger set -e exit
# If we reach here, exit code was 0 (BUG!)
```

### Test: Make command fails properly
```makefile
test:
	swarm doctor || echo "Detected failure"  # || should execute on error
```

### Test: CI pipeline integration
```yaml
# .github/workflows/test.yml
- run: swarm status
- run: echo "This should not run if status failed"
```

## Red Queen Adversarial Checks

These tests actively try to find ways the exit code system could fail:

1. **Shadowing**: Does any variable named `exit_code` shadow the error method?
2. **Default values**: Are there `unwrap_or(0)` that could mask errors?
3. **Early returns**: Do any `return Ok(())` happen before error checks?
4. **Async boundaries**: Do errors cross async/await boundaries correctly?
5. **Drop handlers**: Could a Drop implementation change exit code?
6. **Panic handlers**: Does custom panic handling affect exit codes?
7. **Signal handlers**: Do SIGINT/SIGTERM handlers preserve error exit codes?
8. **Buffered output**: Is stderr flushed before exit?
