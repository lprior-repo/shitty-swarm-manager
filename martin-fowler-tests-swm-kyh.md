# Martin Fowler Test Plan: CLI Exit Codes

## Contract

| Exit Code | Error Type | When |
|-----------|------------|------|
| 0 | Success | Operation completed |
| 1 | CliError | Parsing error |
| 2 | ConfigError | Invalid config |
| 3 | DatabaseError | DB failure |
| 4 | AgentError | Agent lifecycle |
| 5 | BeadError | Bead not found |
| 6 | StageError | Stage failure |
| 7 | IoError | File system |
| 8 | SerializationError | JSON error |
| 9 | Internal | Unexpected failure |

## Given-When-Then Scenarios

### Success
- **Given** valid command → **When** executes → **Then** exit 0

### CLI Errors (exit 1)
- Unknown command: `swarm foobar`
- Missing arg: `swarm agent` (no --id)
- Invalid type: `swarm agent --id abc`

### Database Errors (exit 3)
- Connection refused
- Query failure

### Edge Cases
- Error in async context → correct exit code
- Multiple error conversions → preserved semantics
- SIGINT during operation → non-zero exit

## Invariants

- Exit 0 ONLY on success
- All errors → non-zero exit
- Same error → same exit code (deterministic)
- Shell scripts can rely on `$?`
