# Contract: CLI Exit Codes

## Bead
`swm-kyh` - cli: Fix exit code always 0 even on errors

## Invariants
- Exit 0 ONLY on success
- All errors → non-zero exit
- Deterministic mapping (same error → same code)

## Exit Code Mapping

| Code | Error |
|------|-------|
| 0 | Success |
| 1 | CliError |
| 2 | ConfigError |
| 3 | DatabaseError |
| 4 | AgentError |
| 5 | BeadError |
| 6 | StageError |
| 7 | IoError |
| 8 | SerializationError |
| 9 | Internal |

## Non-goals
- No error message changes
- No new error variants
- No JSON output changes
