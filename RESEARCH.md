# Architecture Research Summary

**Project:** shitty-swarm-manager
**Date:** February 2026

## Quick Reference

| Aspect | Implementation | Key Files |
|--------|---------------|-----------|
| **Protocol** | STDIO JSON (request/response) | `protocol_runtime.rs:515` |
| **Runtime** | Tokio 1.35 (async) | `Cargo.toml` |
| **Events** | SQL-based event sourcing | `schema.sql:261` |
| **Validation** | Serde + types | `types/observability.rs` |
| **Errors** | thiserror + exit codes | `error.rs` |

## Architecture Decisions

### Protocol Layer
- **JSONL over STDIO** - Single-line JSON per request/response
- **No streaming** - All responses are complete JSON objects
- **Pull-based** - No pub/sub, database polling only

### Async Runtime
- **Tokio 1.35** with full features
- All I/O is async (stdin, stdout, files, database, subprocesses)
- Timeouts: 100ms min, 30000ms max, 3000ms default

### Event System
- **SQL event sourcing** - `execution_events` table with sequence numbers
- **Inter-agent messaging** - `agent_messages` table
- **No channels** - No `tokio::sync::broadcast`, all pull-based

### Validation Strategy
- **Type-safe** - Serde `deny_unknown_fields`
- **Enum parsing** - `TryFrom<&str>` for all enums
- **Database constraints** - CHECK constraints in schema

### Error Handling
- **No panics** - `#![deny(clippy::unwrap_used, clippy::panic)]`
- **Structured codes** - CLI_ERROR, EXISTS, NOTFOUND, etc.
- **Exit codes** - 0=success, 1-9 for specific error types

## Key File Reference

| Purpose | File |
|---------|------|
| Protocol entry | `src/protocol_runtime.rs` |
| Response structure | `src/protocol_envelope.rs` |
| Error types | `src/error.rs` |
| Event schema | `src/canonical_schema/schema.sql` |
| Database queries | `src/db/read_ops.rs`, `src/db/write_ops.rs` |
