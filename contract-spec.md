# Contract Specification: Database Connection Timeout Contract

## Context
- **Feature**: Database connection timeout honoring
- **Bead**: swm-1ai - Explicit unreachable database URL can hang >30s and does not honor connect_timeout_ms contract
- **Domain terms**:
  - `connect_timeout_ms`: Request-level timeout budget for database connection attempts
  - `SwarmDb::new()`: Constructor that creates a PostgreSQL connection pool
  - `PgPoolOptions`: SQLx pool configuration builder
  - `try_connect_candidates`: Async function that attempts connections with timeout enforcement

## Preconditions
- [ ] The `database_url` parameter is a valid PostgreSQL connection string format
- [ ] The `connect_timeout_ms` value is clamped between 100ms and 30000ms inclusive
- [ ] The system has async runtime available (Tokio)
- [ ] Network conditions may cause connections to be unreachable (firewall, no listener, etc.)

## Postconditions
- [ ] Connection attempts fail within `connect_timeout_ms + tolerance` (tolerance = 500ms max)
- [ ] Error responses include the actual time spent attempting connection
- [ ] Pool creation honors the configured timeout at the SQLx level (not just Tokio wrapper)
- [ ] No resource leaks from timed-out connection attempts

## Invariants
- [ ] Connection timeout is ALWAYS configured on `PgPoolOptions` when creating a pool
- [ ] The effective timeout is the MINIMUM of request-level and system-default (3s)
- [ ] Timeout errors are distinguishable from authentication/permission errors
- [ ] Connection timing metrics are accurate and include timeout failures

## Error Taxonomy
- `Error::ConnectionTimeout` - when connection cannot be established within timeout budget
- `Error::ConnectionRefused` - when connection is actively refused (immediate failure)
- `Error::AuthenticationFailed` - when credentials are invalid (may take timeout duration)
- `Error::InvalidDatabaseUrl` - when URL format is invalid (immediate, before network)

## Contract Signatures

```rust
// Modified SwarmDb constructor with timeout
pub async fn new_with_timeout(
    database_url: &str, 
    connect_timeout_ms: u64
) -> Result<Self, SwarmError>

// Internal helper for PgPoolOptions configuration
fn configure_pool_options(
    max_connections: u32,
    connect_timeout_ms: u64
) -> PgPoolOptions
```

## Non-goals
- [ ] Do NOT retry connections beyond the candidate list
- [ ] Do NOT cache failed connection attempts
- [ ] Do NOT expose internal SQLx error details to end users
