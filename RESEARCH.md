# Codebase Research Summary: shitty-swarm-manager

**Research Date:** February 11, 2026  
**Project:** PostgreSQL-based agent swarm coordination

---

## 1. WebSocket Implementation

**Status:** NOT IMPLEMENTED

### Current State
- **No WebSocket dependencies** in `Cargo.toml`
- No tungstenite, tokio-tungstenite, or ws crate
- Project uses **synchronous JSON request/response over STDIO**
- All operations are single-threaded request/response pairs

### Current Protocol
- **Location:** `src/protocol_runtime.rs:515` (main loop)
- **Entry Point:** `run_protocol_loop()` - async function that reads STDIN
- **Response Structure:** `ProtocolEnvelope` (src/protocol_envelope.rs)
- **Format:** JSON objects with request -> execute -> JSON response

### Opportunity
This is a greenfield opportunity to add WebSocket support for real-time streaming without breaking existing protocol.

---

## 2. Async Runtime

**Runtime:** Tokio 1.35 (full features)  
**Location:** `Cargo.toml:11`

### Usage Patterns

| Pattern | Location | Details |
|---------|----------|---------|
| `tokio::time::timeout()` | protocol_runtime.rs:789-790 | External command timeouts (3000ms default) |
| `async fn` + `.await` | protocol_runtime.rs throughout | 40+ async functions |
| Main event loop | protocol_runtime.rs:515 | `run_protocol_loop()` reads STDIN |
| `tokio::fs` | protocol_runtime.rs:26 | Async file operations |
| `AsyncBufReadExt`, `AsyncWriteExt` | protocol_runtime.rs:27 | STDIN/STDOUT async traits |
| `tokio::process::Command` | protocol_runtime.rs:28 | External command execution |

### Database Async
- **SQLx 0.8** with `runtime-tokio-rustls` feature
- Async pooled connections to PostgreSQL
- Query timeout boundaries: 100ms - 30000ms

### Key Characteristics
- All I/O is async (stdin, stdout, files, database, subprocesses)
- Main loop never blocks on I/O
- Supports concurrent request handling

---

## 3. Event Handling Infrastructure

**Architecture:** SQL-based event sourcing, NOT pub/sub

### Event Tables

#### execution_events (Line 261 of schema.sql)
**Purpose:** Deterministic execution event log

| Column | Type | Notes |
|--------|------|-------|
| seq | BIGSERIAL PRIMARY KEY | Sequence number |
| schema_version | INTEGER | Version tracking (default 1) |
| event_type | TEXT | Type of event (e.g., transition_retry) |
| entity_id | TEXT | Composite ID for tracing |
| bead_id | TEXT | Which bead triggered event |
| agent_id | INTEGER | Which agent executed |
| stage | TEXT | Stage name |
| causation_id | TEXT | Reference to cause |
| diagnostics_* | TEXT/BOOLEAN | Structured error info |
| payload | JSONB | Event-specific data |
| created_at | TIMESTAMPTZ | Timestamp |

**Event Types:**
- stage_completed, transition_finalize, transition_advance, transition_retry, transition_noop, transition_blocked

#### agent_messages (Line 198 of schema.sql)
**Purpose:** Inter-agent messaging

**Message Types:**
- contract_ready, implementation_ready, qa_complete, qa_failed, red_queen_failed
- implementation_retry, artifact_available, stage_complete, stage_failed
- blocking_issue, coordination

#### broadcast_log (Line 285 of schema.sql)
**Purpose:** Broadcast messages (simple audit log)

### Event Operations

```rust
// Read events - src/db/read_ops.rs:520
async fn get_execution_events(
    &self, 
    repo_id: &RepoId, 
    bead_filter: Option<&str>, 
    limit: i64
) -> Result<Vec<ExecutionEvent>>

// Record events - src/db/write_ops.rs:1605
async fn record_execution_event(
    &self,
    bead_id: &BeadId,
    agent_id: &AgentId,
    input: ExecutionEventWriteInput
) -> Result<()>

// Write broadcasts - src/db/write_ops.rs:140
async fn write_broadcast(&self, from_agent: &str, msg: &str) -> Result<i64>
```

### Important Notes
- **NO pub/sub mechanism** - all events are pull-based via database queries
- **NO `tokio::sync::broadcast`** channel
- **NO event subscriptions** - clients must poll the database
- **Deterministic ordering** via sequence numbers (seq BIGSERIAL)

---

## 4. Data Streaming Patterns

**Status:** PULL-BASED, NOT PUSH-BASED

### Streaming Patterns Used

| Pattern | Description | Location |
|---------|-------------|----------|
| Database polling | LIMIT-based pagination | read_ops.rs |
| Request/Response | JSON over STDIO | protocol_runtime.rs:515 |
| No streaming responses | All data in single JSON object | protocol_envelope.rs |
| Async subprocess | Timeout-wrapped Command execution | protocol_runtime.rs:852 |

### Data Flow
```
Client -> STDIN (JSON request)
  |
  v
parse_protocol_line() -> parse JSON
  |
  v
execute_request() -> dispatch to handler
  |
  v
handle_* function -> query database
  |
  v
Build response ProtocolEnvelope
  |
  v
Write JSON to STDOUT -> Client
```

### Key Characteristics
- All responses are **complete JSON objects** (not streamed)
- **No `futures::Stream`** or streaming iterators
- **No channel-based data flow** between handlers
- **BufReader** used for buffered STDIN processing
- Events retrieved in **batches** (LIMIT 200)

### Timeout Configuration
```rust
const DEFAULT_DB_CONNECT_TIMEOUT_MS: u64 = 3_000;
const MIN_DB_CONNECT_TIMEOUT_MS: u64 = 100;
const MAX_DB_CONNECT_TIMEOUT_MS: u64 = 30_000;
```

---

## 5. JSON Schema Validation

**Approach:** Serde-based type safety, NOT json-schema crate

### Validation Mechanisms

#### 1. deny_unknown_fields
```rust
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureDiagnostics { ... }

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionEvent { ... }
```
**Location:** src/types/observability.rs:19, 28  
**Effect:** Rejects unknown JSON fields during deserialization

#### 2. Field Aliases for Versioning
```rust
#[serde(rename = "sequence", alias = "seq")]
pub seq: i64,

#[serde(rename = "payload_version", alias = "schema_version")]
pub schema_version: i32,
```
**Location:** src/types/observability.rs:30-32, 42  
**Purpose:** Backward compatibility with different field names

#### 3. TryFrom Trait for Enum Parsing
```rust
impl TryFrom<&str> for MessageType {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, String> { ... }
}
```
**Location:** src/types/messaging.rs:39  
**Validation:** Returns Err if unknown message type

#### 4. ParseInput Trait (40+ implementations)
```rust
pub trait ParseInput {
    type Input;
    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError>;
}
```
**Location:** src/protocol_runtime.rs:50  
**Pattern:** Custom parse_input() for each input type

#### 5. Manual Field Extraction
```rust
let field_value = request.args
    .get("field_name")
    .and_then(|v| v.as_type())
    .ok_or_else(|| ParseError::MissingField { field: "field_name".to_string() })?;
```
**Location:** protocol_runtime.rs:81-124  
**Validation:** Returns ParseError::MissingField or InvalidType

### Validation Errors
```rust
pub enum ParseError {
    MissingField { field: String },
    InvalidType { field: String, expected: String, got: String },
    InvalidValue { field: String, value: String },
    Custom(String),
}
```

### Database-Level Validation
CHECK constraints in schema.sql enforce:
- Enum values (status IN ('pending', 'in_progress', ...))
- Range checks (agent_id >= 1)
- Data types (schema_version >= 1)

---

## 6. Error Handling Patterns

**Framework:** `thiserror` crate v1.0  
**Location:** src/error.rs

### Error Type: SwarmError

```rust
pub enum SwarmError {
    DatabaseError(String),           // -> INTERNAL, exit 3
    SqlxError(sqlx::Error),          // -> INTERNAL, exit 3
    ConfigError(String),             // -> INVALID, exit 2
    AgentError(String),              // -> CONFLICT, exit 4
    BeadError(String),               // -> NOTFOUND, exit 5
    StageError(String),              // -> CONFLICT, exit 6
    IoError(std::io::Error),         // -> DEPENDENCY, exit 7
    SerializationError(serde_json::Error), // -> INVALID, exit 8
    Internal(String),                // -> INTERNAL, exit 9
}
```

### Error Mapping
Each error maps to:
1. **Protocol error code** (CLI_ERROR, EXISTS, NOTFOUND, INVALID, CONFLICT, BUSY, UNAUTHORIZED, DEPENDENCY, TIMEOUT, INTERNAL)
2. **Exit code** (2-9 for different error types)
3. **Serialization in ProtocolEnvelope** with optional context

### Network Error Handling

```rust
// Timeout wrapping for external commands
let output = tokio::time::timeout(
    Duration::from_millis(timeout_ms),
    cmd.output()
).await
.map_err(|_| SwarmError::Internal("timeout".to_string()))?
```
**Location:** protocol_runtime.rs:789-790

### Database Error Handling Pattern

```rust
sqlx::query_as::<_, SomeRow>(sql)
    .bind(param)
    .fetch_all(self.pool())
    .await
    .map_err(|e| SwarmError::DatabaseError(format!("Failed to X: {e}")))?
    .with_ctx(json!({"details": value}))
```

### Protocol Error Response

```rust
pub struct ProtocolError {
    pub code: String,           // e.g., "INVALID"
    pub msg: String,            // Human-readable message
    pub ctx: Option<Box<Value>>, // Additional context as JSON
}

pub struct ProtocolEnvelope {
    pub ok: bool,
    pub rid: Option<String>,
    pub t: i64,                 // Timestamp
    pub ms: Option<i64>,        // Execution time
    pub d: Option<Box<Value>>,  // Success data
    pub err: Option<Box<ProtocolError>>, // Error details
    pub fix: Option<String>,    // Suggested fix
    pub next: Option<String>,   // Next command to try
    pub state: Option<Box<Value>>, // Current state
}
```

### No-Panic Policy

Strict lint enforcement:
```rust
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![forbid(unsafe_code)]
```

---

## Summary: Architectural Insights

| Aspect | Current State | Notes |
|--------|---------------|-------|
| Protocol | STDIO JSON | Request/response, not streaming |
| Real-time | Poll-based | Database queries, not push |
| Async | Tokio 1.35 | All I/O async, never blocks |
| Events | SQL event sourcing | Deterministic, sequence-based |
| Pub/Sub | None | Pull-based access pattern |
| Validation | Serde + types | Type-safe, not json-schema crate |
| Error Handling | thiserror | Structured codes, exit codes |
| Panic Policy | Forbidden | Lint denials enforced |

---

## Opportunities for Enhancement

1. **WebSocket Support** - Real-time event streaming
2. **Event Subscriptions** - Push-based notifications
3. **Streaming Responses** - Chunked JSON for large datasets
4. **Server Mode** - Persistent connection vs. STDIO
5. **Multi-client Coordination** - Broadcast messages with subscriptions

---

## Key File Quick Reference

| Purpose | File | Key Lines |
|---------|------|-----------|
| Protocol entry | src/protocol_runtime.rs | 515 (main loop) |
| Response structure | src/protocol_envelope.rs | 12-106 |
| Error codes | src/error.rs | 10-81 |
| Event schema | crates/swarm-coordinator/schema.sql | 261, 198, 285 |
| Event types | src/types/observability.rs | All |
| Message types | src/types/messaging.rs | All |
| Database queries | src/db/read_ops.rs | 520 (get_execution_events) |
| Event recording | src/db/write_ops.rs | 1605 (record_execution_event) |

---

Generated: February 11, 2026
