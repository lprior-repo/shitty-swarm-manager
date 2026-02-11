# Bounded Context Map - Domain-Driven Design

**shitty-swarm-manager** - A PostgreSQL-based agent swarm coordination system

---

## Context Map (Strategic DDD)

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              SHITTY SWARM MANAGER                                   │
│                                                                                     │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐                 │
│  │   COORDINATION  │◄──│    EXECUTION    │◄──│ SKILL INVOCATION│                 │
│  │    CONTEXT      │   │    CONTEXT      │   │    CONTEXT      │                 │
│  │                 │   │                 │   │                 │                 │
│  │  Upstream:      │   │  Downstream:    │   │  Downstream:    │                 │
│  │  External CLI   │   │  Coordination   │   │  Execution      │                 │
│  │                 │   │                 │   │                 │                 │
│  │  ACL: ParseInput│   │  ACL: StageFn   │   │  ACL: Parser    │                 │
│  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘                 │
│           │                      │                      │                           │
│           │    Shared Kernel     │                      │                           │
│           └──────────────────────┼──────────────────────┘                           │
│                                  │                                                  │
│                           ┌──────┴──────┐                                           │
│                           │   LANDING   │                                           │
│                           │   CONTEXT   │                                           │
│                           │             │                                           │
│                           │  Aggregate  │                                           │
│                           │   Roots:    │                                           │
│                           │  Agent,     │                                           │
│                           │  Bead,      │                                           │
│                           │  Stage      │                                           │
│                           └──────┬──────┘                                           │
│                                  │                                                  │
│                           ┌──────┴──────┐                                           │
│                           │  READ MODELS│                                           │
│                           │   CONTEXT   │                                           │
│                           │             │                                           │
│                           │ CQRS Query  │                                           │
│                           │  Side Only  │                                           │
│                           └─────────────┘                                           │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Bounded Context Details

### 1. COORDINATION Context

**Module:** `src/protocol_runtime.rs`, `src/main.rs`

**Subdomain Type:** Core Domain (Generic)

**Purpose:** Accepts external JSON commands, parses them, routes to appropriate handlers.

#### Domain Model

```
┌─────────────────────────────────────────────────────────────────┐
│                      ProtocolRequest                             │
│  ├─ cmd: String                    ← Ubiquitous Language         │
│  ├─ rid: Option<String>           ← Request ID (correlation)    │
│  ├─ dry: Option<bool>             ← Dry-run flag                │
│  └─ args: Map<String, Value>     ← Arbitrary arguments         │
└─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │                   │
                    ▼                   ▼
            ┌───────────────┐   ┌───────────────┐
            │   CliCommand  │   │   BatchAcc    │
            │    (Enum)     │   │   (Wrapper)   │
            └───────────────┘   └───────────────┘
```

#### Anti-Corruption Layer

```rust
// ACL: Converts external JSON to domain types
pub trait ParseInput {
    type Input;
    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError>;
}

// Implementation protects domain from malformed input
impl ParseInput for swarm::AgentInput {
    type Input = Self;
    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> {
        // Input validation before domain entry
        let id = request.args.get("id")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| ParseError::MissingField { field: "id".to_string() })?;

        Ok(Self::Input { id, dry: request.args.get("dry").and_then(|v| v.as_bool()) })
    }
}
```

#### Published Language

| External Term | Internal Term | Transformation |
|--------------|---------------|----------------|
| `id` (CLI arg) | `AgentId` | Parsed via `ParseInput` |
| `dry` (flag) | `dry_run` | Boolean extraction |
| JSON object | `ProtocolRequest` | serde::Deserialize |

---

### 2. EXECUTION Context

**Module:** `src/agent_runtime.rs`, `src/agent_runtime_support.rs`

**Subdomain Type:** Core Domain (Supporting)

**Purpose:** Manages agent lifecycle through stage pipeline.

#### Aggregate: RuntimeAgent

```
┌─────────────────────────────────────────────────────────────────┐
│                    RuntimeAgent (Aggregate Root)                 │
│                                                                 │
│  Identity:                                                      │
│  └─ agent_id: RuntimeAgentId  ──────────────────────────────────┼── Entity
│                                                                 │
│  State:                                                         │
│  ├─ bead_id: Option<RuntimeBeadId>  ──────────────────────────┼── Entity Ref
│  ├─ current_stage: Option<RuntimeStage> ────────────────────────┼── Enumeration
│  ├─ status: RuntimeAgentStatus      ────────────────────────────┼── Enumeration
│  └─ implementation_attempt: u32     ────────────────────────────┼── Value
│                                                                 │
│  Invariants:                                                    │
│  ├─ If status = Working, then bead_id.is_some()                 │
│  ├─ If status = Done, then current_stage = Done                 │
│  └─ implementation_attempt < max_attempts                        │
└─────────────────────────────────────────────────────────────────┘
```

#### Domain Service

```rust
// Pure function - no side effects, deterministic
pub fn runtime_determine_transition(
    stage: RuntimeStage,
    result: &RuntimeStageResult,
    attempt: u32,
    max_attempts: u32,
) -> RuntimeStageTransition {
    // Business rule: Failed stage after max retries → Block
    if !result.is_success() {
        return if attempt >= max_attempts {
            RuntimeStageTransition::Block
        } else {
            RuntimeStageTransition::Retry
        };
    }

    // Business rule: RedQueen passed → Complete
    if stage == RuntimeStage::RedQueen {
        return RuntimeStageTransition::Complete;
    }

    // Business rule: Otherwise advance to next stage
    stage.next().map_or(
        RuntimeStageTransition::NoOp,
        RuntimeStageTransition::Advance,
    )
}
```

#### Value Objects

```rust
// Value Object: Immutable, self-validating
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeAgentId {
    pub repo_id: RuntimeRepoId,  // Value Object
    pub number: u32,            // Primitive
}

// Factory method enforces invariants
impl RuntimeAgentId {
    pub fn new(repo_id: RuntimeRepoId, number: u32) -> Self {
        Self { repo_id, number }
    }
}

// Value Object: Identity without behavior
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeBeadId(String);

impl RuntimeBeadId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())  // Wraps string, prevents raw ID leakage
    }

    pub fn value(&self) -> &str {
        &self.0
    }
}
```

#### Enumerations (State Machines)

```rust
// Enumeration: Finite state machine for stage progression
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeStage {
    RustContract,  // Initial stage
    Implement,     // Implementation phase
    QaEnforcer,    // Quality assurance
    RedQueen,      // Evolutionary testing
    Done,         // Terminal state
}

impl RuntimeStage {
    pub const fn next(&self) -> Option<Self> {
        match self {
            Self::RustContract => Some(Self::Implement),
            Self::Implement => Some(Self::QaEnforcer),
            Self::QaEnforcer => Some(Self::RedQueen),
            Self::RedQueen => Some(Self::Done),
            Self::Done => None,
        }
    }
}

// Enumeration: Agent lifecycle states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeAgentStatus {
    Idle,      // Available for work
    Working,   // Processing a bead
    Waiting,   // Blocked on external resource
    Error,     // In error state
    Done,      // Completed all work
}
```

---

### 3. SKILL INVOCATION Context

**Module:** `src/skill_execution.rs`, `src/skill_execution_parsing.rs`

**Subdomain Type:** Supporting Domain

**Purpose:** Executes reusable skills during bead processing.

#### ACL: Parser

```rust
// Parser ACL protects domain from malformed skill invocations
pub fn parse_skill_execution(input: &str) -> Result<SkillExecution, ParseError> {
    // Input sanitization before domain entry
    let cleaned = input.trim();
    if cleaned.is_empty() {
        return Err(ParseError::Custom("Empty skill input".to_string()));
    }

    // Parse and validate
    serde_json::from_str(cleaned)
        .map_err(|e| ParseError::Custom(format!("Invalid JSON: {e}")))
}
```

---

### 4. LANDING Context

**Module:** `src/ddd.rs`

**Subdomain Type:** Core Domain (Generic)

**Purpose:** Persists agent, bead, and stage data. Implements aggregates.

#### Repository Interfaces (Published)

```rust
// Repository interface defines domain contract
// Implementation details hidden from domain

pub struct RuntimePgAgentRepository {
    pool: PgPool,  // Infrastructure dependency
}

impl RuntimePgAgentRepository {
    // Domain operations - persistence-agnostic signatures
    pub async fn find_by_id(&self, agent_id: &RuntimeAgentId) -> Result<Option<RuntimeAgentState>>;
    pub async fn update_status(&self, agent_id: &RuntimeAgentId, status: RuntimeAgentStatus) -> Result<()>;
}

pub struct RuntimePgBeadRepository {
    pool: PgPool,
}

impl RuntimePgBeadRepository {
    pub async fn claim_next(&self, agent_id: &RuntimeAgentId) -> Result<Option<RuntimeBeadId>>;
    pub async fn release(&self, agent_id: &RuntimeAgentId) -> Result<()>;
    pub async fn mark_blocked(&self, bead_id: &RuntimeBeadId, reason: &str) -> Result<()>;
}

pub struct RuntimePgStageRepository {
    pool: PgPool,
}

impl RuntimePgStageRepository {
    pub async fn record_started(&self, agent_id: &RuntimeAgentId, bead_id: &RuntimeBeadId,
                                stage: RuntimeStage, attempt: u32) -> Result<i64>;
    pub async fn record_completed(&self, agent_id: &RuntimeAgentId, bead_id: &RuntimeBeadId,
                                  stage: RuntimeStage, attempt: u32, result: RuntimeStageResult,
                                  duration_ms: u64) -> Result<()>;
}
```

#### Domain Events (Implicit)

| Event | Producer | Consumer | Meaning |
|-------|----------|----------|---------|
| `StageStarted` | Execution | StageRepository | Agent began a stage |
| `StageCompleted` | Execution | StageRepository | Stage finished with result |
| `BeadClaimed` | BeadRepository | Agent | Agent reserved a bead |
| `BeadReleased` | BeadRepository | Agent | Bead returned to backlog |
| `BeadBlocked` | BeadRepository | Coordinator | Bead marked as blocked |

---

### 5. READ MODELS Context

**Module:** `src/db/read_ops.rs`, `src/db/mappers.rs`

**Subdomain Type:** Supporting Domain (CQRS Read Side)

**Purpose:** Provides denormalized views for efficient queries.

#### CQRS Read Models

```
┌─────────────────────────────────────────────────────────────────┐
│                        Read Models                               │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐    │
│  │ ProgressSummary │  │ AgentState     │  │ SwarmConfig     │    │
│  │                 │  │                 │  │                 │    │
│  ├─ completed: u32 │  ├─ agent_id: Id  │  ├─ max_agents: u32│    │
│  ├─ working: u32  │  ├─ bead_id: Opt  │  ├─ max_attempts  │    │
│  ├─ waiting: u32  │  ├─ status: Enum  │  ├─ claim_label   │    │
│  ├─ errors: u32   │  └─────────────────┘  └─────────────────┘    │
│  └─────────────────┘                                              │
│                                                                 │
│  Materialized Views:                                             │
│  ├─ v_swarm_progress                                            │
│  ├─ v_active_agents                                             │
│  ├─ v_available_agents                                          │
│  ├─ v_feedback_required                                         │
│  └─ v_unread_messages                                           │
└─────────────────────────────────────────────────────────────────┘
```

#### Mapper ACL

```rust
// Mapper shields domain from schema changes
mod mappers {
    use crate::types::{AgentStatus, Stage};

    pub fn parse_agent_state(
        agent_id: &AgentId,
        fields: AgentStateFields,
    ) -> Result<AgentState, SwarmError> {
        // Schema-to-domain transformation
        let status = AgentStatus::try_from(fields.status_str.as_str())
            .map_err(|e| SwarmError::DatabaseError(e.to_string()))?;

        Ok(AgentState {
            agent_id: agent_id.clone(),
            bead_id: fields.bead_id.map(BeadId::new),
            current_stage: fields.stage_str
                .and_then(|s| Stage::try_from_str(&s).ok()),
            status,
            implementation_attempt: fields.implementation_attempt as u32,
        })
    }
}
```

---

## Ubiquitous Language Dictionary

Canonical source: `docs/UBIQUITOUS_LANGUAGE.md`

| Term | Type | Definition | Context |
|------|------|------------|---------|
| **Bead** | Entity | Canonical unit of work processed by an agent | All |
| **Agent** | Entity | Autonomous worker that claims and processes beads | All |
| **Stage** | Enumeration | Pipeline phase (RustContract → Implement → QaEnforcer → RedQueen → Done) | Execution |
| **Claim** | Verb | Reservation of a bead by an agent | Landing |
| **Attempt** | Value | Retry counter for implementation cycles on a bead stage | Execution |
| **Transition** | Domain Event | Deterministic state change derived from stage result and attempt budget | Execution + Landing |
| **Landing** | Process | Finalization workflow that requires push confirmation before completion | Landing |
| **Release** | Verb | Return of an unprocessed bead to the backlog | Landing |
| **Skill** | Entity | Reusable capability invoked during bead processing | Skill |
| **Repo** | Value Object | Git repository identifier | Coordination |
| **Aggregate** | Pattern | Agent, Bead, Stage as transactional consistency boundary | Landing |
| **Repository** | Pattern | Persistence abstraction for aggregates | Landing |
| **Read Model** | Pattern | Denormalized view for efficient queries | Read Models |
| **ACL** | Pattern | Anti-Corruption Layer - translation boundary | All |
| **CQRS** | Pattern | Command Query Responsibility Segregation | Read Models |

Deprecated aliases: task, issue, work item

---

## Aggregate Boundaries

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          AGGREGATE: AGENT                                    │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                         RuntimeAgent (Root)                              │ │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │ │
│  │  │ RuntimeAgentId  │  │ RuntimeBeadId   │  │ RuntimeStage    │       │ │
│  │  │ (Value Object)  │──┤ (Reference)     │──┤ (Enumeration)   │       │ │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘       │ │
│  │                                                                          │ │
│  │  Invariants enforced by root:                                           │ │
│  │  ├─ status ↔ bead_id consistency                                        │ │
│  │  └─ implementation_attempt ≤ max_attempts                              │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  Transaction boundary: Stage progression recorded atomically                  │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                          AGGREGATE: BEAD                                      │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                         RuntimeBead (Root)                               │ │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │ │
│  │  │ RuntimeBeadId   │  │ BeadStatus      │  │ Priority        │       │ │
│  │  │ (Value Object)  │  │ (Enumeration)   │  │ (Value)         │       │ │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘       │ │
│  │                                                                          │ │
│  │  Invariants:                                                             │ │
│  │  ├─ Only one agent can claim at a time                                  │ │
│  │  ├─ P0 beads prioritized in claim_next                                   │ │
│  │  └─ Blocked beads excluded from claim queue                              │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                          AGGREGATE: STAGE                                     │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                         RuntimeStage (Enumeration)                       │ │
│  │                                                                          │ │
│  │  State transitions:                                                      │ │
│  │  RustContract → Implement → QaEnforcer → RedQueen → Done                │ │
│  │       ↑                                              │                   │ │
│  │       └──────────────────────────────────────────────┘                   │ │
│  │              (Retry on failure within max_attempts)                      │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Integration Patterns

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INTER-CONTEXT FLOWS                                │
│                                                                              │
│   COORDINATION                      EXECUTION                                │
│   ───────────                      ─────────                                │
│   │                                                                  │       │
│   │  Command: run_agent(id)                                         │       │
│   │  ─────────────────────────────────────────────────────────────▶  │       │
│   │                                                                  │       │
│   │                                          Stage progression      │       │
│   │                                          RuntimeStage.next()    │       │
│   │  ◀───────────────────────────────────────────────────────────── │       │
│   │           RuntimeStageTransition (Advance/Retry/Block)          │       │
│   │                                                                  │       │
│   └──────────────────────────────────────────────────────────────────┘       │
│                                    │                                          │
│                                    ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐       │
│   │                           LANDING                                │       │
│   │                                                                  │       │
│   │  Repository operations:                                            │       │
│   │  ├─ claim_next(agent_id) → Option<BeadId>                       │       │
│   │  ├─ record_started(agent, bead, stage, attempt) → id            │       │
│   │  ├─ record_completed(agent, bead, stage, result, duration)       │       │
│   │  └─ release(agent_id)                                            │       │
│   └──────────────────────────────────────────────────────────────────┘       │
│                                    │                                          │
│                                    ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐       │
│   │                        READ MODELS                               │       │
│   │                                                                  │       │
│   │  Query operations:                                                │       │
│   │  ├─ get_progress() → ProgressSummary                             │       │
│   │  ├─ get_all_active_agents() → Vec<(Agent, Bead, Status)>         │       │
│   │  └─ get_feedback_required() → Vec<(Bead, Agent, Stage)>          │       │
│   └──────────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Module Ownership Matrix

| Module | Bounded Context | DDD Pattern | ACL Type |
|--------|----------------|-------------|----------|
| `protocol_runtime.rs` | Coordination | ACL + Factory | Input Parser |
| `main.rs` | Coordination | Application Layer | CLI Entry |
| `agent_runtime.rs` | Execution | Aggregate + Service | Stage Transition |
| `ddd.rs` | Landing | Repository + Aggregate | Persistence ACL |
| `skill_execution.rs` | Skill | Service | Parser ACL |
| `read_ops.rs` | Read Models | CQRS Read | Mapper |
| `mappers.rs` | Read Models | Mapper | Schema Translator |

---

## Invariants and Business Rules

### Agent Aggregate Invariants

```rust
impl RuntimeAgentState {
    /// Validates agent state consistency
    pub fn is_valid(&self) -> bool {
        // Rule: Working agents must have a bead
        match self.status {
            RuntimeAgentStatus::Working => self.bead_id.is_some(),
            RuntimeAgentStatus::Idle => true,
            RuntimeAgentStatus::Waiting => true,
            RuntimeAgentStatus::Error => true,
            RuntimeAgentStatus::Done => self.bead_id.is_none(),
        }
    }

    /// Validates attempt count
    pub fn can_retry(&self, max_attempts: u32) -> bool {
        self.implementation_attempt < max_attempts
    }
}
```

### Bead Aggregate Invariants

```rust
// Claim idempotency: Same bead cannot be claimed twice
// Implemented at database level via:
//   SELECT claim_next_p0_bead(agent_id)
// Which atomically:
//   1. SELECTs next pending P0 bead
//   2. UPDATEs bead_id = agent_id
//   3. Returns bead_id or NULL if none available
```

### Stage Transition Rules

| Current Stage | Result | Attempt < Max | Transition |
|--------------|--------|---------------|------------|
| Any | Failed | Yes | Retry |
| Any | Failed | No | Block |
| Any | Passed | N/A | Advance |
| RedQueen | Passed | N/A | Complete |
| Done | N/A | N/A | NoOp |
