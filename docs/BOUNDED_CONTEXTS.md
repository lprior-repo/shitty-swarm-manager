# Bounded Context Map - Domain-Driven Design

**shitty-swarm-manager** - PostgreSQL-based agent swarm coordination

---

## Quick Reference

| Context | Purpose | Key Files |
|---------|---------|-----------|
| **Coordination** | JSONL protocol parsing, command routing | `protocol_runtime/`, `main.rs` |
| **Execution** | Agent lifecycle, stage transitions | `agent_runtime.rs`, `stage_executors/` |
| **Landing** | Persistence, aggregates, repositories | `ddd.rs`, `db/` |
| **Skill** | AI skill invocation | `skill_execution.rs` |
| **Read Models** | CQRS queries, views | `db/read_ops.rs`, `db/mappers.rs` |

**Core Aggregates:** Agent, Bead, Stage

**Pipeline:** `RustContract → Implement → QaEnforcer → RedQueen → Done`

---

## Bounded Contexts

### 1. Coordination Context

**Purpose:** Accepts JSON commands, parses, routes to handlers.

**ACL:** `ParseInput` trait converts external JSON to domain types.

### 2. Execution Context

**Purpose:** Manages agent lifecycle through stage pipeline.

**Aggregate:** `RuntimeAgent` with state machine for stage progression.

**Transitions:**
- Failed + retries left → Retry
- Failed + no retries → Block
- Passed → Advance
- RedQueen passed → Complete

### 3. Skill Invocation Context

**Purpose:** Executes reusable skills during bead processing.

**ACL:** Parser protects domain from malformed skill invocations.

### 4. Landing Context

**Purpose:** Persists agent, bead, stage data. Implements aggregates.

**Repositories:**
- `RuntimePgAgentRepository` - Agent state CRUD
- `RuntimePgBeadRepository` - Bead claiming/release
- `RuntimePgStageRepository` - Stage history

### 5. Read Models Context

**Purpose:** CQRS read side with denormalized views.

**Views:** `v_swarm_progress`, `v_active_agents`, `v_feedback_required`, `v_unread_messages`

---

## Ubiquitous Language

| Term | Type | Definition |
|------|------|------------|
| **Bead** | Entity | Unit of work processed by agent |
| **Agent** | Entity | Autonomous worker that processes beads |
| **Stage** | Enum | Pipeline phase |
| **Claim** | Verb | Reservation of bead by agent |
| **Attempt** | Value | Retry counter for implementation |
| **Transition** | Event | State change from stage result |
| **Landing** | Process | Finalization requiring push confirmation |
| **Skill** | Entity | Reusable capability during processing |

---

## Aggregate Boundaries

### Agent Aggregate
- **Root:** `RuntimeAgent`
- **Invariants:** Working → bead_id required; attempt ≤ max_attempts

### Bead Aggregate
- **Root:** `RuntimeBead`
- **Invariants:** One claimant at a time; P0 priority in claim queue

### Stage Aggregate
- **Root:** `RuntimeStage` (enumeration)
- **Transitions:** Sequential with retry loop on failure

---

## Integration Patterns

```
Coordination → Execution → Landing → Read Models
     │             │           │
     └─────────────┴───────────┘
              │
        Shared Kernel (types, errors)
```

**Data Flow:**
1. Command parsed in Coordination
2. Stage executed in Execution
3. State persisted in Landing
4. Views updated in Read Models

---

## Module Ownership

| Module | Context | Pattern |
|--------|---------|---------|
| `protocol_runtime.rs` | Coordination | ACL + Factory |
| `agent_runtime.rs` | Execution | Aggregate + Service |
| `ddd.rs` | Landing | Repository + Aggregate |
| `skill_execution.rs` | Skill | Service |
| `db/read_ops.rs` | Read Models | CQRS Read |
| `db/mappers.rs` | Read Models | Mapper |

---

## Invariants

### Agent
- `Working` status requires `bead_id`
- `Done` status requires no `bead_id`
- `implementation_attempt < max_attempts`

### Bead
- Only one agent can claim at a time
- Blocked beads excluded from queue

### Stage Transition

| Result | Retries Left | Transition |
|--------|--------------|------------|
| Failed | Yes | Retry |
| Failed | No | Block |
| Passed | N/A | Advance |
| RedQueen passed | N/A | Complete |
