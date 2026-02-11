# Bounded Context Map

This document maps the Shitty Swarm Manager's bounded contexts to module boundaries with explicit ownership and anti-corruption layers.

## Context Map Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SHITTY SWARM MANAGER                                  │
│                                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  COORDINATION │  │  EXECUTION   │  │SKILL INVOKE │  │   LANDING    │       │
│  │   CONTEXT    │◄─┤   CONTEXT    │  │   CONTEXT   │  │   CONTEXT    │       │
│  │              │  │              │  │              │  │              │       │
│  │  Owner:      │  │  Owner:      │  │  Owner:      │  │  Owner:       │       │
│  │  Protocol    │  │  Agent       │  │  SkillExec  │  │  DDD          │       │
│  │  Runtime     │  │  Runtime     │  │              │  │              │       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         │                 │                 │                 │                │
│         │    ACL          │                 │                 │                │
│         └─────────────────┴─────────────────┴─────────────────┘                │
│                                    │                                          │
│                           ┌──────┴──────┐                                      │
│                           │  READ MODELS │                                     │
│                           │   CONTEXT    │                                     │
│                           │              │                                     │
│                           │    Owner:     │                                     │
│                           │      DB       │                                     │
│                           └──────────────┘                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Bounded Contexts

### 1. Coordination Context

**Module Boundary:** `src/protocol_runtime.rs`, `src/main.rs`

**Owned Entities:**
- `ProtocolRequest` / `ProtocolEnvelope` - Command/response protocol
- `CliCommand` - CLI command parsing
- `BatchAcc` - Batch operation accumulation
- `CommandSuccess` - Command result wrapper

**Anti-Corruption Layer:**
```rust
// Protocol boundary - converts external JSON to internal types
impl ParseInput for swarm::AgentInput {
    type Input = Self;
    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, ParseError> { ... }
}

// Entry points protected by ParseError ACL
pub async fn process_protocol_line(line: &str) -> Result<(), SwarmError>
```

**Integration Points:**
- → Execution Context: `execute_request()` → `run_agent()`
- → Read Models: `db_from_request()` → `SwarmDb`

---

### 2. Execution Context

**Module Boundary:** `src/agent_runtime.rs`, `src/agent_runtime_support.rs`

**Owned Entities:**
- `AgentRuntime` - Agent lifecycle management
- `RuntimeAgentState` / `RuntimeAgentId` / `RuntimeBeadId`
- `RuntimeStage` (RustContract → Implement → QaEnforcer → RedQueen → Done)
- `RuntimeStageResult` / `RuntimeStageTransition`

**Anti-Corruption Layer:**
```rust
// Stage decision tree - pure function for transition logic
pub fn runtime_determine_transition(
    stage: RuntimeStage,
    result: &RuntimeStageResult,
    attempt: u32,
    max_attempts: u32,
) -> RuntimeStageTransition
```

**Integration Points:**
- ← Coordination Context: `run_agent()` entry point
- → Skill Invocation: `skill_execution::execute_skill()`
- → Landing Context: Stage completion → state transition

---

### 3. Skill Invocation Context

**Module Boundary:** `src/skill_execution.rs`, `src/skill_execution_parsing.rs`, `src/skill_prompts.rs`

**Owned Entities:**
- `SkillExecution` - Skill execution orchestration
- `SkillExecutionResult` - Execution outcome
- `StagePrompt` / `SkillPrompt` - Prompt templates

**Anti-Corruption Layer:**
```rust
// Parser ACL - sanitizes skill input
pub fn parse_skill_execution(input: &str) -> Result<SkillExecution, ParseError>

// Prompt injection protection
pub fn render_skill_prompt(prompt: &SkillPrompt, context: &ExecutionContext) -> String
```

**Integration Points:**
- ← Execution Context: Skill execution requests
- → Landing Context: Skill completion events

---

### 4. Landing Context

**Module Boundary:** `src/ddd.rs`, `src/stage_executors.rs`, `src/stage_executor_content.rs`

**Owned Entities:**
- `RuntimePgAgentRepository` - Agent state persistence
- `RuntimePgBeadRepository` - Bead claim/release
- `RuntimePgStageRepository` - Stage history tracking
- `RuntimeStageTransition` (Advance, Retry, Complete, Block, NoOp)

**Anti-Corruption Layer:**
```rust
// Repository ACLs - database isolation
impl RuntimePgAgentRepository {
    pub async fn find_by_id(&self, agent_id: &RuntimeAgentId) -> Result<Option<RuntimeAgentState>>
    pub async fn update_status(&self, agent_id: &RuntimeAgentId, status: RuntimeAgentStatus) -> Result<()>
}

impl RuntimePgBeadRepository {
    pub async fn claim_next(&self, agent_id: &RuntimeAgentId) -> Result<Option<RuntimeBeadId>>
    pub async fn release(&self, agent_id: &RuntimeAgentId) -> Result<()>
}
```

**Integration Points:**
- ← Execution Context: Stage transitions
- ← Skill Invocation: Completion events
- → Read Models: Persistence queries

---

### 5. Read Models Context

**Module Boundary:** `src/db/read_ops.rs`, `src/db/mappers.rs`, `src/types/*.rs`

**Owned Entities:**
- `SwarmDb` - Database connection pool
- `ProgressSummary` - Aggregated progress metrics
- `AgentState` / `AgentStatus` - Agent state snapshots
- `SwarmConfig` / `SwarmStatus` - Configuration read model

**Anti-Corruption Layer:**
```rust
// Read-only operations with semantic mapping
impl SwarmDb {
    pub async fn get_progress(&self, repo_id: &RepoId) -> Result<ProgressSummary>
    pub async fn get_all_active_agents(&self) -> Result<Vec<(RepoId, u32, Option<String>, String)>>
    pub async fn list_active_resource_locks(&self) -> Result<Vec<(String, String, i64, i64)>>
}

// Mapper ACL - shields domain from schema changes
mod mappers {
    pub fn map_db_agent_to_agent_state(row: &DbRow) -> AgentState { ... }
}
```

**Integration Points:**
- ← All Contexts: Query operations
- → Coordination Context: State reporting via `ProtocolEnvelope`

---

## Ubiquitous Language Glossary

| Term | Context | Definition |
|------|---------|------------|
| Bead | Landing | Work unit (task/issue) being processed by an agent |
| Agent | Coordination | Autonomous worker that claims and processes beads |
| Stage | Execution | Pipeline phase: RustContract → Implement → QaEnforcer → RedQueen → Done |
| Claim | Landing | Reservation of a bead by an agent for processing |
| Release | Landing | Return of an unprocessed bead to the backlog |
| Skill | Skill Invocation | Reusable capability invoked during bead processing |
| Repo | Coordination | Git repository being coordinated |
| ACL | All | Anti-Corruption Layer - boundary protection mechanism |
| Read Model | Read Models | Denormalized view for efficient queries |

---

## Anti-Corruption Boundaries

```
EXTERNAL INPUT                    BOUNDED CONTEXT OUTPUT
      │                                    │
      ▼                                    ▼
┌──────────────────────────────────────────────────────────┐
│                    ACL LAYER                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │ JSON Parse  │  │ Type        │  │ Schema      │      │
│  │ Sanitize    │  │ Conversion  │  │ Validation  │      │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘      │
│         │                │                │              │
└─────────┼────────────────┼────────────────┼──────────────┘
          │                │                │
          ▼                ▼                ▼
    ┌─────────────────────────────────────────────┐
    │         DOMAIN MODEL (Pure Rust)           │
    │  Entities, Value Objects, Aggregates       │
    └─────────────────────────────────────────────┘
```

---

## Module Ownership Matrix

| Module | Owner Context | ACL Type | Protected By |
|--------|---------------|----------|--------------|
| `protocol_runtime.rs` | Coordination | Input | `ParseError` enum |
| `agent_runtime.rs` | Execution | State Machine | `RuntimeStage` enum |
| `ddd.rs` | Landing | Repository | `RuntimeError` type |
| `skill_execution.rs` | Skill | Parser | `ParseError` type |
| `db/read_ops.rs` | Read Models | Query | `Result<T>` wrapper |
| `types/*.rs` | Shared | Value Objects | `new()` constructors |

---

## Dependencies Between Contexts

```
Coordination ─────► Execution ─────► Skill Invocation
       │                │                   │
       │                ▼                   ▼
       │           Landing ◄───────────────┘
       │                │
       └────────────────┘
                │
                ▼
          Read Models
```

**Flow:** Commands enter via Coordination → execute in Execution context → invoke skills → land results → query via read models.
