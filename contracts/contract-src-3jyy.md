# Contract: Soft Sticky Mode with Fallback Logic

**Bead ID:** src-3jyy  
**Title:** sticky: Implement soft sticky mode with fallback logic  
**Agent:** #16  
**Date:** 2026-02-09

## 1. Requirements Summary

Implement a "soft sticky mode" distribution strategy that:
- **Prefers previous worker**: Assigns beads to the same worker that previously handled them
- **Falls back gracefully**: If the previous worker is unavailable (busy/dead), assigns to any idle worker
- **Always returns a worker**: Never blocks or returns None when idle workers exist
- **Maintains >80% hit rate**: Achieves sticky assignment in >80% of cases where previous worker is idle

## 2. System Context

### Existing Components

1. **DistributionStrategy Trait** (`crates/orchestrator/src/distribution/strategy.rs`)
   - `select_bead(&self, ready_beads: &[String], ctx: &DistributionContext) -> Option<String>`
   - `select_agent(&self, bead_id: &str, agents: &[String], ctx: &DistributionContext) -> Option<String>`

2. **AffinityStrategy** (`crates/orchestrator/src/distribution/affinity.rs`)
   - Already implements capability and preference-based scoring
   - Has `preferred_agents` field in `BeadMetadata`
   - Supports soft/hard affinity modes

3. **SchedulerActor** (`crates/orchestrator/src/actors/scheduler.rs`)
   - Maintains `worker_assignments: HashMap<BeadId, String>` (line 82)
   - Tracks current bead-to-worker mappings
   - Emits `ClaimBead { bead_id, worker_id }` messages

### Integration Points

- **Assignment Storage**: The `worker_assignments` HashMap in `CoreSchedulerState`
- **Distribution Context**: `DistributionContext` carries metadata for decision-making
- **Strategy Registry**: `create_strategy()` in `mod.rs` supports dynamic strategy selection

## 3. Invariants

### MUST Maintain (Functional Correctness)
1. **No blocking**: Must always return a worker if any idle worker exists
2. **No double-assignment**: Never assign a bead to multiple workers simultaneously
3. **Idempotency**: Repeated calls with same inputs produce same results
4. **Thread safety**: Strategy must be `Send + Sync`

### SHOULD Maintain (Performance)
1. **>80% sticky hit rate**: When previous worker is idle, assign to them >80% of the time
2. **O(n) complexity**: Linear scan of agents, no nested loops
3. **Zero heap allocations**: Avoid allocations in hot path

### MUST NOT Violate
1. **Never panic**: Use `Result<T, Error>` throughout
2. **Never unwrap**: Use `.unwrap()` or `.expect()`
3. **Never block**: All operations must be non-blocking

## 4. Data Flow

```
┌─────────────────┐
│ SchedulerActor  │
│                 │
│ worker_assignments: HashMap<BeadId, String>
│   bead-1 → worker-a
│   bead-2 → worker-b
└────────┬────────┘
         │ ClaimBead messages
         ▼
┌─────────────────────────┐
│ StickyStrategy          │
│                         │
│ select_agent(           │
│   bead_id,              │
│   agents,               │
│   ctx                   │
│ ) → Option<String>      │
└────────┬────────────────┘
         │
         │ 1. Check worker_assignments
         │ 2. If previous worker exists:
         │    - Check if idle (in agents list)
         │    - If idle: return previous worker
         │    - If busy: fall back to load-balanced
         │ 3. If no previous worker:
         │    - Return least-loaded agent
         │
         ▼
┌─────────────────┐
│ Agent Assigned  │
│ worker-a        │
└─────────────────┘
```

## 5. Implementation Plan

### Phase 1: Create StickyStrategy Struct

**File:** `crates/orchestrator/src/distribution/sticky.rs`

```rust
pub struct StickyStrategy {
    /// Weight for sticky preference (0.0 - 1.0)
    sticky_weight: f64,
    /// Weight for load balancing (0.0 - 1.0)
    load_weight: f64,
}
```

### Phase 2: Implement Selection Logic

**Key Function:** `select_agent()`

```rust
fn select_agent(&self, bead_id: &str, agents: &[String], ctx: &DistributionContext) -> Option<String> {
    // 1. Extract previous worker from ctx.custom or metadata
    // 2. If previous worker exists and is in agents list:
    //    - Check if idle (load < threshold)
    //    - If idle: return previous worker (STICKY HIT)
    // 3. Fall back to load-balanced selection
    // 4. Return selected agent
}
```

### Phase 3: Integrate Assignment History

**Problem:** The `worker_assignments` map is in `SchedulerActor`, not accessible from `DistributionStrategy`

**Solution:** Pass assignment history via `DistributionContext.custom`

```rust
// In SchedulerActor, when calling select_agent:
let mut ctx = ctx.clone();
if let Some(prev_worker) = self.worker_assignments.get(bead_id) {
    ctx.custom.insert("previous_worker".to_string(), prev_worker.clone());
}
```

### Phase 4: Register Strategy

**File:** `crates/orchestrator/src/distribution/mod.rs`

```rust
pub fn create_strategy(name: &str) -> Option<Box<dyn DistributionStrategy>> {
    match name {
        // ... existing strategies ...
        "sticky" => Some(Box::new(StickyStrategy::new())),
        _ => None,
    }
}
```

### Phase 5: Write Tests

**Test Cases:**

1. **test_sticky_prefer_previous_worker_idle**
   - Setup: bead-1 previously assigned to worker-a
   - Worker-a is idle (load = 0.0)
   - Expected: Returns worker-a (STICKY HIT)

2. **test_sticky_fallback_previous_worker_busy**
   - Setup: bead-1 previously assigned to worker-a
   - Worker-a is busy (load = 1.0)
   - Worker-b is idle (load = 0.0)
   - Expected: Returns worker-b (FALLBACK)

3. **test_sticky_fallback_previous_worker_dead**
   - Setup: bead-1 previously assigned to worker-a
   - Worker-a not in agents list
   - Worker-b is idle
   - Expected: Returns worker-b (FALLBACK)

4. **test_sticky_no_previous_assignment**
   - Setup: bead-1 has no previous assignment
   - Multiple idle workers
   - Expected: Returns least-loaded worker

5. **test_sticky_hit_rate_metric**
   - Setup: 100 assignments with idle previous worker
   - Expected: >80 sticky hits

## 6. Acceptance Criteria

### Functional Requirements
- [ ] Returns previous worker when idle
- [ ] Falls back to idle worker when previous worker busy
- [ ] Falls back to idle worker when previous worker dead
- [ ] Returns least-loaded worker when no previous assignment
- [ ] Never returns None when idle workers exist
- [ ] Thread-safe (Send + Sync)

### Non-Functional Requirements
- [ ] Zero `unwrap()` or `expect()` calls
- [ ] Zero `panic!()`, `todo!()`, `unimplemented!()` calls
- [ ] All functions return `Result<T, Error>` or `Option<T>`
- [ ] O(n) time complexity
- [ ] No heap allocations in hot path
- [ ] All tests pass (moon run :test)

### Integration Requirements
- [ ] Strategy registered in `create_strategy()`
- [ ] Exported in `mod.rs`
- [ ] Documented with examples
- [ ] >80% sticky hit rate achieved

## 7. Error Handling

### Potential Failures

1. **No previous worker in context**
   - Action: Treat as new assignment, use load balancing
   - Error: None (not an error condition)

2. **Previous worker not in agents list**
   - Action: Fall back to available agents
   - Error: None (worker may have crashed)

3. **All workers busy**
   - Action: Return None (no workers available)
   - Error: None (expected condition)

4. **Invalid sticky_weight**
   - Action: Clamp to [0.0, 1.0]
   - Error: Return `Err(DistributionError::configuration(...))`

## 8. Test Data

### Scenario 1: Sticky Hit
```
bead_id: "bead-1"
previous_worker: "worker-a"
agents: ["worker-a", "worker-b"]
ctx.custom: {"previous_worker": "worker-a"}
agent_metadata:
  worker-a: load = 0.0
  worker-b: load = 0.5

Expected: "worker-a" (STICKY HIT)
```

### Scenario 2: Fallback (Busy)
```
bead_id: "bead-1"
previous_worker: "worker-a"
agents: ["worker-a", "worker-b"]
ctx.custom: {"previous_worker": "worker-a"}
agent_metadata:
  worker-a: load = 1.0 (busy)
  worker-b: load = 0.0 (idle)

Expected: "worker-b" (FALLBACK - previous worker busy)
```

### Scenario 3: Fallback (Dead)
```
bead_id: "bead-1"
previous_worker: "worker-a"
agents: ["worker-b", "worker-c"]
ctx.custom: {"previous_worker": "worker-a"}
agent_metadata:
  worker-b: load = 0.0
  worker-c: load = 0.5

Expected: "worker-b" (FALLBACK - previous worker not in list)
```

## 9. Validation Gates

### Gate 0: Research
- [x] All existing strategies reviewed
- [x] Integration points identified
- [x] Data flow documented

### Gate 1: Tests First
- [ ] All test cases written and failing
- [ ] Test data prepared
- [ ] Mock infrastructure ready

### Gate 2: Implementation
- [ ] StickyStrategy struct created
- [ ] select_agent() implemented
- [ ] Integration complete
- [ ] All tests passing

### Gate 3: Integration
- [ ] Moon run :ci passes
- [ ] >80% hit rate verified
- [ ] Documentation complete

## 10. Success Metrics

- **Sticky Hit Rate**: >80% (when previous worker is idle)
- **Test Coverage**: 100% of code paths
- **Performance**: O(n) complexity, no heap allocations
- **Quality**: Zero clippy warnings, zero unwrap calls

---

**Contract Status:** READY FOR IMPLEMENTATION  
**Next Stage:** implement (functional-rust-generator)
