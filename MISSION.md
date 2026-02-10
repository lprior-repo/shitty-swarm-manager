# Mission: Parallel Swarm Coordination

You are an autonomous agent operating within the **shitty-swarm-manager** ecosystem. Your mission is to process work units (beads) through a rigorous 4-stage pipeline while maintaining perfect state synchronization with a central PostgreSQL coordinator.

## The Swarm Architecture

- **Coordinator**: A PostgreSQL database (`swarm_db`) that tracks all agent states, bead claims, and execution history.
- **Agents**: Parallel workers (numbered 1 to N) that claim beads and execute the pipeline.
- **Isolation**: Each agent must work in a dedicated `zjj` (Jujutsu + Zellij) workspace to prevent state pollution.

## Operating Principles for AI Agents

### 1. Context Hygiene & Window Management
To keep your context window clean and stay efficient:
- **Offload State**: Store all artifacts (contracts, logs, test results) in the PostgreSQL database using `swarm` CLI commands.
- **Don't Hallucinate State**: Always query `swarm status` or `swarm monitor` to understand the current swarm health.
- **Atomic Operations**: Perform one stage at a time, update the DB, and only then proceed.

### 2. The Ergonomic CLI (`swarm`)
The `swarm` tool is your primary interface. It emits machine-readable JSONL.
- **Discovery**: `swarm ?` or `swarm --help`
- **Sanity Check**: `swarm doctor` (Always run this first!)
- **State Check**: `swarm status`
- **Execution**: `swarm agent --id {N}`

### 3. The 4-Stage Pipeline
Every bead must pass through these stages in order:

1.  **rust-contract**: Design-by-contract analysis. Invariants and test plans.
2.  **implement**: Functional Rust implementation (Zero panics, zero unwraps).
3.  **qa-enforcer**: Execution of actual tests. Deep inspection.
4.  **red-queen**: Adversarial evolutionary QA and regression hunting.

**Failure Handling**: If QA or Red-Queen fails, collect feedback, increment the `implementation_attempt` counter in the DB, and loop back to the **implement** stage. Max 3 attempts per bead.

## Workflow Walkthrough

```bash
# 1. Verify environment
swarm doctor

# 2. Check swarm state
swarm status

# 3. Dry run to see what will happen
swarm agent --id 1 --dry

# 4. Execute the pipeline
swarm agent --id 1

# 5. Monitor progress
swarm monitor --view progress
```

## Database Schema Highlights

- `agent_state`: Your current mission status.
- `bead_claims`: Who owns which bead.
- `stage_history`: Audit log of every attempt.
- `stage_artifacts`: Where your contracts and logs are stored for resumability.

## Landing the Plane
Work is NOT complete until:
1. The bead is marked `completed` in the DB.
2. `br sync --flush-only` is run to sync bead metadata.
3. `jj git push` succeeds to the remote repository.

**Stay focused. Stay isolated. Stay deterministic.**
