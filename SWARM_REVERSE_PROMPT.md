# Reverse Prompt: Build 12-Agent Parallel Bead Processing Swarm

You are building a **distributed parallel agent swarm** that processes 12 beads simultaneously using isolated workspaces and a central coordinator database.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                       │
│              (swarm_db - central coordinator)                │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ bead_claims  │  │ agent_state  │  │ stage_history    │  │
│  │              │  │ (12 agents)  │  │ (audit log)      │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
          ▲                    ▲                   ▲
          │                    │                   │
          └────────────────────┴───────────────────┘
                               │
    ┌──────────┬──────────┬──────────┬─────────────┐
    │          │          │          │             │
┌───▼───┐ ┌───▼───┐ ┌───▼───┐ ┌────▼────┐   ┌────▼────┐
│Agent 1│ │Agent 2│ │Agent 3│ │Agent 4  │...│Agent 12 │
└───┬───┘ └───┬───┘ └───┬───┘ └────┬────┘   └────┬────┘
    │         │         │            │             │
    │         │         │            │             │
    └─────────┴─────────┴────────────┴─────────────┘
                         │
              ┌──────────▼──────────┐
              │   Each Agent Runs:  │
              │                     │
              │ 1. rust-contract    │
              │ 2. implement        │
              │ 3. qa-enforcer      │
              │ 4. red-queen        │
              │                     │
              │ Loop on failure:    │
              │ qa/red-queen →      │
              │ implement (retry)   │
              └─────────────────────┘
```

## What You Need to Build

### 1. PostgreSQL Database Schema

**Location**: `crates/swarm-coordinator/schema.sql`

**Tables**:

```sql
-- Bead claims: Tracks which beads are claimed by which agents
CREATE TABLE bead_claims (
    bead_id TEXT PRIMARY KEY,
    claimed_by SMALLINT CHECK (claimed_by BETWEEN 1 AND 12),
    claimed_at TIMESTAMPTZ DEFAULT NOW(),
    status TEXT CHECK (status IN ('in_progress', 'completed', 'blocked'))
);

-- Agent state: Current state of each of the 12 agents
CREATE TABLE agent_state (
    agent_id SMALLINT PRIMARY KEY CHECK (agent_id BETWEEN 1 AND 12),
    bead_id TEXT REFERENCES bead_claims(bead_id),
    current_stage TEXT CHECK (current_stage IN ('rust-contract', 'implement', 'qa-enforcer', 'red-queen', 'done')),
    stage_started_at TIMESTAMPTZ,
    status TEXT CHECK (status IN ('idle', 'working', 'waiting', 'error', 'done')),
    last_update TIMESTAMPTZ DEFAULT NOW(),
    implementation_attempt INTEGER DEFAULT 0,
    feedback TEXT
);

-- Stage history: Audit log of all stage executions
CREATE TABLE stage_history (
    id BIGSERIAL PRIMARY KEY,
    agent_id SMALLINT CHECK (agent_id BETWEEN 1 AND 12),
    bead_id TEXT NOT NULL,
    stage TEXT CHECK (stage IN ('rust-contract', 'implement', 'qa-enforcer', 'red-queen')),
    attempt_number INTEGER NOT NULL,
    status TEXT CHECK (status IN ('started', 'passed', 'failed', 'error')),
    result TEXT,
    feedback TEXT,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms INTEGER
);

-- Views for monitoring
CREATE VIEW v_active_agents AS [...] -- Agents currently working
CREATE VIEW v_swarm_progress AS [...] -- Progress summary
CREATE VIEW v_feedback_required AS [...] -- Failed stages needing attention
```

**Key features**:
- `FOR UPDATE SKIP LOCKED` for lock-free bead claiming
- Trigger for auto-updating `last_update` timestamp
- Indexes for fast queries

### 2. Database Initialization Script

**Location**: `.agents/init_postgres_swarm.sh`

```bash
#!/usr/bin/env bash
# Start PostgreSQL (Docker or system)
# Create database if not exists
# Load schema
# Insert 12 idle agents into agent_state table
# Display connection info
```

### 3. Agent Prompt Template

**Location**: `.agents/agent_prompt.md`

This is the **prompt for each agent**. Use `{N}` as placeholder for agent number (1-12).

**Agent workflow** (THIS IS CRITICAL):

```
1. CLAIM BEAD (Transaction)
   - Connect to PostgreSQL
   - Execute: SELECT bead_id FROM beads
              WHERE status='pending' AND priority='p0'
              AND id NOT IN (SELECT bead_id FROM bead_claims WHERE status='in_progress')
              ORDER BY created_at ASC
              LIMIT 1
              FOR UPDATE SKIP LOCKED
   - INSERT INTO bead_claims (bead_id, claimed_by, status)
   - If no beads available, exit gracefully

2. SPAWN WORKSPACE
   - zjj add agent-{N}-{bead_id}
   - Now working in isolated JJ workspace + Zellij tab

3. RUN PIPELINE STAGES (in order)

   ┌─────────────────────────────────────────────┐
   │ STAGE 1: rust-contract                     │
   │ Skill: rust-contract                        │
   │ Input: Bead ID from .beads/beads.db        │
   │ Output: Contract document                   │
   │                                             │
   │ Update DB: INSERT INTO stage_history...     │
   │          UPDATE agent_state SET stage=...   │
   └─────────────────────────────────────────────┘
                        │
                        ▼
   ┌─────────────────────────────────────────────┐
   │ STAGE 2: implement                         │
   │ Skill: functional-rust-generator            │
   │ Input: Contract document                    │
   │ Output: Rust code (zero panics, zero unwrap)│
   │                                             │
   │ Update DB: Track implementation_attempt    │
   └─────────────────────────────────────────────┘
                        │
                        ▼
   ┌─────────────────────────────────────────────┐
   │ STAGE 3: qa-enforcer                       │
   │ Skill: qa-enforcer                          │
   │ Input: Implementation                       │
   │ Action: Execute tests, verify behavior      │
   │                                             │
   │ IF FAILS:                                   │
   │   - Log feedback to stage_history           │
   │   - Update agent_state.feedback             │
   │   - Loop back to STAGE 2                    │
   │   - Increment implementation_attempt        │
   │                                             │
   │ IF PASSES: → STAGE 4                        │
   └─────────────────────────────────────────────┘
                        │
                        ▼
   ┌─────────────────────────────────────────────┐
   │ STAGE 4: red-queen                         │
   │ Skill: red-queen                            │
   │ Input: Tested implementation                │
   │ Action: Adversarial QA, regression hunt     │
   │                                             │
   │ IF FAILS:                                   │
   │   - Log feedback to stage_history           │
   │   - Update agent_state.feedback             │
   │   - Loop back to STAGE 2                    │
   │   - Increment implementation_attempt        │
   │                                             │
   │ IF PASSES: → SUCCESS                        │
   └─────────────────────────────────────────────┘
                        │
                        ▼
4. SUCCESS
   - br update {bead_id} --status completed
   - UPDATE agent_state SET status='done', stage='done'
   - UPDATE bead_claims SET status='completed'
   - jj commit -m "Completed bead {bead_id}"
   - br sync --flush-only
   - git add .beads/ && git commit -m "sync beads"
   - jj git fetch && jj git push
   - zjj done
   - Exit successfully

5. MAX RETRY EXCEEDED
   - If implementation_attempt >= 3:
     - Mark bead as 'blocked'
     - Update agent_state to 'idle'
     - Exit with error
```

### 4. Swarm Launcher

**Location**: `.agents/spawn_swarm.py` (or bash script)

Generate 12 Task tool calls, one for each agent:

```python
for agent_id in 1..12:
    prompt = load_template("agent_prompt.md").replace("{N}", agent_id)

    Task(
        description=f"Agent {agent_id} process bead through pipeline",
        prompt=prompt,
        subagent_type="general-purpose",
        run_in_background=True,
        max_turns=50
    )
```

### 5. Monitoring Scripts

Create convenience scripts for monitoring:

```bash
# .agents/monitor.sh
watch -n 2 'psql -h localhost -U oya -d swarm_db -c "SELECT * FROM v_active_agents;"'

# .agents/progress.sh
psql -h localhost -U oya -d swarm_db -c "SELECT * FROM v_swarm_progress;"

# .agents/failures.sh
psql -h localhost -U oya -d swarm_db -c "SELECT * FROM v_feedback_required;"
```

## Critical Constraints

1. **Each agent works in isolation** via `zjj add` - no shared state pollution
2. **Lock-free bead claiming** via `FOR UPDATE SKIP LOCKED` - no two agents claim the same bead
3. **Full audit trail** in `stage_history` - every stage execution logged
4. **Loop on failure** - qa/red-queen failure → back to implement (not contract)
5. **Max 3 implementation attempts** - then mark bead as blocked
6. **Work not done until pushed** - `jj git push` must succeed before marking done
7. **Functional Rust only** - zero panics, zero unwraps, Railway-Oriented Programming

## Database Connection

```
Host: localhost
Port: 5432
Database: swarm_db
User: oya
Password: oya
```

## Deliverables

1. ✅ PostgreSQL schema with all tables, indexes, views, triggers
2. ✅ Database initialization script
3. ✅ Agent prompt template (with {N} placeholder)
4. ✅ Script to spawn 12 agents via Task tool
5. ✅ Monitoring scripts
6. ✅ README with quick start guide

## Starting PostgreSQL

```bash
# Docker (recommended)
docker run -d \
  --name oya-swarm-db \
  -p 5432:5432 \
  -e POSTGRES_USER=oya \
  -e POSTGRES_PASSWORD=oya \
  -e POSTGRES_DB=swarm_db \
  postgres:16

# Or system PostgreSQL
sudo systemctl start postgresql
sudo -u postgres createuser -s $USER
createdb swarm_db
```

## Testing the Swarm

Before launching all 12 agents, test with 1 agent:

1. Initialize database: `.agents/init_postgres_swarm.sh`
2. Ensure beads exist: `br list --status pending --priority p0`
3. Launch agent 1 only (foreground mode)
4. Monitor: `watch .agents/progress.sh`
5. Verify: bead claimed → stages executed → completed

If test passes, launch all 12 in parallel.

## What Makes This Work

- **PostgreSQL SKIP LOCKED**: Allows 12 concurrent transactions to each grab a different row without contention
- **zjj workspace isolation**: Each agent in its own JJ workspace, no git conflicts
- **Centralized state**: Database is single source of truth for all 12 agents
- **Audit trail**: Full history in stage_history for debugging and accountability
- **Graceful failure**: Max retry prevents infinite loops, blocking allows manual intervention

---

Now build this system. Start with the database schema, then the initialization script, then the agent prompt template. Test with 1 agent before scaling to 12.
