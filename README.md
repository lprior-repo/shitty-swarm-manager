# 12-Agent Parallel Swarm for Bead Processing

Spin up 12 parallel agents that each claim a P0 bead and execute:
```
rust-contract → implement → qa-enforcer → red-queen
                      ↑              ↓
                      └── FAIL: feedback ──┘
```

## Quick Start

### 1. Start PostgreSQL

```bash
# Option A: System PostgreSQL
sudo systemctl start postgresql
sudo -u postgres createuser -s $USER
createdb swarm_db

# Option B: Docker (recommended for isolation)
docker run -d \
  --name oya-swarm-db \
  -p 5432:5432 \
  -e POSTGRES_USER=oya \
  -e POSTGRES_PASSWORD=oya \
  -e POSTGRES_DB=swarm_db \
  postgres:16
```

### 2. Initialize Database

```bash
cd /home/lewis/src/oya
.agents/init_postgres_swarm.sh
```

This creates:
- `bead_claims` table (tracks bead assignments)
- `agent_state` table (12 agents, idle state)
- `stage_history` table (audit log)
- Views for monitoring

### 3. Ensure Beads Exist

```bash
# Check available beads
br list --status pending --priority p0

# Or query directly
sqlite3 .beads/beads.db "SELECT id, title FROM beads WHERE status = 'pending' AND priority = 'p0' LIMIT 12;"
```

### 4. Launch the Swarm

**From Claude Code**, spawn 12 agents in parallel:

```python
# For each agent 1-12:
Task(
    description="Agent N processing bead",
    prompt="./.agents/agent_N.md",
    subagent_type="general-purpose",
    run_in_background=True
)
```

Or use the prepared launcher:
```bash
.agents/launch_swarm.sh
# Then manually launch each agent using Task tool
```

## Monitoring

### Check Active Agents

```sql
psql -h localhost -U oya -d swarm_db -c "SELECT * FROM v_active_agents;"
```

### Check Progress

```sql
psql -h localhost -U oya -d swarm_db -c "SELECT * FROM v_swarm_progress;"
```

### View Failures Requiring Feedback

```sql
psql -h localhost -U oya -d swarm_db -c "SELECT * FROM v_feedback_required;"
```

### Get Specific Agent State

```sql
psql -h localhost -U oya -d swarm_db -c "
SELECT
    agent_id,
    bead_id,
    current_stage,
    status,
    implementation_attempt,
    feedback
FROM agent_state
WHERE agent_id = 1;
"
```

### View Stage History for a Bead

```sql
psql -h localhost -U oya -d swarm_db -c "
SELECT
    stage,
    attempt_number,
    status,
    feedback,
    started_at,
    completed_at
FROM stage_history
WHERE bead_id = 'your-bead-id'
ORDER BY started_at;
"
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PostgreSQL Database                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ bead_claims  │  │ agent_state  │  │  stage_history       │  │
│  │ (12 beads)   │  │ (12 agents)  │  │  (audit log)         │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
          ▲                    ▲                      ▲
          │                    │                      │
          └────────────────────┴──────────────────────┘
                            │
          ┌─────────────────┴─────────────────┐
          │                                   │
     ┌────▼────┐  ┌────▼────┐      ┌────▼────┐
     │ Agent 1 │  │ Agent 2 │  ...  │Agent 12 │
     └────┬────┘  └────┬────┘      └────┬────┘
          │            │                 │
          └────────────┴─────────────────┘
                     │
          ┌──────────▼──────────┐
          │   Skills (in order) │
          │                     │
          │ 1. rust-contract    │
          │ 2. functional-rust  │
          │ 3. qa-enforcer      │
          │ 4. red-queen        │
          └─────────────────────┘
```

## Agent Workflow

Each agent:

1. **Claims a bead** (transactional, no two agents claim the same bead)
2. **Spawns zjj workspace** (`zjj add agent-N-{bead_id}`)
3. **Runs rust-contract** → creates contract document
4. **Runs implement** → creates functional Rust code
5. **Runs qa-enforcer** → executes tests
   - If fail → loop back to step 4 with feedback
6. **Runs red-queen** → adversarial QA
   - If fail → loop back to step 4 with feedback
7. **On success** → commits, pushes, marks bead complete, `zjj done`

## Failure Handling

- **Max 3 implementation attempts** per bead
- After 3 failed attempts, bead marked as `blocked`
- Feedback logged to `stage_history` table
- Agent moves to next available bead (if any)

## Database Schema

Key tables:
- `bead_claims`: bead_id, claimed_by, claimed_at, status
- `agent_state`: agent_id, bead_id, current_stage, status, implementation_attempt, feedback
- `stage_history`: agent_id, bead_id, stage, attempt_number, status, feedback, timestamps
- `pipeline_config`: max_agents=12, max_implementation_attempts=3, claim_label=p0

## Environment Variables

```bash
export SWARM_DB=swarm_db      # Database name
export SWARM_USER=oya         # Database user
export SWARM_HOST=localhost   # Database host
export SWARM_PORT=5432        # Database port
```

## Troubleshooting

### Database connection failed
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Check database exists
psql -h localhost -U oya -d postgres -c "\l" | grep swarm_db
```

### No beads available
```bash
# Check beads exist
sqlite3 .beads/beads.db "SELECT COUNT(*) FROM beads WHERE status = 'pending' AND priority = 'p0';"

# Add test beads if needed
br new --slug test-bead-{1..12} --priority p0
```

### Agent stuck in error state
```sql
-- Reset agent to idle
UPDATE agent_state
SET status = 'idle',
    bead_id = NULL,
    current_stage = NULL,
    feedback = NULL,
    implementation_attempt = 0,
    last_update = NOW()
WHERE agent_id = <agent_id>;

-- Release bead claim
UPDATE bead_claims
SET status = 'blocked'
WHERE bead_id = '<bead_id>';
```

## Cleanup

```bash
# Stop agents (Ctrl+C or kill Task processes)

# Drop database
psql -h localhost -U oya -d postgres -c "DROP DATABASE IF EXISTS swarm_db;"

# Or reset for next run
psql -h localhost -U oya -d swarm_db -c "
TRUNCATE bead_claims, stage_history;
UPDATE agent_state SET bead_id = NULL, current_stage = NULL, status = 'idle', feedback = NULL, implementation_attempt = 0;
"

# Clean up workspaces
zjj status  # List workspaces
# Manually remove completed workspaces
```
