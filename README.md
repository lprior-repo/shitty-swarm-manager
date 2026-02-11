# Parallel Agent Swarm for Bead Processing

Spin up multiple parallel agents that each claim a P0 bead and execute:
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
  --name shitty-swarm-manager-db \
  -p 5437:5432 \
  -e POSTGRES_USER=shitty_swarm_manager \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_DB=shitty_swarm_manager_db \
  postgres:16
```

### 2. Initialize Database

```bash
swarm init-db

# Or use native bootstrap + local DB initialization commands
swarm bootstrap
swarm init-local-db

# Optional: target a specific connection/schema
swarm init-db --url "postgresql://shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db" --schema "crates/swarm-coordinator/schema.sql" --seed_agents 12
```

The CLI defaults to loading the canonical coordinator schema at `crates/swarm-coordinator/schema.sql` for `init-db`, `init-local-db`, and other bootstrap actions. Use `--schema` only when a different SQL file is required.

This creates:
- `bead_backlog` (pending/in_progress/completed/blocked queue)
- `bead_claims` (current claim owner + claim status)
- `agent_state` (agent lifecycle + retries + feedback)
- `stage_history` (per-stage execution history)
- `stage_artifacts` (typed artifact storage per stage run)
- `agent_messages` (message passing between stages/agents)
- Monitoring and artifact/message views (`v_active_agents`, `v_swarm_progress`, `v_feedback_required`, `v_bead_artifacts`, `v_unread_messages`)

### 3. Ensure Backlog Beads Exist

```bash
# Check available P0 backlog rows in Postgres
psql -h localhost -p 5437 -U shitty_swarm_manager -d shitty_swarm_manager_db -c "
SELECT bead_id, status, priority, created_at
FROM bead_backlog
WHERE status = 'pending' AND priority = 'p0'
ORDER BY created_at
LIMIT 12;
"

# Seed test backlog rows if empty
psql -h localhost -p 5437 -U shitty_swarm_manager -d shitty_swarm_manager_db -c "
INSERT INTO bead_backlog (bead_id, priority, status)
VALUES ('test-1', 'p0', 'pending'), ('test-2', 'p0', 'pending'), ('test-3', 'p0', 'pending')
ON CONFLICT (bead_id) DO NOTHING;
"
```

### 4. Launch the Swarm

Single-agent smoke check first:

```bash
swarm smoke --id 1
```

**From Claude Code**, spawn agents in parallel (example for 12 agents):

```python
# For each agent 1-N:
Task(
    description="Agent N processing bead",
    prompt="./.agents/agent_N.md",
    subagent_type="general-purpose",
    run_in_background=True
)
```

Or use the prepared launcher:
```bash
swarm spawn-prompts --count 12  # Defaults to max_agents from config
# Uses .agents/agent_prompt.md and writes .agents/generated/agent_01.md ... agent_N.md
```

## AI-Native Operator Guide

This CLI is designed to be machine-operated first and human-operated second.

### Output Contract (Global)

All `swarm` commands emit **single-line JSON output** (JSONL-compatible).
Treat each emitted line as a structured record and parse by keys.

If you are building automations, controllers, or agent workers, use this section as the default contract.

### Design Principles

1. **Deterministic state over hidden behavior**
   - Every meaningful action should map to a visible database transition.
2. **JSONL-native interfaces**
    - Commands emit single-line JSON records by default (JSONL-compatible).
    - Commands return a single JSON object per line.
3. **Safe-by-default execution**
    - Use `--dry` before side-effecting commands when running in unknown environments.
4. **Resumability and auditability**
   - Stage outcomes and artifacts must be queryable after failure.

### Minimal Reliable Agent Loop

Use this baseline flow for robust autonomous execution:

```bash
# 1) Environment sanity
swarm doctor

# 2) Optional preflight plan (no side effects)
swarm agent --id 1 --dry

# 3) Real execution
swarm agent --id 1

# 4) Verification
swarm status
swarm monitor --view progress
```

### Operator Patterns by Phase

**Bootstrapping a fresh environment**

```bash
swarm init-db
swarm register --count N
swarm spawn-prompts --count N
```

**Smoke test before parallel fan-out**

```bash
swarm smoke --id 1
swarm monitor --view active
```

**During active swarm execution**

```bash
swarm monitor --view progress
```

**Recovery / intervention**

```bash
swarm release --agent_id 3
swarm status
```

### Artifact retrieval command

Use `swarm artifacts --bead-id <bead-id> [--artifact-type <type>]` to list persistent artifacts for a bead, including content, metadata, and hashes. Filter by typed artifact values such as `contract_document`, `test_results`, `failure_details`, `stage_log`, or other values documented in `ArtifactType`.


### AI Integration Rules

- Treat command output as API responses, not logs.
- Parse keys, do not pattern-match free text.
- Retry based on explicit failure state, not assumptions.
- Never infer completion without checking `status`/monitor views.
- Prefer idempotent checks (`doctor`, `status`, `monitor`) between mutations.
- Default output is already machine-readable JSONL-style single-line JSON; keep parsing strict.

### First Invocation Handshake (Agent Boot Sequence)

When an AI agent is invoked for the first time in a session, run this exact sequence before any mutating command.

Output expectation for every command below: single-line JSON by default.

```bash
# 1) Confirm toolchain and environment health
echo '{"cmd":"doctor"}' | swarm

# 2) Confirm reachable coordinator state
echo '{"cmd":"status"}' | swarm
echo '{"cmd":"monitor","view":"active"}' | swarm

# 3) Preview intended action without side effects
echo '{"cmd":"agent","id":1,"dry":true}' | swarm

# 4) Execute for real only after successful dry-run
echo '{"cmd":"agent","id":1}' | swarm

# 5) Verify postconditions
echo '{"cmd":"monitor","view":"progress"}' | swarm
echo '{"cmd":"monitor","view":"failures"}' | swarm
```

Interpretation rules for this boot sequence:
- If `doctor` reports unhealthy checks, stop and fix those checks first.
- If `dry_run` payload differs from intended action, do not proceed until corrected.
- If post-run failures exist, loop through retry workflow rather than declaring success.

### Strong Recommendation for Prompt Templates

Use `.agents/agent_prompt.md` as the canonical template for generated agents.
It is written to be explicit for low-context agents and includes:
- deterministic stage order
- retry behavior
- completion criteria
- machine-friendly reporting style

Generate fresh prompts whenever process rules change:

```bash
echo '{"cmd":"spawn-prompts","template":".agents/agent_prompt.md","out_dir":".agents/generated","count":N}' | swarm
```

### Native Rust Operations (Protocol Commands)

All operational helpers are first-class `swarm` protocol commands:

- `{"cmd":"init-local-db"}`
- `{"cmd":"monitor","view":"active","watch_ms":2000}`
- `{"cmd":"monitor","view":"progress"}`
- `{"cmd":"monitor","view":"failures"}`
- `{"cmd":"monitor","view":"messages"}`
- `{"cmd":"spawn-prompts","count":N}`

## Monitoring

Output contract for this section: each command defaults to single-line JSON (JSONL-compatible).

### Check Active Agents

```bash
echo '{"cmd":"monitor","view":"active"}' | swarm
echo '{"cmd":"monitor","view":"active","watch_ms":2000}' | swarm
```

### Check Progress

```bash
echo '{"cmd":"monitor","view":"progress"}' | swarm
```

### View Failures Requiring Feedback

```bash
echo '{"cmd":"monitor","view":"failures"}' | swarm
```

### View Unread Inter-Agent Messages

```bash
echo '{"cmd":"monitor","view":"messages"}' | swarm
```

### Get Specific Agent State

```sql
psql -h localhost -U shitty_swarm_manager -d shitty_swarm_manager_db -c "
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
psql -h localhost -U shitty_swarm_manager -d shitty_swarm_manager_db -c "
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

### DB Sanity Check (end-to-end)

After a smoke run:

```bash
echo '{"cmd":"smoke","id":1}' | swarm
```

Run these checks to verify claims, stage history, artifacts, and messages:

```sql
-- 1) Agent and claim state
SELECT agent_id, bead_id, current_stage, status, implementation_attempt
FROM agent_state
WHERE agent_id = 1;

SELECT bead_id, claimed_by, status, claimed_at
FROM bead_claims
ORDER BY claimed_at DESC
LIMIT 5;

-- 2) Stage history for recent bead(s)
SELECT bead_id, stage, attempt_number, status, started_at, completed_at
FROM stage_history
ORDER BY started_at DESC
LIMIT 20;

-- 3) Stored stage artifacts
SELECT bead_id, stage, artifact_type, LENGTH(content) AS bytes, created_at
FROM v_bead_artifacts
ORDER BY created_at DESC
LIMIT 30;

-- 4) Unread inter-agent messages
SELECT id, from_agent_id, to_agent_id, bead_id, message_type, subject, created_at
FROM v_unread_messages
ORDER BY created_at DESC
LIMIT 20;
```

Expected outcomes:
- `stage_history` has one row per executed stage attempt.
- `v_bead_artifacts` contains at least `stage_log` plus stage-specific artifacts.
- `v_unread_messages` shows coordination messages (`contract_ready`, `implementation_ready`, etc.) until read.
- `agent_state` and `bead_claims` reflect the latest stage/ownership transitions.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     PostgreSQL Database                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ bead_claims  │  │ agent_state  │  │  stage_history       │  │
│  │ (M beads)    │  │ (N agents)   │  │  (audit log)         │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
          ▲                    ▲                      ▲
          │                    │                      │
          └────────────────────┴──────────────────────┘
                            │
          ┌─────────────────┴─────────────────┐
          │                                   │
     ┌────▼────┐  ┌────▼────┐      ┌────▼────┐
     │ Agent 1 │  │ Agent 2 │  ...  │ Agent N │
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

## PostgreSQL Schema

Key tables:
- `bead_backlog`: queue of beads eligible for claiming
- `bead_claims`: bead_id, claimed_by, claimed_at, status
- `agent_state`: agent_id, bead_id, current_stage, status, implementation_attempt, feedback
- `stage_history`: agent_id, bead_id, stage, attempt_number, status, feedback, timestamps
- `stage_artifacts`: stage_history_id, artifact_type, content, metadata, content_hash
- `agent_messages`: from/to agent routing, message_type, subject/body, read status
- `swarm_config`: max_agents=N, max_implementation_attempts=3, claim_label=p0

Core DB functions:
- `claim_next_bead(agent_id)`
- `store_stage_artifact(stage_history_id, artifact_type, content, metadata)`
- `send_agent_message(from_repo_id, from_agent_id, to_repo_id, to_agent_id, bead_id, message_type, subject, body, metadata)`
- `get_unread_messages(repo_id, agent_id, bead_id)`
- `mark_messages_read(repo_id, agent_id, message_ids)`

## Configuration

```bash
# Runtime DB URL (used by swarm commands)
DATABASE_URL=postgresql://shitty_swarm_manager@localhost:5432/shitty_swarm_manager_db

# Optional: dedicated test DB URL for ignored DB integration tests
SWARM_TEST_DATABASE_URL=postgresql://shitty_swarm_manager@localhost:5432/shitty_swarm_manager_test_db

# One-command ephemeral DB + Flyway migrate/seed + ignored DB tests
moon run :db-test-full
```

Or set `.swarm/config.toml`:

```toml
database_url = "postgresql://shitty_swarm_manager@localhost:5432/shitty_swarm_manager_db"
rust_contract_cmd = "br show {bead_id}"
implement_cmd = "jj status"
qa_enforcer_cmd = "moon run :quick"
red_queen_cmd = "moon run :test"
```

Protocol DB override options (available on all commands):

```bash
echo '{"cmd":"status","database_url":"postgresql://shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db"}' | swarm
```

## Troubleshooting

### Database connection failed
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Check database exists
psql -h localhost -U shitty_swarm_manager -d postgres -c "\l" | grep shitty_swarm_manager_db

# Recreate schema if needed
echo '{"cmd":"init-db","url":"postgresql://shitty_swarm_manager@localhost:5432/shitty_swarm_manager_db"}' | swarm
```

### No beads available
```bash
# Check Postgres backlog has pending p0 beads
psql -h localhost -U shitty_swarm_manager -d shitty_swarm_manager_db -c "
SELECT COUNT(*)
FROM bead_backlog
WHERE status = 'pending' AND priority = 'p0';
"

# Add test beads if needed
psql -h localhost -U shitty_swarm_manager -d shitty_swarm_manager_db -c "
INSERT INTO bead_backlog (bead_id, priority, status)
SELECT format('test-%s', g), 'p0', 'pending'
FROM generate_series(1, 10) AS g
ON CONFLICT (bead_id) DO NOTHING;
"
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
psql -h localhost -U shitty_swarm_manager -d postgres -c "DROP DATABASE IF EXISTS shitty_swarm_manager_db;"

# Or reset for next run
psql -h localhost -U shitty_swarm_manager -d shitty_swarm_manager_db -c "
TRUNCATE agent_messages, stage_artifacts, stage_history, bead_claims, bead_backlog RESTART IDENTITY;
UPDATE agent_state
SET bead_id = NULL,
    current_stage = NULL,
    stage_started_at = NULL,
    status = 'idle',
    feedback = NULL,
    implementation_attempt = 0,
    last_update = NOW();
"

# Clean up workspaces
zjj status  # List workspaces
# Manually remove completed workspaces
```
