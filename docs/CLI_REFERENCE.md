# CLI Command Reference

All commands emit JSONL. Parse by keys, not pattern-matching.

## Quick Reference

| Command | Purpose | Next Action |
|---------|---------|-------------|
| `doctor` | Health check | Fix any failures before proceeding |
| `status` | Swarm state | Check `working` count before claiming |
| `init` | Full bootstrap | Run `doctor` to verify |
| `init-db` | Database setup | Run `register` to seed agents |
| `init-local-db` | Local Docker DB | Run `init-db` with new URL |
| `bootstrap` | Repo bootstrap | Run `init-db` next |
| `register` | Seed agents | Check `status` to verify |
| `next` | Top bead rec | Run `claim-next` if available |
| `claim-next` | Claim bead | Run `agent` with returned ID |
| `assign` | Explicit assign | Run `agent` with assigned agent |
| `agent` | Run pipeline | Check `monitor --view progress` |
| `run-once` | Single cycle | Run `status` to see result |
| `smoke` | Smoke test | Fix errors before parallel launch |
| `monitor` | View state | Poll with `watch_ms` for updates |
| `release` | Free agent | Check `status` to confirm |
| `artifacts` | Get outputs | Parse `artifact_type` for stage |
| `resume` | Resumable beads | Run `resume-context` for details |
| `resume-context` | Deep context | Use to reconstruct state |
| `qa` | QA checks | Fix failures, re-run |
| `state` | Full dump | Use for debugging |
| `history` | Event log | Filter by `bead_id` if needed |
| `lock` | Acquire lock | Check `ttl_ms` for expiry |
| `unlock` | Release lock | Verify with `state` |
| `agents` | List agents | Check availability before assign |
| `broadcast` | Send message | Verify with `monitor --view messages` |
| `load-profile` | Simulate load | Check `status` during run |
| `spawn-prompts` | Generate prompts | Launch agents with generated files |
| `prompt` | Get prompt text | Use for agent configuration |
| `batch` | Multi-command | Verify each op result |
| `?` / `help` | Help | Check `examples` for patterns |

---

## Command Details

### Health & Status

#### `doctor`
**Purpose:** Environment health check
**Output:** `checks: [{name, ok, msg}]`
**Next:** Fix any `ok: false` items before proceeding
**Hint:** Always run first in new session. Checks DB, config, toolchain.

#### `status`
**Purpose:** Current swarm state
**Output:** `agents: {idle, working, done}, beads: {pending, in_progress, completed, blocked}`
**Next:** If `idle > 0` and `pending > 0`, run `claim-next`
**Hint:** Snapshot only - for live updates use `monitor`

#### `state`
**Purpose:** Full coordinator state dump
**Output:** All tables, all agents, all claims
**Next:** Use for debugging complex issues
**Hint:** Verbose - use `status` for quick checks

---

### Initialization

#### `init`
**Purpose:** Full bootstrap (bootstrap + init-db + register)
**Args:** `dry`, `database_url`, `schema`, `seed_agents`
**Next:** Run `doctor` to verify setup
**Hint:** One-command setup for fresh environment

#### `bootstrap`
**Purpose:** Bootstrap repository structure
**Args:** `dry`
**Next:** Run `init-db` to create schema
**Hint:** Creates `.swarm/` directory and config

#### `init-db`
**Purpose:** Initialize database schema
**Args:** `url`, `schema`, `seed_agents`, `dry`
**Next:** Run `register` if `seed_agents` not set
**Hint:** Idempotent - safe to re-run

#### `init-local-db`
**Purpose:** Start local Docker PostgreSQL
**Args:** `container_name`, `port`, `user`, `database`, `schema`, `seed_agents`, `dry`
**Next:** Run `init-db` with `--url` pointing to new DB
**Hint:** Creates ephemeral DB for testing

#### `register`
**Purpose:** Seed agent records
**Args:** `count`, `dry`
**Next:** Run `status` to verify agents
**Hint:** Default count from config (usually 12)

---

### Bead Operations

#### `next`
**Purpose:** Get top bead recommendation (no claim)
**Args:** `dry`
**Output:** `bead_id, priority, score, reason`
**Next:** Run `claim-next` to reserve it
**Hint:** Preview only - doesn't modify state

#### `claim-next`
**Purpose:** Atomically claim top available bead
**Args:** `dry`
**Output:** `bead_id, agent_id`
**Next:** Run `agent --id <agent_id>` to process
**Hint:** Returns `null` if no beads/agents available

#### `assign`
**Purpose:** Assign specific bead to specific agent
**Args:** `bead_id`, `agent_id`, `dry`
**Next:** Run `agent --id <agent_id>` to process
**Hint:** Bypasses priority queue - use for explicit routing

#### `release`
**Purpose:** Release agent's claim, free the agent
**Args:** `agent_id`, `dry`
**Next:** Run `status` to confirm release
**Hint:** Use when agent is stuck or bead blocked

#### `artifacts`
**Purpose:** Retrieve stored artifacts for a bead
**Args:** `bead_id`, `artifact_type`
**Output:** Array of `{artifact_type, content, metadata, hash}`
**Next:** Parse `content` based on `artifact_type`
**Hint:** Types: `contract_document`, `test_results`, `stage_log`, `failure_details`

---

### Agent Execution

#### `agent`
**Purpose:** Run single agent through pipeline
**Args:** `id`, `dry`
**Output:** Stage results, final status
**Next:** Check `monitor --view progress` for overall state
**Hint:** Always `--dry` first. Loops on QA failure (max 3 attempts)

#### `run-once`
**Purpose:** Single orchestration cycle (claim → execute)
**Args:** `id`, `dry`
**Next:** Run `status` to see resulting state
**Hint:** Compact alternative to manual claim + agent

#### `smoke`
**Purpose:** Smoke test with single agent
**Args:** `id`, `dry`
**Next:** Fix any errors before `spawn-prompts`
**Hint:** Validates full pipeline end-to-end

---

### Monitoring

#### `monitor`
**Purpose:** Live view of swarm state
**Args:** `view`, `watch_ms`
**Views:** `active`, `progress`, `failures`, `messages`
**Next:** Poll for updates, or use `watch_ms` for streaming
**Hint:** `active` shows working agents; `failures` shows items needing attention

#### `history`
**Purpose:** Event history log
**Args:** `limit`
**Output:** Array of execution events
**Next:** Filter by `bead_id` or `agent_id` for specific items
**Hint:** Audit trail - all stage transitions logged here

#### `agents`
**Purpose:** List all agents with state
**Output:** Array of `{agent_id, status, bead_id, current_stage}`
**Next:** Find `idle` agents before `assign`
**Hint:** Quick availability check

---

### Resumability

#### `resume`
**Purpose:** List resumable beads (in_progress with context)
**Output:** Array of `{bead_id, stage, agent_id, artifacts}`
**Next:** Run `resume-context` for specific bead
**Hint:** Use after crash/restart to find orphaned work

#### `resume-context`
**Purpose:** Deep context for resuming a bead
**Args:** `bead_id`
**Output:** Full state: artifacts, history, feedback
**Next:** Continue from `current_stage` with context
**Hint:** Provides everything needed to resume work

---

### QA & Testing

#### `qa`
**Purpose:** Run deterministic QA checks
**Args:** `target`, `id`, `dry`
**Output:** Pass/fail with details
**Next:** If fail, check `artifacts` for failure details
**Hint:** Lighter than full pipeline - use for validation

---

### Coordination

#### `lock`
**Purpose:** Acquire distributed lock
**Args:** `resource`, `agent`, `ttl_ms`, `dry`
**Next:** Proceed if `ok: true`, else wait or fail
**Hint:** Use for cross-agent coordination

#### `unlock`
**Purpose:** Release distributed lock
**Args:** `resource`, `agent`, `dry`
**Next:** Verify with `state` if needed
**Hint:** Auto-expires after `ttl_ms`

#### `broadcast`
**Purpose:** Send message to all agents
**Args:** `msg`, `from`, `dry`
**Next:** Check `monitor --view messages` for delivery
**Hint:** Use for coordination, not individual replies

---

### Utility

#### `spawn-prompts`
**Purpose:** Generate agent prompt files
**Args:** `template`, `out_dir`, `count`, `dry`
**Output:** Creates `.agents/generated/agent_01.md` ... `agent_N.md`
**Next:** Launch agents with generated prompts
**Hint:** Run after `register` to prepare parallel launch

#### `prompt`
**Purpose:** Get agent/skill prompt text
**Args:** `id`, `skill`
**Output:** Prompt template with placeholders filled
**Next:** Use to configure agent invocation
**Hint:** Useful for understanding expected behavior

#### `load-profile`
**Purpose:** Simulate load for testing
**Args:** `agents`, `rounds`, `timeout_ms`, `dry`
**Next:** Monitor with `status` during run
**Hint:** Use to validate performance under load

#### `batch`
**Purpose:** Execute multiple commands atomically
**Args:** `ops` (array of commands), `dry`
**Output:** Array of results per op
**Next:** Check each result in response array
**Hint:** Use `ops` key, not `cmds`. Stops on first failure.

---

### Help

#### `?` / `help`
**Purpose:** Show command reference
**Output:** Commands, examples, response format
**Next:** Check `examples` for common patterns
**Hint:** Always available - first resort when uncertain

---

## Common Workflows

### Fresh Start
```
doctor → init → doctor → status
```

### Single Agent
```
claim-next → agent --id N → monitor --view progress
```

### Parallel Launch
```
register --count 12 → spawn-prompts --count 12 → [launch agents]
```

### Recovery
```
resume → resume-context --bead-id X → [continue work]
```

### Debug
```
status → history → artifacts --bead-id X → state
```
