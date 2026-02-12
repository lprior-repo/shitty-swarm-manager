# shitty-swarm-manager

**PostgreSQL-based parallel agent coordination for bead processing.**

```
┌─────────────────────────────────────────────────────────────────┐
│                     PostgreSQL Database                          │
│     bead_claims │ agent_state │ stage_history │ artifacts       │
└─────────────────────────────────────────────────────────────────┘
                            │
     ┌──────────────────────┼──────────────────────┐
     │                      │                      │
┌────▼────┐           ┌────▼────┐           ┌────▼────┐
│ Agent 1 │           │ Agent 2 │     ...   │ Agent N │
└────┬────┘           └────┬────┘           └────┬────┘
     └──────────────────────┼──────────────────────┘
                            │
              ┌─────────────▼─────────────┐
              │   4-Stage Pipeline        │
              │  rust-contract → implement│
              │  → qa-enforcer → red-queen│
              └───────────────────────────┘
```

**Features:** Transactional claiming • Resumable execution • Built-in retry (max 3) • JSONL-native CLI

---

## Quick Start

```bash
# 1. Start PostgreSQL
docker run -d --name swarm-db -p 5437:5432 \
  -e POSTGRES_USER=shitty_swarm_manager \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_DB=shitty_swarm_manager_db \
  postgres:16

# 2. Initialize
swarm init-db

# 3. Smoke test
swarm smoke --id 1

# 4. Launch swarm
swarm spawn-prompts --count 12
```

---

## AI-Native Operator Guide

**Output:** All commands emit single-line JSON (JSONL-compatible).

### Minimal Agent Loop

```bash
swarm doctor                    # Environment sanity
swarm agent --id 1 --dry        # Preview (no side effects)
swarm agent --id 1              # Execute
swarm status                    # Verify
```

### Design Principles

1. **Deterministic state** - Every action maps to database transition
2. **JSONL-native** - Single JSON object per line
3. **Safe-by-default** - Use `--dry` before mutations
4. **Auditable** - All outcomes queryable after failure

### AI Integration Rules

- Parse JSON keys, don't pattern-match text
- Retry on explicit failure state only
- Check `status`/`monitor` before declaring success
- Use idempotent checks between mutations

---

## Monitoring

```bash
swarm monitor --view active     # Active agents
swarm monitor --view progress   # Progress summary
swarm monitor --view failures   # Failed stages
swarm monitor --view messages   # Inter-agent messages
```

---

## Architecture

| Table | Purpose |
|-------|---------|
| `bead_backlog` | Queue (pending/in_progress/completed/blocked) |
| `bead_claims` | Current claim owner + status |
| `agent_state` | Agent lifecycle + retries + feedback |
| `stage_history` | Execution audit log |
| `stage_artifacts` | Typed output per stage |
| `agent_messages` | Inter-agent messaging |

**Pipeline:** `rust-contract → implement → qa-enforcer → red-queen`

**Failure:** Max 3 attempts, then mark bead as `blocked`

**DDD Docs:** [docs/BOUNDED_CONTEXTS.md](docs/BOUNDED_CONTEXTS.md)

---

## Configuration

```bash
# Environment
DATABASE_URL=postgresql://shitty_swarm_manager@localhost:5432/shitty_swarm_manager_db

# Or .swarm/config.toml
database_url = "postgresql://shitty_swarm_manager@localhost:5432/shitty_swarm_manager_db"
rust_contract_cmd = "br show {bead_id}"
implement_cmd = "jj status"
qa_enforcer_cmd = "moon run :quick"
red_queen_cmd = "moon run :test"
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| DB connection failed | `pg_isready -h localhost -p 5432` |
| No beads available | Insert into `bead_backlog` table |
| Agent stuck | `UPDATE agent_state SET status='idle' WHERE agent_id=N` |

---

## Cleanup

```bash
# Reset database
psql -d shitty_swarm_manager_db -c "TRUNCATE agent_messages, stage_artifacts, stage_history, bead_claims, bead_backlog RESTART IDENTITY;"

# Reset agents
psql -d shitty_swarm_manager_db -c "UPDATE agent_state SET bead_id=NULL, current_stage=NULL, status='idle', implementation_attempt=0;"
```
