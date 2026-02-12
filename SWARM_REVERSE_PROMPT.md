# Swarm Architecture Design

## Overview

Distributed parallel agent swarm that processes beads simultaneously using isolated workspaces and a central PostgreSQL coordinator.

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                       │
│              (swarm_db - central coordinator)                │
│  bead_claims │ agent_state │ stage_history │ artifacts      │
└─────────────────────────────────────────────────────────────┘
                            │
    ┌──────────┬──────────┬──────────┬─────────────┐
    │          │          │          │             │
┌───▼───┐ ┌───▼───┐ ┌───▼───┐ ┌────▼────┐   ┌────▼────┐
│Agent 1│ │Agent 2│ │Agent 3│ │Agent 4  │...│Agent N  │
└───┬───┘ └───┬───┘ └───┬───┘ └────┬────┘   └────┬────┘
    └─────────┴─────────┴────────────┴─────────────┘
                         │
              ┌──────────▼──────────┐
              │   4-Stage Pipeline  │
              │  rust-contract →    │
              │  implement →        │
              │  qa-enforcer →      │
              │  red-queen          │
              └─────────────────────┘
```

## Agent Workflow

```
1. CLAIM BEAD     → SELECT ... FOR UPDATE SKIP LOCKED
2. SPAWN WORKSPACE → zjj add agent-{N}-{bead_id}
3. RUN PIPELINE   → rust-contract → implement → qa-enforcer → red-queen
4. ON FAILURE     → Loop back to implement (max 3 attempts)
5. ON SUCCESS     → jj commit → br sync → jj git push → zjj done
```

## Critical Constraints

| Constraint | Enforcement |
|------------|-------------|
| Workspace isolation | `zjj add` for each agent |
| Lock-free claiming | `FOR UPDATE SKIP LOCKED` |
| Full audit trail | `stage_history` table |
| Max 3 retries | Mark bead as blocked after 3 failures |
| Must push | Work not done until `jj git push` succeeds |
| Zero panics | `#![deny(clippy::unwrap_used, clippy::panic)]` |

## Database Tables

| Table | Purpose |
|-------|---------|
| `bead_claims` | Bead ownership + status |
| `agent_state` | Agent lifecycle + retries |
| `stage_history` | Execution audit log |
| `stage_artifacts` | Typed output per stage |
| `agent_messages` | Inter-agent messaging |

## Quick Start

```bash
# 1. Start PostgreSQL
docker run -d --name swarm-db -p 5432:5432 \
  -e POSTGRES_USER=oya -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_DB=swarm_db postgres:16

# 2. Initialize
swarm init-db

# 3. Smoke test
swarm smoke --id 1

# 4. Launch swarm
swarm spawn-prompts --count 12
```
