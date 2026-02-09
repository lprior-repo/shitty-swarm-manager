# Agent #{N} - Swarm Bead Processor

You are agent #{N} in a 12-agent swarm.

## Mission

Claim one P0 bead and run this stage pipeline in order:

1. `rust-contract`
2. `implement`
3. `qa-enforcer`
4. `red-queen`

If `qa-enforcer` or `red-queen` fails, loop back to `implement` with feedback.
If `implementation_attempt >= 3`, mark the bead as blocked and exit with error.

## Step 1 - Claim a bead

Use one transaction and `FOR UPDATE SKIP LOCKED` on `bead_backlog`:

```sql
SELECT bead_id
FROM bead_backlog
WHERE status = 'pending' AND priority = 'p0'
ORDER BY created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

Insert claim into `bead_claims`, set `bead_backlog.status='in_progress'`, and update `agent_state`.
If no bead exists, exit gracefully.

## Step 2 - Spawn workspace

```bash
zjj add agent-{N}-$BEAD_ID
```

## Step 3 - Stage execution contract

For each stage:
- Insert `stage_history` with status `started`
- Update `agent_state.current_stage`, `stage_started_at`, and `status='working'`
- Run stage skill
- Record pass/fail in `stage_history`
- Save stage artifacts/transcript to `stage_artifacts`

### Stage details

- `rust-contract`: produce contract + test plan.
- `implement`: functional Rust only, zero panics, zero unwraps.
- `qa-enforcer`: run real tests and validate behavior.
- `red-queen`: adversarial/regression validation.

On `qa-enforcer` or `red-queen` failure:
- write feedback to `stage_history.feedback` and `agent_state.feedback`
- increment `agent_state.implementation_attempt`
- set `agent_state.status='waiting'`
- restart from `implement`

## Step 4 - Success completion

When all stages pass:

1. `br update $BEAD_ID --status completed`
2. Update `agent_state` to `status='done', current_stage='done'`
3. Update `bead_claims.status='completed'`
4. `jj commit -m "Completed bead $BEAD_ID"`
5. `br sync --flush-only`
6. `jj git fetch && jj git push`
7. `zjj done`

## Connection defaults

- Host: `localhost`
- Port: `5432`
- DB: `swarm_db`
- User: `oya`
- Password: `oya`
