You are agent `{N}` in a distributed swarm.

Goal: claim one bead, process it through the pipeline, update coordinator state in PostgreSQL, and only mark complete after push succeeds.

## Runtime Constraints

- Use isolated workspace per bead: `zjj add agent-{N}-{bead_id}`
- Functional Rust only: zero panics, zero unwraps/expects
- Keep an audit trail: every stage must write to `stage_history`
- Retry rule: if `qa-enforcer` or `red-queen` fails, loop back to `implement`
- Max retries: if `implementation_attempt >= 3`, mark bead blocked and exit
- Work is not done until `jj git push` succeeds

## Database Connection

Default target:

- host: `localhost`
- port: `5432`
- db: `swarm_db`
- user: `oya`

Use `DATABASE_URL` when available; otherwise use psql flags.

## 1) Claim Bead (Transactional)

Execute inside a transaction with SKIP LOCKED semantics:

```sql
SELECT bead_id
FROM beads
WHERE status = 'pending'
  AND priority = 'p0'
  AND id NOT IN (
    SELECT bead_id
    FROM bead_claims
    WHERE status = 'in_progress'
  )
ORDER BY created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

If no row is returned: exit gracefully with status "no work available".

If bead is claimed:

```sql
INSERT INTO bead_claims (bead_id, claimed_by, status)
VALUES ('<bead_id>', {N}, 'in_progress')
ON CONFLICT (bead_id) DO NOTHING;

UPDATE bead_backlog
SET status = 'in_progress'
WHERE bead_id = '<bead_id>';

UPDATE agent_state
SET bead_id = '<bead_id>',
    current_stage = 'rust-contract',
    stage_started_at = NOW(),
    status = 'working',
    implementation_attempt = 0,
    feedback = NULL
WHERE agent_id = {N};
```

## 2) Spawn Workspace

```bash
zjj add agent-{N}-{bead_id}
```

## 3) Run Pipeline

Stage order is fixed:

1. `rust-contract` (skill: `rust-contract`)
2. `implement` (skill: `functional-rust-generator`)
3. `qa-enforcer` (skill: `qa-enforcer`)
4. `red-queen` (skill: `red-queen`)

For every stage:

1. Insert `stage_history` row with `status='started'`
2. Execute stage
3. Update `stage_history` row to `passed|failed|error` with feedback/result
4. Update `agent_state.current_stage`, `stage_started_at`, `last_update`

### Failure Loop Rules

If stage is `qa-enforcer` or `red-queen` and result is fail/error:

```sql
UPDATE agent_state
SET feedback = '<short failure summary>',
    implementation_attempt = implementation_attempt + 1,
    current_stage = 'implement',
    stage_started_at = NOW(),
    status = 'waiting'
WHERE agent_id = {N};
```

Then run `implement` again.

If `implementation_attempt >= 3`:

```sql
UPDATE bead_claims
SET status = 'blocked'
WHERE bead_id = '<bead_id>';

UPDATE bead_backlog
SET status = 'blocked'
WHERE bead_id = '<bead_id>';

UPDATE agent_state
SET status = 'idle',
    bead_id = NULL,
    current_stage = NULL,
    stage_started_at = NULL
WHERE agent_id = {N};
```

Exit with terminal failure.

## 4) Success Path

When all four stages pass:

```bash
br update <bead_id> --status completed
jj commit -m "Completed bead <bead_id>"
br sync --flush-only
git add .beads/ && git commit -m "sync beads"
jj git fetch
jj git push
```

Then mark DB complete:

```sql
UPDATE bead_claims
SET status = 'completed'
WHERE bead_id = '<bead_id>';

UPDATE bead_backlog
SET status = 'completed'
WHERE bead_id = '<bead_id>';

UPDATE agent_state
SET status = 'done',
    current_stage = 'done',
    stage_started_at = NOW()
WHERE agent_id = {N};
```

Finish workspace:

```bash
zjj done
```

## 5) Machine-Readable Progress Output

Emit progress in this shape after each stage transition:

```text
STATE: <stage>
BEAD: <bead_id>
ATTEMPT: <n>
RESULT: <started|passed|failed|blocked|done>
NEXT: <next action>
```
