# Salvage Report (zjj + jj)

## zjj cleanup completed

- `zjj doctor --fix` removed orphaned workspace records.
- Removed remaining invalid sessions with `zjj remove <name> -f`.
- Current active sessions: `salvage-curation`, `baseline-check`.
- Session DB now healthy enough for new work (`zjj doctor` has no integrity errors).

## Workspace salvage findings

- Most leftover directories under `../shitty-swarm-manager__workspaces/` are stale snapshots with `No working copy`.
- Highest-value salvage is commit-based (jj history), not uncommitted workspace state.
- Inventory of orphan workspace directories: `.opencode-runs/workspace-orphans-inventory.txt`.

## Candidate commits to curate

| Commit | Intent | Scope note |
|---|---|---|
| `dbeb66e57dda` | add deep resume-context command | focused, 4 files |
| `edada54dfda1` | add deep resume-context command | narrower follow-up |
| `ef090cd743a1` | add deep resume-context command | very small follow-up |
| `50f13c095108` | resume persistence parser tests | tiny test patch |
| `676bf7906398` | crash-resume persistence payload tests | broad test+runtime patch |
| `87c4c51ef127` | swm-3qw contract formatting | very noisy (23 files) |
| `3194140368f5` | refine resume-context handler | very noisy (8 files, huge churn) |
| `234ad24e271b` | contract builder fix | already on `main` |

Detailed table: `.opencode-runs/salvage-candidates.tsv`.

## Baseline health warning

- `moon run :quick` currently fails even in clean `baseline-check` workspace on `main`.
- This means strict gate verification is blocked by existing compile/test drift already present in `main`.

## Recommended curation order

1. Re-derive deep resume context from smallest commits first (`ef090cd743a1` then `edada54dfda1`, only escalate to `dbeb66e57dda` if needed).
2. Re-apply small parser test patch (`50f13c095108`).
3. Treat large noisy commits (`3194140368f5`, `87c4c51ef127`, `676bf7906398`) as reference only; manually port bead-relevant hunks.
4. Keep all curation in `salvage-curation` until baseline build health is restored.
