# Ubiquitous Language Glossary

Canonical domain language for `shitty-swarm-manager`.

## Canonical Terms

| Term | Type | Definition | Primary Surface |
|------|------|------------|-----------------|
| **Bead** | Entity | Single unit of coordinated work tracked across backlog, claims, and stage history. | Runtime + Schema + Docs |
| **Claim** | Verb/Noun | Reservation of a bead by an agent with lease ownership and heartbeat. | Runtime + Schema |
| **Attempt** | Value | Monotonic counter for implementation retries on a bead stage. | Runtime + Schema |
| **Transition** | Domain Event | Deterministic state change emitted from stage outcome rules. | Runtime + Schema |
| **Landing** | Process | Completion flow that requires commit + push confirmation before finalization. | Runtime + Docs |

## Deprecated Aliases

Deprecated aliases: task, issue, work item

These aliases are retained only for backward-compatible reading. New runtime prompts, schema comments, and documentation MUST use canonical terms.

| Deprecated | Canonical | Notes |
|------------|-----------|-------|
| task | Bead | Legacy wording in older prompts.
| issue | Bead | Informal tracker synonym.
| work item | Bead | Generic planning synonym.

## Cross-Layer Mapping

| Layer | Canonical Usage |
|-------|-----------------|
| Runtime (`src/prompts.rs`) | "bead" lifecycle wording and stage transitions |
| Schema (`crates/swarm-coordinator/schema.sql`) | `bead_*`, `attempt_number`, transition event rows |
| Docs (`docs/BOUNDED_CONTEXTS.md`) | Canonical glossary and bounded-context vocabulary |
