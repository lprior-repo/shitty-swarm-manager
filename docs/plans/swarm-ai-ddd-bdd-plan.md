# AI Swarm DDD + BDD Planning Brief

## Product Goal

Build a deterministic, async Rust swarm coordinator where PostgreSQL stores complete execution context so AI agents can resume with short context windows.

## Architectural Intent

- Domain-Driven Design first: explicit bounded contexts, ubiquitous language, aggregate invariants.
- Martin Fowler principles: Service Layer, Repository, Unit of Work, CQRS-style read models, event/audit logs, separation of domain and infrastructure.
- BDD tests decoupled from implementation details: behavior and contracts over internal methods.

## Required Runtime Behaviors

- Deterministic 4-stage pipeline: `rust-contract -> implement -> qa-enforcer -> red-queen`.
- Recursive retry DAG: QA or Red Queen failure routes to `implement` with persisted feedback.
- Max 3 implementation attempts before terminal blocked state.
- Completion only after landing workflow confirms push succeeded.
- Full resumability from database artifacts and stage transcripts.

## Planning Constraints

- Use `br` (beads_rust) workflow for decomposition and tracking.
- Keep beads atomic (max 4h effort each).
- Prefer behavior-first acceptance criteria and scenario-based BDD tests.
