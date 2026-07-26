---
component: adr-runtime-event-ledger-lifecycle-truth
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - lifecycle
  - persistence
  - runtime-events
  - cleanup
code_paths:
  - portal/backend/service/storage/repos/lifecycle.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/bots/storage_gateway.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/db/models.py
  - docs/architecture/persistence/PERSISTENCE_BOUNDARY.md
---
# ADR 0042: Use The Runtime Event Ledger As Lifecycle Truth

## Status

Accepted on 2026-07-25.

**Retroactive cleanup ADR:** this records the lifecycle hard cutover completed
during the platform-baseline cleanup.

## Context

Lifecycle state was previously recoverable through more than one storage shape.
That allowed run summaries, compatibility readers, and synchronization logic to
disagree about whether a run had started, degraded, completed, or failed.

## Decision

Canonical lifecycle checkpoints are immutable events in
`portal_bot_run_events`. `portal_bot_runs.status`, `started_at`, and `ended_at`
are a rebuildable summary projection, not an independent lifecycle authority.

A lifecycle append and its run-summary projection update occur in one database
transaction. Non-lifecycle writers reject lifecycle-owned summary fields.
Lifecycle readers use canonical ordered events. Repair is an explicit replay of
those events, never a fallback read from a retired store.

The backend creates each run identity and passes it into the container. Missing
container run identity is a startup error; the runtime does not generate a new
run ID.

## Invariants

- Every accepted lifecycle transition has one canonical ledger event.
- Canonical lifecycle order is the durable runtime `run_seq`.
- Unknown, backdated, status/phase-mismatched, and post-terminal transitions
  fail loudly.
- Terminal lifecycle is monotonic.
- A reused event ID is idempotent only when its material payload is identical.
- Summary projection failure rolls back the lifecycle event append.
- Deleting the summary fields must not destroy the evidence needed to rebuild
  them.
- Container runtime requires the exact backend-owned run ID and never invents a
  replacement identity when startup configuration is incomplete.

## Consequences

Lifecycle recovery and reporting have one evidence source. The summary row
remains fast to query, but all mutation must pass through the lifecycle
repository. Repair tooling is explicit and auditable.

Container misconfiguration now stops before facts can be split across an
invented identity, making leases, wallet state, accounting, and reports
reconcilable to the backend-created run.

## Rejected Alternatives

- Keep a legacy lifecycle table synchronized with runtime events.
- Treat `portal_bot_runs.status` as canonical and use events only for audit.
- Let general run upserts update lifecycle fields.
- Fall back to whichever lifecycle source contains a value.
- Generate a local run UUID when the backend-owned ID is missing.

## Enforcing Tests Or Evidence

- `tests/test_portal/test_lifecycle_repo.py` covers canonical append,
  transactional projection, strict transition admission, canonical-only reads,
  terminal monotonicity, and explicit rebuild.
- `tests/test_portal/test_run_storage_json_safety.py` verifies general run
  upserts reject lifecycle-owned fields.
- `tests/test_portal/test_runtime_events_repo.py` verifies event idempotency,
  dense `run_seq`, and producer-order preservation.
- `tests/test_portal/test_container_runtime_startup_identity.py` verifies
  missing backend-owned run identity fails before container startup proceeds.
- Cleanup commit `00440b2` removed fallback readers and direct summary writers.

## References

- [ADR 0009: One Postgres Persistence Boundary And Retained Event Ledger](0009-use-one-postgres-persistence-boundary-and-retained-event-ledger.md)
- [ADR 0030: Keep Portal Bots Definition Only](0030-keep-portal-bots-definition-only.md)
- [Persistence Boundary](../persistence/PERSISTENCE_BOUNDARY.md)
