---
component: adr-canonical-publication-admission
subsystem: data
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - storage
  - concurrency
code_paths:
  - portal/backend/service/storage/repos/market_lifecycle.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/service/storage/repos/market_structure.py
  - portal/backend/service/storage/repos/normalization.py
  - tests/test_market_data/test_canonical_publication_admission.py
  - tests/test_market_data/test_canonical_publication_admission_db.py
---
# ADR 0081: Admit Canonical Publication Before Row Locks

## Status

Draft, 2026-10-18. Source implementation and focused behavioral checks are
complete. Disposable PostgreSQL concurrency qualification is required before a
release claim. This decision authorizes no production cleanup or migration.

## Context

Reclamation takes the existing lifecycle advisory fence exclusively before
its final proof and relation locks. A canonical publisher could hold a hot-row
or header witness lock and then wait for the shared lifecycle fence. The final
reclaimer then refuses a relation lock because of the waiting publisher.
Fresh archive proof and the final relation-lock guard remain necessary.

## Decision

Every canonical publication transaction takes the existing shared transaction
lifecycle advisory lock before per-series, lease/fence, raw-reference, hot-row
or header reads/locks. The common market-data writer and outer managed trade,
L2, derived-feature and normalization transactions enforce the same order.
Transaction commit/rollback releases admission; existing SQL/lock timeouts and
cancellation remain authoritative.

Heartbeat and lease renewal do not take publication admission. A publisher
blocked at admission must still renew independently and must recheck actual
ownership after admission. An expired/reassigned claim cannot publish.
Reclamation retains its fresh proof, exclusive advisory fence, relation-lock
NOWAIT refusal and rollback protections. No limit is increased.

## Consequences

Writers may wait before acquiring application row locks. The ordering removes
the observed lock inversion; it does not promise a fixed cleanup latency,
prove that another proof timeout is solved, or authorize destructive work.
Tests cover both acquisition orders, timeout/cancel rollback and a real
managed publisher whose lease becomes stale while it waits.
