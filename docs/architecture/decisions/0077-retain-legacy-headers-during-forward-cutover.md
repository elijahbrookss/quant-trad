---
component: adr-forward-header-cutover
subsystem: persistence
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - storage
  - postgres
  - explicit-migration
code_paths:
  - scripts/db/fact_header_forward_keys.py
  - scripts/db/fact_header_forward_adoption.py
  - scripts/db/fact_header_forward_placement.py
  - scripts/automation/storage_online_forward.py
  - scripts/automation/storage_online_forward_worker.py
  - scripts/automation/storage_online_reschedule.py
  - tests/test_storage_forward_reschedule.py
  - scripts/automation/storage_online_keys.py
  - scripts/automation/storage_online_terminal.py
  - tests/test_storage_forward_successor.py
  - scripts/db/fact_header_v2_references.py
  - tests/test_market_data/test_fact_header_forward_keys_db.py
  - tests/test_market_data/test_fact_header_forward_adoption_db.py
  - tests/test_market_data/test_fact_header_forward_handoff_db.py
  - portal/backend/db/fact_header_legacy_schema.py
  - portal/backend/db/fact_identity_schema.py
  - portal/backend/db/fact_series_day_schema.py
  - portal/backend/db/market_data_models.py
  - portal/backend/service/storage/header_catalog.py
  - portal/backend/service/storage/repos/fact_storage.py
  - scripts/db/fact_header_v2_handoff.py
  - scripts/db/raw_mapping_v2_placement.py
  - cli/storage_placement.py
  - tests/test_cli/test_storage_placement.py
  - scripts/automation/server_deploy.sh
  - tests/test_server_promotion.py
  - tests/test_market_data/test_raw_mapping_history_placement_db.py
  - tests/test_market_data/test_incremental_raw_placement_db.py
---
# ADR 0077: Retain Legacy Headers During a Forward Cutover

Proposed October 2, 2026 as an alternative to rebuilding all historical headers
before new storage can serve. This is an architectural proposal with explicit
implementation and qualification boundaries, not production operation authority.
[Storage management](../persistence/STORAGE_MANAGEMENT.md) owns the current
procedure and supported operating path.

## Reuse equivalent material without weakening identity

When the retained header heap has the required column contract, preserve it as
one sealed historical range beneath the canonical dated parent. New days use
normal daily partitions. The global identity registry remains the single owner
of Fact and revision uniqueness; payload and archive references still use it.

Attachment needs exact composite primary/revision keys, a valid range constraint,
registered ownership and compatible indexes. Preserve heap and index identities
where PostgreSQL can reuse them. Keep the unchanged canonical raw table when
its identity and content already satisfy the target contract.

This can avoid copying the historical heap and rebuilding equivalent search
indexes. It cannot eliminate key preparation, registry/raw catch-up, reference
validation, archive work or recovery publication. A completed baseline is not a
caught-up target, and retained bytes are not proof of equivalence by themselves.

## One reader must cover retained and new ranges

The same canonical repository must read the sealed historical range and new
daily partitions with unchanged source filters, corrections, invalidations,
known-at selection and frozen Dataset identity. A directory designed only for
daily partitions cannot silently omit retained history.

Seal and binding validation belong to startup and the storage boundary.
Detached or missing history, disabled enforcement and identity drift must refuse
rather than produce an apparently complete empty result. Physical inventory must
count the retained heap, indexes, TOAST and temporary/movement obligations.

## Adopt completed work through explicit ownership

Reusing preparation from an earlier attempt requires a preserving terminal
transition and exact ownership/content verification. Canceled capture is never
revived. Completed key preparation may be reusable when its immutable identity
matches; incomplete work remains bounded by its original deadline.

Commit adoption, archive interval capture and initialization evidence under the
owning transaction and clocks. Resolve lost replies from the same complete record.
Foreign requests, changed keys, expired incomplete initialization and retired
adoption refuse. Host admission must also reconcile the prior worker, source
publisher fences and database login gate.

The final switch retains exact fresh archive/reference proof, bounded residual
catch-up, uncertain-COMMIT handling and independent reader-worker retirement.
Recovery mounts, matching runtime activation and the complete encrypted recovery
pair follow only through their owning transition. No manual schema approximation
or marker deletion substitutes for that procedure.

## Costs and evidence limits

A retained range can postpone historical placement and retain different query
costs from daily history. Measure preparation, catch-up, WAL, capacity, current and
historical query behavior and final pause before admission. Avoiding one copy
does not imply a zero-cost or zero-downtime cutover.

Mechanical attachment/rollback probes establish PostgreSQL behavior for their
fixtures. QT regression fixtures establish the exercised current, correction,
frozen and verified archive reads. They do not establish production workload
capacity, a whole-host transition or custody of a particular recovery set.

This alternative preserves the identity decision in
[ADR 0070](0070-separate-global-fact-identity-from-dated-headers.md) and the online
ownership rules in [ADR 0073](0073-prepare-storage-migrations-with-live-collection.md).
