---
component: adr-online-storage-migration
subsystem: persistence
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - storage
  - migration
code_paths:
  - scripts/automation/storage_online_recovery.py
  - tests/test_storage_repository_continuation.py
  - scripts/automation/storage_online_keys.py
  - tests/test_storage_online_keys.py
  - scripts/automation/storage_online_forward.py
  - tests/test_storage_online_forward.py
  - scripts/automation/storage_online_terminal.py
  - tests/test_storage_online_terminal.py
  - tests/test_terminal_placement.py
  - scripts/automation/storage_online_deadline.py
  - scripts/db/fact_header_v2_deadline.py
  - tests/test_storage_online_deadline.py
  - tests/test_market_data/test_fact_header_deadline_amendment_db.py
  - cli/main.py
  - scripts/automation/storage_online_release.py
  - tests/test_storage_online_release.py
  - scripts/automation/storage_online_operation.py
  - tests/test_storage_online_operation.py
  - scripts/ci/online_operation_fixture.py
  - tests/test_storage_online_capture.py
  - tests/test_storage_online_runtime.py
  - scripts/automation/storage_online_runtime.py
  - scripts/provenance/source_tree_hash.py
  - portal/backend/Dockerfile
  - tests/test_storage_online_repositories.py
  - scripts/automation/storage_recovery_prepare.py
  - scripts/automation/storage_online_repositories.py
  - scripts/ci/online_guarded_source_fixture.py
  - src/core/storage_writer_fence.py
  - portal/backend/run_backend.py
  - portal/backend/workers/single_node_initializer.py
  - tests/test_market_data/test_storage_writer_fence.py
  - src/market_data/archive.py
  - src/core/settings.py
  - tests/test_market_data/test_archive_shared_ownership.py
  - scripts/automation/storage_host_boundary.py
  - tests/test_storage_inventory_removal.py
  - scripts/automation/storage_online_drain.py
  - tests/test_storage_online_drain.py
  - tests/test_market_data/test_storage_online_collector_drain_db.py
  - tests/test_market_data/tiered_v1_ingestion.py
  - scripts/automation/storage_online_final.py
  - tests/test_storage_online_final.py
  - scripts/automation/storage_online_prepare.py
  - tests/test_storage_online_prepare.py
  - scripts/automation/server_deploy.sh
  - tests/test_server_promotion.py
  - scripts/automation/storage_online_controller.py
  - scripts/automation/storage_online_launch.py
  - scripts/ci/rehearse_online_launch.py
  - scripts/ci/rehearse_online_prepared.py
  - scripts/ci/online_start_reply_fixture.py
  - tests/test_storage_online_launch.py
  - tests/test_storage_online_controller.py
  - tests/test_market_data/test_storage_online_controller_db.py
  - tests/test_market_data/test_storage_online_entrypoint_db.py
  - scripts/db/fact_header_v2_cancel.py
  - scripts/automation/storage_online_worker.py
  - scripts/ci/rehearse_online_worker.py
  - tests/test_storage_online_worker.py
  - scripts/db/fact_header_v2_online.py
  - tests/test_market_data/test_fact_header_online_prepare_db.py
  - scripts/db/fact_header_v2_online_proof.py
  - scripts/db/archive_root_v2_online.py
  - scripts/db/archive_file_v2_proof.py
  - scripts/db/fact_header_v2_capture.py
  - scripts/db/fact_header_v2_copy.py
  - scripts/db/raw_mapping_v2_copy.py
  - scripts/db/fact_header_v2_handoff.py
  - scripts/automation/storage_handoff_pause.py
  - tests/test_market_data/test_fact_header_online_db.py
  - tests/test_market_data/test_fact_header_online_proof_db.py
  - tests/test_market_data/test_archive_online_copy_db.py
  - tests/test_archive_file_proof.py
  - tests/test_market_data/test_fact_header_online_references_db.py
---
# ADR 0073: Prepare Storage Migrations With Live Collection

Proposed September 26, 2026 after whole-copy writer suspension proved an
unacceptable operating model. This decision establishes the online preparation
boundary. It is not a release receipt or an instruction to execute a migration.
Use [storage management](../persistence/STORAGE_MANAGEMENT.md) for the current
workflow owners, commands and operator admission.

## Separate migration work from the final pause

Keep one authoritative source serving throughout bounded bulk preparation.
Capture committed changes transactionally before the baseline copy begins.
Commit verified pages with durable cursors and fairly advance header, raw-reference
and archive work so one growing tail cannot starve the others.

A previous scan, count, sampled hash or empty queue cannot authorize the final
switch. Retain exact proof whose assumptions survive concurrent writes, or
revalidate them under the final fences. Copy, index construction and large
archive/reference work belong before publisher suspension.

Admit a final pause only when the residual delta, locks, reference reconciliation,
runtime transition and recovery fit the separately measured outage budget.
Migration elapsed time and publisher downtime are different measurements.
Neither a configured deadline nor a small fixture is a completion forecast.

## Process lifetime and database authority are different fences

Database login closure does not stop an already-running API or filesystem
publisher. The source-compatible lifetime interlock retains a shared directory
lock through the backend and its supervised children; the operator retains
exclusive ownership across the switch. Image, entrypoint, environment, root,
container, mount and publisher admission must establish who participates.

This is a cooperating-process boundary. It does not exclude privileged tools,
alternate entrypoints or unqualified images. Those remain part of operator
admission. A saved observation is never lock authority, and owner death cannot
authorize automatic resumption.

The host deployment lock and durable private intent prevent ordinary deployment,
rollback and relaunch from racing an unresolved transition. Corrupt, partial,
expired or canceled markers remain refusals until the owning procedure resolves
them. A marker is an exclusion mechanism, not evidence that SQL committed.

## Dispatch once and reconcile uncertainty

Record durable dispatch intent before a possible database switch. Retain the same
worker, SQL session, admitted source context and original final deadline.
A lost COMMIT reply is unknown until fresh authoritative database inspection
resolves it. Never repeat COMMIT, assume rollback from a missing reply, renew an
expired attempt or start the old runtime while outcome is unresolved.

A qualified abort may resume the original source only after proving the switch
did not commit and reconciling publisher, worker and database ownership.
After commit, activate the matching reader/runtime and preserve the committed
database; compatible application rollback is not a storage rollback.

Independently verify that the read-capability worker has stopped and been reaped
before exposing recovery keys or repositories. Then bind the retained layout,
matching runtime, initial policy and complete encrypted database/archive pair.
Ordinary deployment remains blocked until the owning completion transition
finishes its durable bookkeeping.

## Preserve evidence without making it permanent permission

Cancellation retires an attempt; it does not erase completed copies, source
rows, WAL, frozen bindings or private receipts. Retry and reconciliation use the
original persisted clocks. Completed observations may be inspected read-only
after mutation deadlines; they cannot reopen those deadlines.

Resource admission covers continuing growth, catch-up, WAL, temporary/index work,
retained originals and recovery baselines. Slow or nonconverging preparation
prevents switch eligibility rather than extending collector suspension.

## Qualification follows the complete boundary

Unit checks, disposable PostgreSQL proofs, synthetic Docker peers, actual QT
publisher checks and native host rehearsal exercise different responsibilities.
Retain exact-source evidence for the combined normal path and its interruption,
lost-reply and recovery paths. None alone certifies production publishers,
physical capacity, pause duration or recovery custody.

This design reuses the storage host boundary, controller, existing policy and
paired recovery owners. It does not add a second operator framework.
See [the source interlock decision](0074-retain-source-writer-interlock-during-storage-preparation.md)
and [the forward-cutover decision](0077-retain-legacy-headers-during-forward-cutover.md)
for complementary constraints.
