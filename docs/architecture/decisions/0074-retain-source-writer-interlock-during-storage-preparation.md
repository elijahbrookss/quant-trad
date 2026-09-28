---
component: adr-source-storage-writer-interlock
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - storage
  - migration
  - recovery
code_paths:
  - src/core/storage_writer_fence.py
  - portal/backend/run_backend.py
  - portal/backend/workers/market_data_collector.py
  - portal/backend/workers/single_node_initializer.py
  - tests/test_market_data/test_storage_writer_fence.py
---
# ADR 0074: Retain the source writer interlock during storage preparation

## Context

An online migration must allow collection during bulk preparation and exclude
late source writers during its final switch. A stopped-container snapshot or a
locally reaped Docker client cannot exclude a later process start. Deploying the
new storage-layout runtime merely to add this guard would change the serving
v1 contract before migration.

## Decision

Backport the existing fixed source-directory shared lock to the deployed source
revision's backend, collector and initializer entrypoints. Keep the current
schema, settings, research behavior and ownership. The backend's existing
process supervisor explicitly passes the held descriptor to its children; the
lock lasts through the last inherited descriptor. An explicitly guarded backend
checks database readiness before spawning, using the existing wait boundary.

The operator's exclusive lock is an additional necessary interlock, not switch
authority. Actual image/fleet/mount admission, database admission, live proofs,
deadlines, durable dispatch intent and authoritative outcome inspection remain
separate requirements. This does not cover arbitrary privileged actors or
independent bot containers. Unconfigured startup is unchanged; invalid or
contended explicit bindings fail before work. No source data, directory mode,
schema, marker, policy or recovery key is modified.

## Verification

Linux tests exercise real shared/exclusive contention, private file preservation,
all three entrypoint refusals, inherited ownership after supervisor exit, root
replacement/configuration removal and database readiness before child spawn.
The source-compatible package and deployment remain separately qualified; this
code does not authorize migration or ordinary relaunch after an unresolved
final transition.
