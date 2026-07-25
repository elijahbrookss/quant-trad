---
component: adr-field-owned-version-provenance-contracts
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - persistence
  - reporting
  - provenance
  - schema
  - versioning
code_paths:
  - portal/backend/service/provenance.py
  - portal/backend/db/models.py
  - portal/backend/db/session.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/storage/repos/runs.py
  - portal/backend/service/storage/repos/report_materializations.py
  - portal/backend/service/reports/materialization.py
  - portal/backend/service/reports/contract.py
  - portal/backend/service/reports/run_research_dataset.py
  - portal/backend/service/market/instrument_service.py
  - src/engines/bot_runtime/runtime/components/step_trace_rollup.py
  - scripts/db/manual_migration_versioning_hard_cutover.sql
---
# ADR 0032: Use Field-Owned Version And Provenance Contracts

## Status

Accepted on 2026-06-01.

## Context

Version identifiers had accumulated in several different places with different
meanings:

- physical storage names such as report materialization, profiler, and
  observability tables carried version suffixes,
- report cache identity used contract/schema strings but did not clearly
  separate code provenance from durable input fingerprints,
- runtime rows mixed runtime provenance with report-owned schema identity,
- some payload identifiers used underscore-style version labels that looked like
  object names instead of contract fields.

This made the system look legacy even when the actual data contract was still
current. It also blurred the answer to "what changed?" across code, data,
storage, and report layers.

## Decision

Physical database tables, SQLAlchemy classes, constraints, and indexes use stable
concern names. They do not carry semantic version suffixes unless two physical
implementations are intentionally active at the same time.

Versions live in fields owned by the artifact or layer they describe:

- runtime rows own `runtime_contract_version`, `runtime_source_revision`,
  `runtime_image`, and `storage_schema_version`,
- report materializations own `contract_version`, `report_schema_version`,
  `dataset_schema_version`, `builder_source_revision`, and
  `storage_schema_version`,
- dataset/report payloads keep their explicit `schema_version` fields,
- input fingerprints remain separate from schema versions and include durable
  source boundaries and material run provenance.

Source revision means the commit hash that produced the backend process. Local
development may resolve it from the checkout; containerized execution must
provide `SOURCE_REVISION`. Legacy aliases such as `GIT_COMMIT` are not part of
the contract.

Report materializations are stale when the durable input fingerprint changes,
when the report/dataset contract version changes, or when the report builder
source revision changes. Runtime rows keep their original runtime source
revision as historical run provenance and are not rewritten by later report
builds.

## Update Cadence

- `SOURCE_REVISION` changes every code build or local checkout revision.
- `runtime_contract_version` changes only when runtime fact/event semantics
  change.
- `report_schema_version` changes only when the single-run report payload shape
  or consumer semantics change.
- `dataset_schema_version` changes only when `RunResearchDataset` shape or
  semantics change.
- `storage_schema_version` changes when required table/column/index/constraint
  shape changes.
- config, strategy, data, and report input fingerprints change when their
  material inputs change.

## Consequences

- Storage names read as durable concerns, not as old versions.
- Report cache validity is code-aware without using table names as versioning.
- Runtime provenance and report provenance no longer live in the same row by
  accident.
- Hard cutovers require DB migration and app code to move together; the system
  intentionally does not provide compatibility readers for old physical names.
