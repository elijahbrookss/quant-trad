---
component: adr-candle-dataset-identity-quality
subsystem: reporting
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - dataset-identity
  - candles
  - quality
  - provenance
  - gaps
  - cleanup
code_paths:
  - src/core/candle_snapshot.py
  - src/engines/bot_runtime/strategy/series_builder_parts/series_construction.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - portal/backend/service/reports/run_research_dataset.py
  - docs/architecture/data/DATA_BOUNDARY.md
  - docs/architecture/reporting/REPORTING_BOUNDARY.md
---
# ADR 0046: Fingerprint Exact Candle Inputs And Keep Quality Separate

## Status

Accepted on 2026-07-25.

**Retroactive cleanup ADR:** this records the dataset-identity correction made
during the baseline cleanup. It maps onto the existing continuity, provenance,
readiness, and caveat contracts; it does not introduce a second quality
envelope.

## Context

A candle catalog and gap summary can describe coverage without identifying the
actual OHLCV/ATR values consumed by runtime. Two runs could therefore share a
`data_snapshot_hash` even when a candle value changed. Conversely, changing
diagnostic gap metadata should not create a different material dataset identity
when the consumed candle rows are identical.

## Decision

Series construction fingerprints the exact normalized candle values consumed by
runtime. The per-series snapshot includes strategy, instrument, timeframe,
ordered timestamps, OHLC, ATR, and volume using an exact canonical numeric
encoding. Terminal producer-owned candle-continuity facts carry this snapshot
through the existing runtime event, BotLens diagnostic, and report paths.

Reporting aggregates every required runtime series snapshot into
`candle_data_snapshot.v1`. Snapshot ordering is canonical. Missing evidence for
any expected series makes `data_snapshot_hash` unavailable and readiness
surfaces `missing_data_snapshot_hash`. The expected
strategy/instrument/timeframe inventory comes from backend preflight planning,
is preserved across worker aggregation independently of worker success and
decision, signal, or trade traces, and must equal the terminal snapshot
inventory exactly.

Continuity gaps, provider provenance, confidence, caveats, warmup, and
truncation remain separate quality evidence. They may block or degrade a run,
but do not substitute for or mutate the exact value fingerprint.

## Invariants

- Changing one consumed candle value changes dataset identity.
- Reordering identical timestamped rows does not change identity.
- Duplicate timestamps, non-finite values, malformed OHLC, negative ATR, and
  negative volume fail loudly.
- Every expected strategy/instrument/timeframe series has runtime-produced
  snapshot evidence before a run-level hash is available.
- Multiple strategies consuming the same market series remain distinct inputs
  and are not collapsed accidentally.
- Diagnostic or observer-only gap metadata cannot change material data identity.
- Missing snapshot evidence is omitted or unavailable, never represented by an
  empty object that looks like proof.

## Consequences

Repeated-run comparison can distinguish exact input equality from data quality.
The terminal continuity fact becomes the propagation carrier for both exact
snapshot evidence and separate continuity evidence, avoiding a parallel quality
contract. Older runs without value snapshots cannot claim a complete data hash.

## Rejected Alternatives

- Hash only requested windows, counts, and gap summaries.
- Round numeric values before hashing.
- Re-fetch mutable provider data during report construction.
- Persist a second quality envelope solely for snapshot identity.
- Treat missing snapshots as an empty or all-zero dataset hash.

## Enforcing Tests Or Evidence

- `tests/test_candle_snapshot.py` covers value sensitivity, order stability,
  malformed input, duplicate detection, aggregate stability, and multi-strategy
  identity.
- `tests/integration/runtime/test_runtime_push_stream.py` verifies terminal
  producer continuity carries the snapshot.
- `tests/test_portal/test_botlens_domain_events.py` verifies the evidence
  survives the durable diagnostic projection.
- `tests/test_portal/test_run_research_dataset.py` verifies gap metadata is
  non-material, configured/terminal series sets must match exactly, missing
  series block the hash, and observer diagnostics do not alter material
  identity.
- `tests/test_portal/test_series_builder_incremental.py` verifies one failed
  eligible configured series aborts the whole multi-instrument build.
- `tests/test_portal/test_report_artifact_bundle_workers.py` verifies
  multi-worker run metadata retains the complete planned inventory even when
  the available worker series are partial.
- `tests/test_portal/test_container_runtime_transport.py` verifies container
  worker planning consumes the backend preflight instrument inventory.
- `tests/test_portal/test_report_execution_mode_contract.py`,
  `tests/test_cli/test_experiments_orchestration.py`, and
  `tests/test_cli/test_mcp_server.py` verify canonical dataset identity,
  quality, blockers, repeatability, and caveats survive compact report,
  persisted experiment, and thin MCP projections without a second quality
  contract.

## References

- [ADR 0003: Preserve Data Boundary Source Facts](0003-preserve-data-boundary-source-facts.md)
- [ADR 0015: Split Semantic And Operational Golden Fingerprints](0015-split-semantic-and-operational-golden-fingerprints.md)
- [ADR 0031: Fingerprint Reports And Slim Runtime Storage](0031-fingerprint-reports-and-slim-runtime-storage.md)
- [ADR 0032: Field-Owned Version And Provenance Contracts](0032-use-field-owned-version-and-provenance-contracts.md)
