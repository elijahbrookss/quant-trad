---
component: adr-frozen-candle-derivation
subsystem: data
layer: decision
doc_type: adr
status: accepted
tags:
  - candles
  - provenance
  - known-at
  - datasets
code_paths:
  - src/market_data/candle_derivation.py
  - src/market_data/store.py
  - portal/backend/service/market/candle_derivation_service.py
  - portal/backend/service/storage/repos/market_data.py
  - portal/backend/controller/candles.py
  - cli/main.py
---
# ADR 0074: Derive coarser candles from explicit frozen source snapshots

## Context

Research can have accepted minute history while its required coarser candle
series are absent. Implicit read-time aggregation would hide mutation, source
selection and later-known revisions. Acquiring another provider window is not
necessary when complete compatible accepted inputs already exist.

## Decision

An explicit `qt data derive-candles` / `POST /api/candles/derive` operation
materializes coarser canonical candle Facts from one verified frozen Dataset
series. The existing frozen-series validator verifies material, provenance,
quality and watermark before the producer computes anything. Each target bucket
must contain exactly one same-source candle in each source slot; windows must
align to the target UTC grid and the target cadence must be an integral multiple
of the source cadence, at most one day. Inputs are bounded to 100,000 frozen rows
and outputs to 10,000 per request. Gaps, duplicates and mixed sources fail before
writes. No provider calls, gap filling or automatic Check preparation occur.

The producer owns OHLC aggregation, additive volume/trade count (unknown if any
input is unknown), and exact per-bucket input lineage. Derived `known_at` is the
maximum of interval close and every pinned input's `known_at`. `accepted_at` is
the actual materialization time. This is a selected-snapshot transformation;
it does not reconstruct old correction history or make late corrections usable
at the original close. Normal consumer readiness may reject late derived bars.

A distinct source identity binds transformation version, source Dataset ID/hash,
series and original source identity. It retains the original provider/venue for
binding compatibility, with `canonical_candle_derivation` source kind. It never
claims native provider coarse-candle provenance. Per-row provenance also records
input revisions, commit sequences, row/material hashes and ingestion runs.

The existing canonical writer is used without corrections. An opt-in
`require_same_source` guard checks the existing row's source under the existing
series transaction lock, before row-hash dedupe; a conflict rolls back the batch.
This matters because the legacy candle row hash intentionally excludes source
identity. Default writer behavior, historical row hashes, existing Datasets,
placement and schema stay unchanged. Identical retries against the same input
snapshot can no-op; another snapshot/source cannot overwrite occupied candles.

## Consequences

A different snapshot for an occupied target window is an explicit conflict, not
an automatic update. Operators must retain and reuse the original source Dataset
for a retry. Derivation is not a substitute for missing source data, a provider
adapter, an Indicator, or a second market-analysis engine. It unblocks exact
existing-data research while preserving the input clocks and immutable evidence.
