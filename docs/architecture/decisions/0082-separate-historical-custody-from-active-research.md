---
component: adr-active-research-and-historical-custody
subsystem: research-orchestration
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - research
  - indicators
  - evidence
code_paths:
  - src/core/execution_control.py
  - src/engines/indicator_engine/runtime_engine.py
  - portal/backend/service/research/execution.py
  - portal/backend/service/research/execution_limits.py
  - src/indicators/reversal/definition.py
  - src/indicators/reversal/runtime.py
  - src/engines/indicator_engine/signal_output.py
  - src/research_science/hashing.py
  - src/research_science/temporal.py
  - portal/backend/service/research/recognition_preparation.py
  - portal/backend/service/research/registry.py
  - portal/backend/service/research/catalog.py
  - portal/backend/service/research/service.py
---
# ADR 0082: Separate Historical Custody From Active Research

## Status

Draft, 2026-10-18. Implements the approved clean replacement. Exact integrated
qualification and release remain separate. No production data migration is
performed or inferred by this decision.

## Context

Keeping every fixed symbol/date research adapter runnable solely to read old
evidence caused a growing execution catalog. An unused executable
StudyDefinition/implementation registry also described a second orchestration
architecture without a production consumer. Several listed Indicators had
compute helpers but no supported typed runtime.

## Decision

Use existing nouns and boundaries:

- Indicator owns reusable causal recognition/state, references, stages,
  completion-known duration and context through initialize/apply/snapshot.
- CheckDefinition pins one admitted procedure/settings; its Python evaluator
  owns selection/comparison, future paths and analytical summaries.
- A saved Check holds the immutable execution evidence. A Study owns the
  question, findings, interpretation revisions and evidence relationships.

Signal remains the existing typed Indicator output `{"events": [...]}` with
causal event identity/known-at and optional metadata/reference. A candidate
signal rule may be developed before a Check; Checks assess usefulness. No
Indicator requires a prior Check, and no new Signal framework is introduced.
Strategy still owns decisions, actions and risk, while Bot owns execution.

These are development choices, not a compulsory linear runtime pipeline.
Indicators can be implemented directly once causal rules are defined. An
exploratory Check may reveal reusable calculations worth extracting into an
Indicator. Checks test and help develop candidate signals; a signal can exist
before validation and does not imply proven profitability.

Remove the unused executable StudyDefinition/bundle registry and unsupported
Pivot Level, Trendline and VWAP entries. Keep Market Profile v1 and v2 as
distinct pinned contracts, Candle Stats, Regime and Reserve State. Register
the reusable Reversal typed runtime on the existing shared recognizer. It
emits only currently known completion/lifecycle/context deltas; no labels,
future-path calculation, prediction or trading authority. Origin-aware
planning is explicit opt-in and leaves existing Indicator warmups unchanged.

The active Check catalog contains 19 base definitions: raw_forward_outcome
3/6/7/8/17/18/19/20 and event_fact_analysis 4–14. Preserve their exact base and
configured-example identities. Retire the other 17 factories, fixed recipe
wrappers and legacy sweep executor. Do not translate a different historical
procedure into a current one. Shared scientific kernels remain reusable.
Methods 17, 18 and 20 require frozen evidence; discovery must not advertise
preview where the evaluator rejects it.

Historical reading is independent of current execution. Saved Check payloads,
Observations, Study interpretations and their existing relationships remain in
QT without being rewritten. A retired method is inspectable as
`legacy_frozen_unverifiable`, with current execution, replay and new
evidence-based admission unavailable. This is an availability limit, not a
claim that the original result was invalid.

The earlier record-specific runtime custody allowlist is removed. Source code
must not carry private research-record identities or hashes to grant selected
legacy records a verified-history status. Retirement does not install a private
replacement registry or require a data migration.

New Observation admission, publication references, image attachment and verified
exports use supported Check validation. Existing Study interpretation history
and image bindings remain readable without re-admitting retired research.
Saved-selection measurements accept supported Check17 selections; retired
selection profiles cannot feed a new measurement. The versioned Check18 rule
metadata remains fixed for existing payload identities; current admission does
not restore retired input procedures. ScientificAttempt references still
require actual supported deterministic replay.

## Shared execution observability

Common operational timing, sizing and execution outcomes belong to the shared
harness, not each Indicator or Check evaluator. Reuse `ExecutionControl`, its
stage wrappers and existing worker execution snapshots. With an active control,
`stage_seconds` records inclusive wall time and `stage_outcomes` counts completed,
failed, cancelled and budget-exceeded entered stage calls. The existing operation log
adds execution status and error class, including worker-owned controls; nested
bounded calls share one meter and one outer operation log after scope unwind.
Admission failures report status/error class with null metrics if no control was
obtained. Shutdown uncertainty also omits the snapshot: an interrupt helper may
still hold its lock, and reporting must not delay propagation. Diagnostic logs
use a nonblocking snapshot and also emit null metrics when a reused worker
control is busy; authoritative worker result snapshots remain unchanged. A completed build
operation does not certify later publication or whole-job success.

Check input binding, input preparation, evaluation and persistence are distinct
stages. Input preparation may include Indicator engine execution; evaluation may
include lazy input reads and scalar assertions. These are overlapping wall
measurements, not additive or exclusive CPU measurements. Elapsed time starts
with the execution-local control, excluding queue residence. Queue timestamps
and terminal status remain the existing async-job repository's responsibility.

`observed_sizes` holds latest known structural counts, not cumulative totals or
heap sizes. Unavailable/lazy counts are null. Output counts clear when a new
frame starts; rejection before entry leaves prior observations unchanged, so
execution/job status remains authoritative for attempt completion. Read/evidence/result byte counters
remain existing I/O and budget accounting. Do not traverse lazy inputs or add
payload serialization solely for metrics. Keep stage/count names stable; use
structured log/report correlation for IDs, ranges and hashes, not metric labels.

Indicator frames retain shared guard timings and overlay size/warning fields,
including outside a controlled operation. Component-specific inspection uses
existing `runtime_diagnostics`, source diagnostics and `RuntimeDetail`; declared
Check result diagnostics retain their own schema. These are not replacements
for common timers, and operational observations do not enter scientific result
or definition hashes. Existing opt-in backtest profiling reports process CPU and
process-peak RSS; it does not establish per-Indicator/Check CPU or memory use.
No new tracker, profiling store, telemetry platform or resource limit is added.

## Consequences

No original evidence payload, hash, image, Study publication or scientific
result is rewritten. Current runtime no longer carries retired executors to
read those records. Historical dependencies do not cause automatic
re-registration, new evidence eligibility or data migration. Producing-source
archives may permit separate future forensic reproduction, but current QT does not claim that it can execute retired code.
New analytical semantics require a new admitted version, not a generic DSL.
