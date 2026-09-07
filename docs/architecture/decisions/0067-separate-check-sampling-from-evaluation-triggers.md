---
component: adr-check-availability-triggers
subsystem: research-orchestration
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - research
  - causality
  - checks
code_paths:
  - portal/backend/service/research/event_fact_evaluator.py
  - portal/backend/service/research/registry.py
  - portal/backend/service/research/planning.py
  - src/engines/bot_runtime/core/domain/candle_factory.py
  - src/engines/bot_runtime/runtime/mixins/setup_prepare.py
  - tests/test_portal/test_event_fact_snapshot_check.py
  - tests/integration/runtime/test_candle_frame_validation.py
---
# ADR 0067: Separate Check Sampling From Evaluation Triggers

## Status

Accepted on 2026-09-06 for the bounded fact-snapshot research change.

## Context

The v4 fact-snapshot Check sampled book features at a primary candle close and
used the candle's availability as the only decision time. Even an exact matching
book feature arriving one microsecond later was excluded. The causal gate was
correct for that question, but the evaluator could not express a later decision
using the same market sample.

A read-only trace of one BIP update on 2026-08-23 identified QT receipt at
00:00:59.793226, the last receipt in its approximately 60-second archive segment
at 00:01:06.613079, archive acknowledgement at 00:01:06.702190, and canonical
acceptance at 00:01:07.207636. Most of that event's delay was attributable to
waiting for segment finalization/archive publication. This does not establish
that every collector delay has the same cause. Collector settings are unchanged.

The shared runtime candle converter also discarded `known_at`, even though the
Candle model had that field. Context reads used the market interval start.
Historical candle ingestion already separates inferred/provider availability
from later import acceptance; that distinction needs to survive conversion.

## Decision

Keep market sample identity and causal evaluation time separate. Introduce a
versioned Check trigger that evaluates once when all declared required facts and
required candle inputs are usable. Reuse canonical timestamps, revision
selection, source bindings, and frozen research execution. Preserve existing
Check versions. Do not add a grace period or a general scheduling engine.

Use subsequent supported candle prices for analytical outcomes and retain both
decision time and price-sample time in evidence. With minute candles, a decision
at 12:01:07 can use a 12:02 outcome price proxy; it cannot claim a 12:01 entry or
an observed 12:01:07 fill.

Preserve candle availability through conversion and use the close/availability
clock for bot context reads. The current bot close-price model rejects late
candles until it has an explicit execution model capable of honoring the later
decision. This change does not grant delayed bot execution support.

## Consequences

A missing input remains unresolved; an available later input can support a new,
later analytical decision. Later revisions cannot mutate an earlier decision.
Historical source/import clocks are unchanged. The existing trade-flow receipt
replay helper remains a separate, currently unwired counterfactual capability.

The implementation is bounded to existing admitted Check fact types and market
sampling rules. Collector performance, historical acquisition expansion,
strategy search, and external order submission are outside this decision.

## References

- [System contract](../../contracts/platform/00_system_contract.md)
- [Runtime contract](../../contracts/platform/01_runtime_contract.md)
- [Check evidence boundary](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md)
- [Research replay availability](../research-orchestration/RESEARCH_REPLAY_AVAILABILITY.md)
