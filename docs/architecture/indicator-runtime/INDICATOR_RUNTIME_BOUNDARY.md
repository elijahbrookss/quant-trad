---
component: indicator-runtime-boundary
subsystem: indicator-runtime
layer: boundary
doc_type: architecture
status: active
tags:
  - indicators
  - runtime
  - typed-outputs
  - overlays
  - research-validation
  - known-at
code_paths:
  - src/engines/indicator_engine
  - src/indicators
  - portal/backend/controller/indicators.py
  - portal/backend/service/indicators/indicator_factory.py
  - portal/backend/service/indicators/signal_payload_filtering.py
  - portal/backend/service/indicators/indicator_service/runtime_validation.py
  - docs/architecture/indicator-runtime/diagrams/indicator-runtime-contract.mmd
  - docs/architecture/indicator-runtime/diagrams/indicator-surfaces.mmd
---
# Indicator Runtime Boundary

## Purpose

The indicator runtime boundary converts source facts and dependency outputs into typed, known-at outputs. It also exposes chart and debug projections without letting those projections become strategy inputs.

Related diagrams:

- [indicator-runtime-contract.mmd](diagrams/indicator-runtime-contract.mmd)
- [indicator-surfaces.mmd](diagrams/indicator-surfaces.mmd)

## Boundary Contract

Indicators own private state. The engine owns call order, dependency resolution, and output validation.
The engine also owns the indicator commit clock. Indicators return plain
runtime outputs; `IndicatorExecutionEngine` stamps `indicator_commit_seq` only
after it has applied the bar and validated the declared snapshot surface.

| Surface | Consumer | Contract |
| --- | --- | --- |
| `snapshot()` | decision layer, runtime, research | canonical public typed outputs |
| `overlay_snapshot()` | BotLens, charts, previews | visual projection of indicator state |
| `detail_snapshot()` | operator/debug views | diagnostic payload, not a strategy input |

Strategies consume signal, context, and metric typed outputs only. They do not
inspect lifecycle outputs, overlays, details, helper caches, or mutable
indicator internals.

Indicator output catalogs describe every public typed output the indicator may
emit. A catalog entry may describe signal event keys, context state keys, or
metric fields, but it must not carry persisted enable/disable preferences.
Output selection belongs to consumers such as strategy rules, strategy variants,
research checks, signal preview requests, or UI visibility state. Persisting a
disabled output on the indicator would turn a consumer choice into indicator
truth and make reports, replay, and observation mining incomplete.

## Diagram Walkthrough: Runtime Contract

[indicator-runtime-contract.mmd](diagrams/indicator-runtime-contract.mmd) shows one bar:

1. `IndicatorExecutionEngine` resolves declared dependency outputs.
2. The engine calls `apply_bar(bar, inputs)`.
3. The indicator mutates only its own internal state.
4. The engine reads `snapshot()` and validates declared output names, types, readiness, and bar time.
5. The engine stamps typed outputs and output deltas with the next
   `indicator_commit_seq` for that indicator.
6. Overlay/detail surfaces are read for projection/debug consumers when requested
   and inherit the source indicator commit sequence for provenance.
7. Typed outputs flow to the decision layer; projections flow to BotLens or chart surfaces.

This is the indicator-specific form of `initialize -> apply_bar -> snapshot`.

## Diagram Walkthrough: Indicator Surfaces

[indicator-surfaces.mmd](diagrams/indicator-surfaces.mmd) separates three surfaces:

- typed outputs answer "what may strategy logic use?"
- overlays answer "what should an operator see?"
- details answer "what should a debugger inspect?"

All three can derive from the same indicator-owned state, but only typed outputs are part of the decision contract.

## What The Engine Accepts And Publishes

The engine advances indicators from provider-backed candle bars, declared
dependency outputs by `OutputRef`, runtime specs, and params. Preview,
validation, strategy-preview, and bot-runtime callers configure render-only
retention through the single `configure_indicator_overlay_history` dispatcher.
The positive `history_bars` bound may prune overlay geometry, but it must not
change warmup, source inputs, output readiness, or decision semantics. Runtime
indicators implement the `configure_overlay_history` contract; a missing or
malformed contract fails loudly rather than being skipped by capability
probing. The engine publishes `RuntimeOutput` values typed as `signal`, `context`,
`metric`, or `lifecycle`, plus output deltas carrying `base_indicator_commit_seq`,
`indicator_commit_seq`, and `indicator_commit_seq_status=indicator_scoped`.
Lifecycle outputs are optional public research evidence for stateful candidate
funnels such as setup, eligible, touched, confirmed, invalidated, or expired.
They are not decision inputs. A lifecycle candidate should reference the
earliest public typed output that formed the candidate. It should not wait for a
later confirmation signal unless that confirmation is truly the source fact for
the candidate family.

Visual and debug surfaces leave through separate projection payloads:
`RuntimeOverlay` for charts and `RuntimeDetail` for inspection. Guard metrics,
payload warnings, and source-fact diagnostics can explain expensive or invalid
projection/output behavior, but they do not become strategy inputs.

Indicators that load independent source candles publish the existing
`indicator_source_candle_continuity.v1` diagnostic. Bot runtime composition
collects it immediately after graph construction, adds canonical
strategy/instrument/series identity, and stores it in series metadata before
artifact creation. The runtime dependency is required: missing, malformed, or
unattributed source diagnostics fail loudly rather than disappearing.

## Runtime Validation Surface

The backend indicator runtime validation endpoint and output evidence collector
are the research-facing proof surfaces for agent-visible indicators. They build
the same runtime graph as normal indicator execution and advance the engine one
candle at a time through:

```text
initialize -> apply_bar -> snapshot
```

For each bar, validation requires every declared typed output to be present in
the engine frame. Readiness is measured separately: warmup windows may produce
`ready=false`, but missing outputs are invalid. The validation result summarizes
per-output presence, first/last readiness, ready bar counts, signal/lifecycle
event counts, observed metric/context/lifecycle fields, commit sequence provenance, guard
warnings, source-fact diagnostics, and optional assertions such as
`require_ready_by_end` or `min_ready_bars`. The evidence collector exposes the
same declared output rows alongside aligned source candles so research checks
can test indicator-produced facts without inspecting indicator internals.

This surface is intentionally a validator, not an alternate runtime. It must not
reconstruct indicator state from overlays, details, mutable internals, or MCP
payloads.

Research signal audits also use this evidence surface. If an audit needs to
distinguish one semantic group from another, such as a session, regime, pivot,
or active reference id, that grouping fact must be exposed as a public typed
output field. Research checks may name those fields in their expectation
contracts, but they must not import indicator-family code or read private
indicator state.

Frozen evidence execution supplies recorded Dataset gaps to this same runtime
graph. Each Indicator declares one versioned policy: reject, reset and re-warm,
or continue with explicit degraded status. The Indicator owns the state
transition, warmup floor, readiness, and whether an Indicator event exists.
Orchestration and Check code may report the transition but may not recreate it.

Signal event `known_at` must equal the availability time of the current source
bar/output. Events learned after a Check evaluation boundary are excluded. A
Check cannot relabel a metric or context row as an Indicator event; it may only
consume a registered signal event emitted by this runtime timeline.

## How Indicator State Advances

Indicators should have one internal timeline:

```text
source facts / dependency outputs -> evidence -> committed state -> snapshot outputs -> projections
```

Every declared output is returned every bar. `ready=false` means the output exists but is not usable yet. The engine should not wait, substitute, or reconstruct missing values.
All declared outputs from the same indicator/bar share the same
`indicator_commit_seq`. Downstream consumers use that sequence to replay typed
output transitions in indicator-local causal order without relying on wall-clock
or unordered mapping iteration.

## Failure And Recovery

- Missing declared outputs fail at the engine boundary.
- Bar-time mismatches fail because they break known-at semantics.
- Invalid output types fail because they break strategy contracts.
- Overlay/detail failures should be visible to projection/debug consumers without becoming strategy truth.
- Consumers that need new strategy-visible fields should add typed outputs, not read overlays.

## Invariants

- Indicators never predict or backfill future state.
- Dependency outputs are read through declared refs.
- Signal, context, and metric outputs are typed contracts, not arbitrary blobs.
- Public output catalogs are complete; consumers select outputs without mutating
  indicator identity.
- Lifecycle outputs describe optional candidate/setup research facts. They are
  public and known-at, but strategies do not consume them.
- Overlays and details are projections.
- Runtime validation must validate declared output presence on every bar and
  report readiness separately from presence.
- Indicator commit sequence is engine-owned; indicator implementations must not
  fabricate or persist alternate clocks.
- Only the Indicator runtime may emit an Indicator-defined event.
- Gap reset, re-warm, degraded state, and post-gap readiness are
  Indicator-owned semantics.
- Indicator-specific docs should exist only when an indicator family has architecture behavior beyond ordinary authoring guidance.

## Related Docs

- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Decision layer boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)

## Known Gaps

- Full indicator tutorials are intentionally deferred to guide docs.
- Existing indicator families may need focused architecture notes only if they introduce distinct runtime contracts.
