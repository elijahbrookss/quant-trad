---
component: adr-decouple-visual-overlay-projection-runtime-push
subsystem: botlens-projections
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - botlens
  - overlays
  - projections
  - runtime
  - performance
code_paths:
  - src/core/settings.py
  - config/defaults.yaml
  - src/engines/bot_runtime/runtime/mixins/setup_prepare.py
  - src/engines/bot_runtime/runtime/mixins/execution_loop.py
  - src/engines/bot_runtime/runtime/mixins/runtime_push_stream.py
  - src/engines/bot_runtime/runtime/mixins/runtime_projection.py
  - src/engines/bot_runtime/strategy/series_builder_parts/models.py
  - portal/backend/service/bots/botlens_domain_events.py
  - portal/backend/service/bots/botlens_event_retention.py
  - portal/backend/service/bots/botlens_chart_service.py
  - portal/backend/service/bots/botlens_overlay_history.py
  - portal/backend/service/bots/botlens_state.py
  - portal/backend/service/bots/botlens_transport.py
  - portal/frontend/src/components/bots/botlensProjection.js
  - portal/frontend/src/features/bots/botlens/buildBotLensRuntimeViewModel.js
  - portal/frontend/src/features/bots/botlens/components/ChartPanel.jsx
  - portal/frontend/src/features/bots/botlens/state/botlensRuntimeSelectors.js
  - portal/frontend/src/features/bots/botlens/state/botlensRuntimeState.js
  - docs/architecture/botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md
  - docs/architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md
---
# ADR 0038: Decouple Visual Overlay Projection From Runtime Push

## Status

Accepted on 2026-06-15.

Amended on 2026-08-03 to retain the bounded delta timeline as non-authoritative
research context and replay it into causally paged historical chart geometry.

Amends [ADR 0028](0028-use-bounded-projection-dispatch-for-botlens-live-facts.md):
runtime still emits bounded BotLens facts, but selected-symbol visual overlay
geometry is no longer built inside the ordinary runtime push-update batch.

## Context

Long bot runs showed `step_push_update` carrying too much BotLens visual cost.
The most expensive part was not strategy math or indicator typed-output
construction. It was materializing, diffing, transporting, and accounting for
rich overlay geometry on the same path that emits ordinary runtime facts.

That coupling made a debugger surface compete with execution timing. It also
left old runtime state fields such as `StrategySeries.overlays` and
runtime-owned trade overlay contracts in the model, which made it unclear where
visual state lived.

The platform still needs visual overlays. BotLens should be able to show a
bounded recent view of indicator geometry when a user opens the run. But those
overlays are projection/read-model artifacts, not strategy inputs, not report
truth, and not trade execution state.

## Decision

Visual overlays are projected through a separate bounded BotLens projection
step.

The ordinary runtime push update emits compact runtime facts such as candles,
series state, runtime health, decisions, trades, wallet events, logs, and
series stats. It does not build indicator overlay geometry and does not emit
overlay deltas.

After a bar is finalized, runtime may run `overlay_projection` as a separate
projection step. That step:

- snapshots indicator overlays from the current runtime state engine,
- keeps the indicator visual history bounded by
  `bot_runtime.botlens.overlay_window_bars`,
- emits at most on the configured bar cadence
  `bot_runtime.botlens.overlay_emit_every_bars`,
- stores visible overlays in the runtime projection cache,
- emits `overlay_ops_emitted` when the overlay delta changes and forces a final
  no-op clock advance at terminal state,
- includes projection metadata in the overlay delta:
  `mode`, `window_bars`, `emit_every_bars`, `bar_index`, `reason`, and `terminal`,
- records compaction/truncation evidence in each bounded overlay summary,
- records `overlay_projection` timing and count metrics separately from
  `step_push_update`.

The runtime series model no longer owns visual overlays. `StrategySeries` holds
execution inputs and runtime state only: candles, signals, risk engine,
instrument identity, config, and execution profile. Trade visuals are built by
the BotLens/frontend trade-fact path, not by runtime-owned overlay types.

BotLens overlay projection is degradable. Projection failure records explicit
diagnostics and can make BotLens stale or unavailable, but it must not rewrite
execution truth or fail a valid run. Canonical persistence and execution-state
errors remain fail-loud.

The backend may retain `OVERLAY_STATE_CHANGED` as Tier 2 research context. This
does not promote it into canonical run truth. Historical pages replay exact
overlay clocks, stop at the page's causal end, and are complete only when clock,
cadence, window, terminal-checkpoint, and truncation checks all pass. Old runs
without retained deltas remain explicitly unavailable. Terminal timelines with
a final checkpoint may be reused in a bounded process-local LRU; every page is
still causally sliced before rendering.

## Consequences

- `step_push_update` timing becomes a cleaner measure of compact fact
  construction and handoff, not visual overlay geometry.
- Overlay cost is visible under `overlay_projection` metrics, making future
  pressure easier to locate.
- Long runs avoid replaying or serializing full visual history on every bar.
- Frontend overlay state receives projection metadata and can tell the user
  whether the visible overlays are bounded projections.
- Completed new runs can provide deterministic page-by-page overlay geometry
  without asking indicators to reconstruct history or making the UI authoritative.
- Missing events, transport gaps, and compaction are visible as incomplete
  evidence rather than silently reduced geometry.
- Removing `StrategySeries.overlays`, the runtime trade-overlay registration,
  and legacy frontend suppression removes a second visual truth layer.
- The tradeoff is that BotLens overlays can be behind by up to the configured
  bar cadence. That is acceptable because overlays are debugger/read-model
  state, not execution truth.

## References

- [Runtime Contract](../../contracts/platform/01_runtime_contract.md)
- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [BotLens Projection Boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
- [ADR 0008](0008-treat-botlens-as-projection-debugger.md)
- [ADR 0028](0028-use-bounded-projection-dispatch-for-botlens-live-facts.md)
