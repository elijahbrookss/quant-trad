---
component: execution-runtime-boundary
subsystem: execution-runtime
layer: boundary
doc_type: architecture
status: active
tags:
  - runtime
  - execution
  - lifecycle
  - wallet
  - deterministic
code_paths:
  - src/engines/bot_runtime
  - portal/backend/service/bots/bot_watchdog.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/bots/runtime_dependencies.py
  - portal/backend/service/bots/startup_lifecycle.py
  - docs/architecture/execution-runtime/diagrams/runtime-hot-path.mmd
  - docs/architecture/execution-runtime/diagrams/runtime-lifecycle-state.mmd
---
# Execution Runtime Boundary

## Purpose

The execution runtime is the source of truth for bot runs. It owns walk-forward execution, deterministic ordering, execution modes, fills, fees, margin, wallet effects, settlement, lifecycle transitions, and runtime event emission.

Related diagrams:

- [runtime-hot-path.mmd](diagrams/runtime-hot-path.mmd)
- [runtime-lifecycle-state.mmd](diagrams/runtime-lifecycle-state.mmd)

## Boundary Contract

Runtime owns execution truth. BotLens, reports, observability, and frontend state are projections over runtime facts.

Runtime consumes:

- provider-backed market series,
- typed indicator outputs,
- decision artifacts,
- bot/strategy/instrument config,
- wallet and execution-mode settings.

Runtime emits:

- accepted/rejected decision events,
- trade lifecycle rows/events,
- fee, margin, wallet, and settlement effects,
- lifecycle checkpoints,
- runtime diagnostics and fallback events.

## Diagram Walkthrough: Runtime Hot Path

[runtime-hot-path.mmd](diagrams/runtime-hot-path.mmd) shows one run:

1. Runtime prepares dependencies, strategy series, indicators, wallet context, and persistence collaborators.
2. Warmup advances state without creating trade truth.
3. The per-bar loop advances indicator snapshots and decision evaluation.
4. Execution core resolves FAST/FULL behavior, intrabar fallback, fills, fees, margin, and settlement.
5. Runtime emits events and persists trade/run facts.
6. Projections and reports consume those facts downstream.

Hot-path payloads should stay compact. Detailed debug and history belong on cold paths.

## Diagram Walkthrough: Lifecycle State

[runtime-lifecycle-state.mmd](diagrams/runtime-lifecycle-state.mmd) shows startup and terminal states:

- startup phases prove the container, config, series, and first snapshot are available,
- live means runtime has first usable runtime truth,
- degraded means partial recoverable failure,
- terminal states stop execution and preserve failure/completion context.

Frontend status should derive from lifecycle/projection facts, not client guesses.

Lifecycle terminal states are monotonic for golden-run validation. A true
terminal failure (`failed`, `crashed`, `startup_failed`, or equivalent) cannot be
silently overwritten by a later completion. If durable facts contain both
completion and an unclassified terminal failure/fault, reporting must expose a
lifecycle contradiction and block golden-run certification.

Watchdog stale-heartbeat detection is recoverable lifecycle degradation unless
there is independent evidence that the runtime process actually reached a
terminal failure. Container-not-running and startup/process failures remain
terminal only when the watchdog can verify the container belongs to the run it
is evaluating and startup launch grace has expired. A fixed-name container from
an older run is startup ambiguity, not proof that the new run crashed.
Recoverable watchdog conditions should produce degraded operational health with
context, not `RUN_FAILED` or an unclassified terminal fault.

## Execution Semantics

FAST and FULL are execution semantics, not playback modes.

- FAST uses strategy timeframe OHLC and pessimistic same-bar handling.
- FULL uses lower-timeframe intrabar data when available.
- Missing/incomplete/ambiguous intrabar data falls back to pessimistic behavior with diagnostics.
- UI animation can replay events, but it must not change execution truth.

## State And Truth

Runtime truth includes decisions, rejected decisions, fills, fees, trade state, wallet reservations, margin effects, terminal closes, lifecycle transitions, and domain events.

Runtime projections include BotLens snapshots, fleet cards, API transport shapes, and report views. Projection state may be rebuilt or unavailable; runtime truth should remain durable and inspectable.

Runtime performance diagnostics are supporting evidence, not execution truth.
Step traces may be batched and lag the hot path, but they must flush before a
run is considered fully finalized or surface a diagnostic if they cannot be
drained.

Canonical BotLens facts are required runtime truth, so they use a stricter
buffer than step traces. The runtime may enqueue sequenced canonical fact
batches off the bar hot path and write them in bounded DB batches, but the queue
must not drop rows. Terminal completion requires draining that buffer after the
final status push. Queue overflow, write failure, or drain timeout fails the run
instead of silently producing a report from partial canonical facts.

## Failure And Recovery

- Invalid config fails before execution.
- Missing source series fails or degrades with explicit context.
- Ambiguous intrabar execution uses contract-defined pessimistic fallback and emits diagnostics.
- Runtime exceptions become lifecycle/runtime events and terminal state.
- Projection/storage failures should surface as degraded or unavailable states without fabricating execution.

## Invariants

- All bot runs are walk-forward.
- Known-at timing governs indicators, decisions, and execution.
- Runtime truth does not come from frontend playback.
- Shared-wallet and symbol-sharded paths must preserve deterministic ordering.
- Heavy debug/history reads are cold-path behavior.

## Related Docs

- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Decision layer boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)
- [Wallet and capital boundary](WALLET_AND_CAPITAL_BOUNDARY.md)
- [Runtime composition root](RUNTIME_COMPOSITION_ROOT.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
