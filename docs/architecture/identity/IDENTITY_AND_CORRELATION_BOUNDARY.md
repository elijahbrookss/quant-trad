---
component: identity-correlation-boundary
subsystem: identity
layer: boundary
doc_type: architecture
status: active
tags:
  - identity
  - correlation
  - runtime-events
  - lineage
  - known-at
code_paths:
  - src/strategies/evaluator.py
  - src/strategies/compiler.py
  - src/engines/bot_runtime/core/runtime_events.py
  - portal/backend/db/models.py
  - portal/backend/service/bots/botlens_contract.py
  - docs/architecture/identity/diagrams/identity-key-relationships.mmd
---
# Identity And Correlation Boundary

## Purpose

The identity boundary explains how runtime truth is stitched together across strategy definitions, runs, symbols, decisions, trades, events, BotLens, reports, and logs.

Related diagram: [identity-key-relationships.mmd](diagrams/identity-key-relationships.mmd).

## Boundary Contract

Stable IDs are part of the runtime contract. They are not display labels and they should not be silently aliased for compatibility.

Core identifiers:

- `bot_id`: configured bot instance.
- `run_id`: one runtime execution of a bot.
- `strategy_id`: saved strategy definition.
- `strategy_hash`: compiled strategy material identity.
- `instrument_id`: strategy/runtime instrument binding.
- `series_key`: normalized symbol/timeframe series key.
- `signal_id`: emitted signal provenance.
- `decision_id`: evaluated strategy decision artifact.
- `trade_id`: trade lifecycle identity.
- `event_id`, `seq`, `correlation_id`, `root_id`: runtime event identity and replay/correlation support.

## Diagram Walkthrough

[identity-key-relationships.mmd](diagrams/identity-key-relationships.mmd) shows the identity chain:

1. A strategy definition compiles into a `strategy_hash`.
2. A bot starts a `run_id` with a strategy, instruments, and runtime config.
3. Each symbol/timeframe path has a normalized `series_key`.
4. Decision evaluation produces `decision_id` values tied to strategy, instrument, bar time, and rule context.
5. Signal events preserve `signal_id` provenance. `signal_id` and `decision_id` are intentionally distinct.
6. Accepted decisions may create or update a `trade_id`.
7. Runtime events carry hot fields for the IDs needed by BotLens, reporting, forensics, and logs.

## Truth And Projection

Identity truth lives in runtime/domain contracts and durable rows. Projection layers may index or cache by those IDs, but they should not mint replacement IDs for canonical objects.

Examples:

- BotLens can build a selected-symbol view keyed by `bot_id`, `run_id`, and `series_key`.
- Reports can group decisions and trades by `strategy_id`, `strategy_hash`, `instrument_id`, and `trade_id`.
- Observability can correlate latency or projection failures by `run_id`, `bot_id`, `bar_time`, and `correlation_id`.

## Failure Behavior

- Required runtime event IDs fail loud when missing.
- `signal_id` must not equal `decision_id`.
- Missing parent context should be explicit through parent-missing fields or diagnostics, not hidden by fallback IDs.
- Duplicate event IDs are idempotency/replay outcomes, not new domain facts.

## Invariants

- `run_id` scopes execution truth.
- `bot_id` scopes the configured bot owner.
- `strategy_hash` captures compiled material identity; `strategy_id` alone is not enough for reproducibility.
- `known_at`, `bar_time`, `event_time`, and `created_at` have different meanings and should not be conflated.
- Event hot fields must be sufficient for common queries without forcing every reader to parse full payload blobs.

## Known Gaps

- There is no single identity registry module. Identity rules currently live across strategy compilation/evaluation, runtime events, database models, and BotLens contracts.
- Some historical rows may not have every hot field populated. New code should prefer fail-loud behavior for required current contracts.

## Related Docs

- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Decision layer boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
