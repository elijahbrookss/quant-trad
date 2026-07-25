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
  - leasing
  - wallet
  - deterministic
code_paths:
  - src/engines/bot_runtime
  - src/engines/bot_runtime/runtime/components/canonical_facts.py
  - portal/backend/service/bots/bot_watchdog.py
  - portal/backend/service/bots/runner_observability.py
  - portal/backend/service/bots/run_lease.py
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
- runtime diagnostics and fallback events,
- bounded BotLens projection/debug facts that never become execution authority.

Runtime separates source identity from execution modeling:

- `instrument_type` comes from the canonical instrument record and describes the
  market-data source.
- `execution_semantics` describes how the bot runtime models orders, shorts,
  wallet effects, and margin for that run.
- `SeriesExecutionProfile` is the single runtime authority for tick size,
  contract size, tick value, fees, amount constraints, quote currency,
  collateral model, and margin calculator.
- `LadderRiskEngine` consumes those values from the compiled profile, not from
  bot config, ATM templates, or ad hoc instrument dictionary lookups.
- `proxy_derivative` is a backtest research binding where a spot source remains
  labeled as spot while runtime applies derivative-style execution semantics.
- Startup readiness and reports must carry both source instrument type and
  execution semantics so mixed spot/perp research windows are explicit.
- Proxy-derivative execution must compile from explicit derivative evidence
  such as `proxy_derivative_margin_rates`,
  `proxy_derivative_instrument_fields`, or a validated derivative reference.
  Missing evidence is an admission failure; runtime must not silently fall back
  to spot full-notional accounting or spot quantity constraints.
- ATM templates are strategy/risk templates. They must not be used to patch
  missing instrument execution fields for runtime admission.
- Series construction routes candles through the linked instrument identity.
  Strategy-level provider fields are fallback defaults, not a reason to fetch a
  spot proxy from the derivative venue.
- Series construction must fail on provider `ingestion_failure` candle evidence.
  It may carry sparse-source classifications into diagnostics, but it must not
  accept a truncated replay as a completed backtest window.

## Position Lifecycle And Order Semantics

ATM templates declare position lifecycle intent; runtime executes that intent.
The ATM boundary accepts only schema-v2 snake-case policy fields and rejects
unknown or malformed input. Instrument constraints, fees, currencies, and
margin evidence belong exclusively to `SeriesExecutionProfile`. Runtime
compiles the normalized template once into a `RuntimeExecutionPlan` before
constructing execution state. The plan owns
entry, initial-stop, take-profit, fixed-horizon, breakeven, trailing, and
stop-adjustment semantics. The engine projects target dictionaries only from
that plan; position state consumes resolved runtime policy objects rather than
raw template dictionaries.

Strategy and standalone ATM-template write paths compile this same plan before
persistence so semantically invalid templates cannot wait until run startup to fail.

The canonical policy fields are:

- `exit_plan.fixed_horizon`: close remaining open legs after `bars` completed
  position bars at the strategy bar close. This is a market/taker close and
  emits a `fixed_horizon` exit fill plus a close reason of `FIXED_HORIZON`.
- `take_profit_orders`: stable target IDs, exactly one price expression per
  target, and explicit `size_fraction` values totaling one.
- `stop_adjustments`: flattened, stable-ID one-time stop movement rules such as
  move-to-breakeven at a configured R multiple, an absolute trigger tick value,
  or after a target hit. Runtime converts them into resolved stop-adjustment
  objects before opening a position. Omitting this field means no stop
  adjustment; runtime never inserts a move-to-breakeven rule implicitly.
- `breakeven`: direct breakeven activation for simple strategies when explicit
  stop adjustments are not configured.
- `trailing`: trailing-stop activation and distance config. With
  `activation_type=r_multiple`, `r_multiple` defines activation and `ticks` or
  `atr_multiplier` defines distance. With `activation_type=target_hit`, the
  target ID or index must resolve during compilation. A trailing stop may only
  tighten in the favorable direction; it must never loosen an existing stop.

Unsupported enums, non-finite or out-of-range numbers, contradictory fixed
horizons, duplicate targets, invalid allocation fractions, unresolved target
references, and incomplete stop adjustments are admission failures.

Runtime maps exit event types to liquidity roles:

- target fills represent resting take-profit limits and use maker fees,
- stop fills represent stop-market exits and use taker fees,
- fixed-horizon and terminal closes are market closes and use taker fees.

Limit-maker entries are post-only. If a submitted maker entry would cross the
current reference price immediately, runtime rejects it with
`POST_ONLY_WOULD_CROSS`. Limit-maker entries are submitted from the signal
close and cannot fill from the already-known signal bar range. Once a maker
order is accepted as resting, later bars may fill it as maker liquidity when
price trades through the limit for the configured validity window.

Execution profiles remain the fee and instrument authority. Templates may
request order style and exit behavior, but they must not patch missing
instrument fee, tick, quantity, or margin fields.

Runtime supports only `signal_price` as the immediate entry anchor. Entry timing
beyond current signal-close submission is not hidden behind a price anchor. A
true next-bar entry model requires its own pending signal-entry lifecycle so
reports can distinguish when the signal was known from when the order became
executable.

Executable fills use `FillOrder` semantics: side, quantity, price, order type,
liquidity role, price source, and fee rate are known before the adapter applies
the fill. The older `fill_market` adapter method remains only as a compatibility
facade for adapters that have not yet implemented direct order execution.

## Slippage Modeling Gap

Slippage is not yet empirically calibrated. Runtime has deterministic execution
hooks, but Quant-Trad does not yet have enough paper/live fill evidence to
model symbol-, venue-, regime-, order-type-, and size-sensitive slippage with
confidence.

Until that evidence exists, slippage assumptions must be explicit and bounded.
They must not be buried inside maker/taker fee logic, stop logic, or report
summaries. Future slippage models should attach to the execution-policy
boundary after order type, liquidity role, and fallback behavior are known.

## Diagram Walkthrough: Runtime Hot Path

[runtime-hot-path.mmd](diagrams/runtime-hot-path.mmd) shows one run:

1. Runtime prepares dependencies, strategy series, indicators, wallet context, and persistence collaborators.
2. Warmup advances state without creating trade truth.
3. The per-bar loop advances indicator snapshots and decision evaluation.
4. Execution core resolves FAST/FULL behavior, intrabar fallback, fills, fees, margin, and settlement.
5. Runtime emits events and persists trade/run facts.
6. Runtime emits compact BotLens push facts without building visual overlay
   geometry.
7. Visual overlay projection runs as a separate bounded projection step when
   its bar cadence is due.
8. Projections and reports consume those facts downstream.

Hot-path payloads should stay compact. Detailed debug and history belong on
cold paths or bounded projection steps.

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

Watchdog expired-lease detection is recoverable lifecycle degradation unless
there is independent evidence that the runtime process actually reached a
terminal failure. Container-not-running and startup/process failures remain
terminal only when the watchdog can verify the container belongs to the run it
is evaluating and startup launch grace has expired. A fixed-name container from
an older run is startup ambiguity, not proof that the new run crashed.
Recoverable watchdog conditions should produce degraded operational health with
context, not `RUN_FAILED` or an unclassified terminal fault. Watchdog lifecycle
rows should include bounded diagnostics such as lease expiry age, previous
runner, detecting runner, runner clock gap evidence, and nearby container
lifecycle evidence when those facts are available.

Run ownership is leased per `run_id`. The backend acquires a run lease before
launching the runner, the runtime renews that lease while it is alive, and clean
terminal exit releases it. `portal_bot_run_leases` is the liveness and ownership
source; `portal_bots` remains a bot definition row and must not carry
`runner_id`, heartbeat, status, summary, or artifact state. Runtime processes
must fail loud if they lose the lease or cannot renew it before continuing to
emit run facts.

## Execution Semantics

FAST and FULL are execution semantics, not playback modes.

- FAST uses strategy timeframe OHLC and pessimistic same-bar handling.
- FULL uses lower-timeframe intrabar data when available.
- Missing execution mode defaults to FAST; all non-`fast`/`full` values fail.
- Playback values are never interpreted as execution values.
- Missing/incomplete/ambiguous intrabar data falls back to pessimistic behavior with diagnostics.
- UI animation can replay events, but it must not change execution truth.

## Evidence Runtime Must Leave Behind

Runtime truth includes decisions, rejected decisions, fills, fees, trade state,
wallet reservations, margin effects, terminal closes, lifecycle transitions,
and domain events. BotLens snapshots, fleet cards, API transport shapes, and
report views are projections over that truth. Projection state may be rebuilt or
unavailable; runtime truth should remain durable and inspectable.

Performance diagnostics are supporting evidence, not execution truth. Step
traces may be batched and lag the hot path, but they must flush before a run is
considered fully finalized or surface a diagnostic if they cannot be drained.

Canonical BotLens facts are required runtime evidence, so they use a stricter
buffer than step traces. Runtime may enqueue sequenced canonical fact batches
off the bar hot path and write them in bounded DB batches, but the queue must
not drop rows. Terminal completion requires draining that buffer after the final
status push. Queue overflow, write failure, or drain timeout fails the run
instead of silently producing a report from partial canonical facts.

Live BotLens projection dispatch happens after the sequenced fact append. The
bar step may build the compact fact batch from the current runtime timeline,
assign its producer sequence, and enqueue the committed batch to a bounded
projection dispatcher. It must not wait for websocket subscriber fanout or
projector transport work before continuing execution. The dispatcher consumes
the already stamped batch from that bar's known-at snapshot; it does not rebuild
state from mutable runtime internals.

Visual overlay projection is separate from ordinary runtime push dispatch.
`_push_update` does not materialize indicator overlay geometry and does not
read or write a `StrategySeries.overlays` cache. After bar finalization,
runtime may run an `overlay_projection` step that snapshots indicator visual
state, diffs the bounded overlay cache, and emits overlay deltas only when the
projected viewport changed. Overlay projection pressure degrades BotLens
overlay freshness; it must not change decisions, fills, wallet effects,
reports, or execution completion.

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
- Visual overlays are bounded BotLens projection state, not runtime series
  truth.
- Shared-wallet and symbol-sharded paths must preserve deterministic ordering.
- Heavy debug/history reads are cold-path behavior.

## Related Docs

- [Engine state model](../engine/ENGINE_STATE_MODEL.md)
- [Decision layer boundary](../decision-layer/DECISION_LAYER_BOUNDARY.md)
- [Wallet and capital boundary](WALLET_AND_CAPITAL_BOUNDARY.md)
- [Runtime composition root](RUNTIME_COMPOSITION_ROOT.md)
- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [BotLens projection boundary](../botlens-projections/BOTLENS_PROJECTION_BOUNDARY.md)
