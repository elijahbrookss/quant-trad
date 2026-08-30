---
component: adr-execution-profiles-runtime-instrument-authority
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - execution
  - instruments
  - runtime
  - reporting
code_paths:
  - src/engines/bot_runtime/core/execution_profile.py
  - src/engines/bot_runtime/core/execution_context.py
  - src/engines/bot_runtime/core/domain/engine.py
  - src/engines/bot_runtime/strategy/series_builder_parts/series_construction.py
  - portal/backend/service/bots/config_service.py
  - portal/backend/service/market/instrument_service.py
  - portal/backend/service/reports/contract.py
  - cli/main.py
---
# ADR 0027: Use Execution Profiles As Runtime Instrument Authority

## Status

Accepted on 2026-05-25.

## Context

Research needs to compare derivative instruments, spot instruments, and spot
sources used as derivative proxies without hiding what market data was actually
used. Legacy bot config carried an `instrument_type` policy that tried to gate
spot versus derivative runs at bot startup. That mixed source identity with
execution modeling and pushed tick, contract, fee, and margin reads into several
runtime branches.

That shape made proxy-derivative research possible only through transitional
template overlays. It also made diagnostics ambiguous: the runtime could trade
against spot candles while reporting derivative-like behavior without one clear
contract saying which fields governed execution.

## Decision

Use `SeriesExecutionProfile` as the runtime compiler and compatibility authority
for instrument execution fields.

The canonical instrument record owns source identity:

- `instrument_type` describes the market-data source.
- `datasource`, `exchange`, symbol, and instrument id route candles.
- Spot instruments may carry proxy-derivative evidence copied from a validated
  derivative sibling, but the source instrument remains spot.

Runtime owns execution binding:

- `execution_semantics=spot` uses spot/full-notional semantics.
- `execution_semantics=derivative` uses derivative margin semantics.
- `execution_semantics=proxy_derivative` uses spot source candles with explicit
  derivative-style execution evidence for backtest research.

`LadderRiskEngine` must read tick size, contract size, tick value, fees, amount
constraints, quote currency, and margin model from the compiled execution
profile. ATM templates remain strategy/risk templates and must not carry
instrument execution metadata.

Execution-context addendum, accepted 2026-08-05: the immutable run-scoped
authority is
now `ResolvedExecutionContext`. `SeriesExecutionProfile` compiles the
instrument, risk, margin, and legacy fee inputs used to resolve that context;
it must not expand into a venue, fee-tier, fill-model, and calibration monolith.
Venue rules, fee schedules, and model evidence have separate versioned and
hashed contracts, as recorded by ADR 0056.

Bot startup no longer applies a bot-level spot/derivative source gate. It
resolves each linked canonical instrument, chooses or honors explicit
`execution_semantics`, compiles a profile, and fails loud if the profile cannot
prove the required execution fields. `proxy_derivative` is admitted only for
backtest runs until paper/live proxy execution is separately modeled.

Reports and CLI surfaces must expose the mixed semantics instead of smoothing
them away. Run reports include instrument semantics, and `qt` exposes
instrument profiles plus per-symbol report summaries so agents can inspect
signals, decisions, opened positions, margin rejects, and performance without
manual SQL.

## Consequences

- Strategies and historical bots can run mixed source instruments when each
  instrument has a valid execution profile.
- Spot proxy backtests remain truthful: source type is spot, execution semantics
  is proxy-derivative, and report caveats can say so.
- Execution field reads have one home, reducing template and risk-engine drift.
- Existing profile callers remain compatible, while new venue, fee, and model
  facts flow through the pinned resolved context.
- A missing proxy margin rate, proxy contract field, or derivative margin model
  is an admission failure, not an implicit spot fallback.
- Paper/live support for proxy semantics remains a future execution-adapter
  feature rather than an accidental side effect of source data availability.

## References

- [Runtime Contract](../../contracts/platform/01_runtime_contract.md)
- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
- [Data Boundary](../data/DATA_BOUNDARY.md)
- [ADR 0056: Pin venue-neutral execution contexts per run](0056-pin-venue-neutral-execution-contexts-per-run.md)
