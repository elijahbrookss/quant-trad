---
component: paper-engine-v1
subsystem: execution-runtime
layer: design
doc_type: architecture
status: draft
tags:
  - paper
  - execution
  - runtime
  - providers
  - market-data
  - botlens
code_paths:
  - src/engines/bot_runtime
  - src/data_providers
  - src/data_providers/streams
  - src/engines/bot_runtime/live_market.py
  - portal/backend/controller/providers.py
  - portal/backend/service/bots
  - portal/backend/service/bots/market_data_stream_policy.py
  - portal/backend/service/bots/paper_market_stream.py
  - portal/backend/service/market
  - portal/backend/service/providers
  - cli/main.py
  - docs/guides/coinbase-derivatives-paper-setup.md
---
# Paper Engine V1 Design

## Purpose

Define the smallest clean architecture for provider-backed paper trading.

The paper engine should let Quant-Trad run against live market data without
placing live orders. It is an execution-runtime capability, not a strategy
feature and not a provider-specific strategy mode.

The first operational slice was `paper` + `execution_behavior=observe-only`.
The first execution slice is now normal `paper` + `execution_behavior=simulated`
inside the Docker bot runtime: the worker warms indicators from historical
candles, subscribes to live provider candle streams, aggregates closed live
candles into the strategy timeframe, and advances the existing runtime state
engine with the existing `PaperAdapter`.

## Core Invariant

Paper runtime consumes canonical provider facts and emits normal runtime facts.

It must not change:

- strategy decision semantics,
- wallet semantics,
- order semantics,
- trade semantics,
- fee semantics,
- slippage semantics,
- report DTO semantics,
- BotLens projection semantics.

## Recommended Design

Build a provider-agnostic paper runtime path with Coinbase as the first concrete
market-data provider.

The boundary should look like this:

```text
Coinbase WebSocket / REST
  -> Coinbase provider adapter
  -> canonical market data events and instrument metadata
  -> paper runtime loop
  -> existing execution adapter / risk / wallet / trade lifecycle
  -> runtime event ledger
  -> BotLens and reports
```

Provider-specific behavior belongs only in provider adapters. Paper execution
belongs in the runtime boundary and should work with any provider that can
produce canonical instrument metadata and market events.

## Existing State

Current strengths:

- `src/data_providers/providers/coinbase.py` already maps Coinbase products,
  futures metadata, candle REST data, fee rates, margin rates, funding fields,
  and instrument type.
- `src/data_providers/streams` defines the first canonical read-only stream
  contracts and Coinbase public WebSocket parser/adapter.
- `POST /api/providers/stream-smoke` and `qt providers stream-smoke` provide a
  bounded operator/agent smoke surface for live stream checks.
- `qt bots start --run-type paper --execution observe-only` starts a bounded
  Docker-backed live intake run for bots whose strategy instruments resolve to
  Coinbase Direct products.
- `qt bots start --run-type paper --execution simulated` starts the normal
  Docker runtime and wires provider stream candles into the existing paper
  execution path.
- `src/engines/bot_runtime/live_market.py` owns closed live-candle aggregation
  and buffering for paper workers.
- `portal/backend/service/bots/paper_market_stream.py` owns container-local
  provider stream intake for paper workers.
- `portal/backend/service/bots/market_data_stream_policy.py` owns the
  bot-level reconnect policy used by paper/live market-data streams.
- Terminal paper runs persist stream diagnostics under
  `run.summary.paper_market_stream`.
- `InstrumentMetadata` already provides a canonical contract shape for runtime
  sizing and execution.
- `PaperAdapter` already exists and delegates to the backtest fill adapter.
- Runtime composition has an explicit `paper` mode seam.
- BotLens already treats runtime facts as projection input, not source truth.

Current gaps:

- The first live-paper provider is Coinbase Direct only.
- The paper runtime advances on closed strategy-timeframe candles. In-progress
  provider prices are projected to BotLens as display-only provisional candles;
  they are not execution/runtime truth.
- REST comparison of locally aggregated live candles is still future audit
  instrumentation.
- Coinbase provider metadata advertises order support, but there is no
  Coinbase-specific live order adapter in the runtime boundary.
- BotLens has backtest-shaped assumptions that need live current-candle
  projection support.

## Observe-Only Paper V1

Observe-only is a bot execution behavior, not a strategy variant.

```bash
qt bots start <bot_id> \
  --run-type paper \
  --execution observe-only \
  --duration-seconds 30
```

The backend resolves the normal bot startup artifacts, freezes the effective
bot/strategy snapshot in the run config, and launches the normal Docker runtime
container. The container reads the run snapshot, recognizes
`execution_behavior=observe-only`, derives stream subscriptions from the
strategy's instrument links, and runs the observe-only market intake loop.

Observe-only invariants:

- instruments are the canonical source for provider product IDs;
- provider-specific channel names stay inside the stream adapter boundary;
- the bot run still goes through the normal Docker container route;
- no strategy decision evaluation runs;
- no orders, fills, trades, fees, slippage, or wallet mutations are produced;
- run summaries explicitly report `orders_submitted=0`, `fills_recorded=0`,
  and `wallet_mutations=0`;
- terminal lifecycle state is recorded by the runtime container.

This mode is for operational validation: live connectivity, product mapping,
message parsing, lifecycle recording, and long-running resource behavior. It is
not a profitability or execution-quality test.

## Streaming Paper V1

Streaming paper is a bot run, not a separate command surface:

```bash
qt bots start <bot_id> \
  --run-type paper \
  --execution simulated \
  --duration-seconds 86400
```

The backend still resolves and freezes the normal run snapshot before launching
the Docker runtime container. Inside each symbol worker:

1. Runtime series are built from the frozen strategy and instrument snapshot.
2. Historical candles seed the indicator runtime only.
3. The worker starts a provider stream derived from the prepared runtime series.
4. Provider `market_candle_update` events are aggregated into closed candles for
   the strategy timeframe.
5. Provider ticker/candle updates also produce a throttled provisional candle
   for BotLens display only.
6. Closed live candles are appended to the existing `BotRuntime` series.
7. The normal runtime loop evaluates indicators, strategy rules, risk, paper
   fills, wallet effects, trades, runtime events, BotLens facts, and reports.

The important semantic distinction is that historical seed candles do not
create paper trades in this streaming path. They exist to make indicator state
known before the first live runtime candle.

Streaming paper invariants:

- no Coinbase payload fields are strategy inputs;
- no live orders are placed;
- paper fills remain behind the existing `PaperAdapter`;
- wallet, order, trade, fee, and slippage semantics are unchanged;
- closed live candles are immutable once admitted to the worker store;
- provisional candles are emitted through BotLens live transport only and must
  not enter runtime series, indicators, strategy decisions, reports, or replay
  truth;
- transient provider stream disconnects reconnect within the configured bot
  `market_data_stream_policy` and remain operational diagnostics only;
- provider stream failure marks the live candle store failed only for fatal
  provider/config/contract errors or when the continuous disconnect budget is
  exhausted.

## Market Data Stream Policy

`market_data_stream_policy` is bot runtime configuration. It is not strategy,
variant, ATM, sizing, wallet, order, fee, or slippage configuration.

The resolved policy is frozen into the run snapshot:

```text
bot.market_data_stream_policy
  + run-start market_data_stream_policy override
  -> config_snapshot.bot.market_data_stream_policy
  -> worker PaperMarketStreamRunner
```

Default policy:

```json
{
  "reconnect_enabled": true,
  "initial_backoff_seconds": 1.0,
  "max_backoff_seconds": 60.0,
  "continuous_disconnect_budget_seconds": 900.0,
  "heartbeat_stale_seconds": 30.0
}
```

Recovered reconnects do not change run lifecycle status. They are recorded in
`run.summary.paper_market_stream` as diagnostics:

- `disconnect_count`
- `reconnect_attempt_count`
- `reconnect_success_count`
- `total_disconnected_seconds`
- `max_continuous_disconnected_seconds`
- recent reconnect events and last disconnect reason

The worker degrades terminally only when a fatal provider/config/contract error
occurs or when the stream stays disconnected longer than
`continuous_disconnect_budget_seconds`.

## Boundary Ownership

| Boundary | Owns | Does Not Own |
| --- | --- | --- |
| Provider adapter | external REST/WebSocket quirks, credentials, product metadata, stream parsing | strategy rules, paper fills, wallet effects |
| Canonical market event layer | normalized event shape, provider sequence health, receive timestamps | order lifecycle, report materialization |
| Paper runtime | market-driven runtime loop, simulated fills, runtime events | provider channel names, provider payload details |
| BotLens | projections over runtime facts and current market state | source-data ingestion, execution truth |
| Reports | run/research materialization from persisted facts | live stream control, strategy mutation |

## Minimal Contracts

### Provider Market Stream

Suggested interface:

```python
class ProviderMarketDataStream(Protocol):
    async def connect(self) -> None: ...
    async def subscribe(self, subscriptions: Sequence[MarketSubscription]) -> None: ...
    async def events(self) -> AsyncIterator[CanonicalMarketEvent]: ...
    async def close(self) -> None: ...
```

This is a provider boundary. It may be implemented by Coinbase first, then other
providers later.

### Market Subscription

Fields:

- `provider`
- `venue`
- `symbol`
- `product_id`
- `channels`
- `timeframe`
- `auth_mode`

### Canonical Market Event

Common fields:

- `event_id`
- `event_kind`
- `provider`
- `venue`
- `symbol`
- `product_id`
- `provider_sequence_num`
- `provider_event_time`
- `received_at`
- `payload`
- `raw_ref`

Initial event kinds:

- `provider_connected`
- `provider_disconnected`
- `provider_subscription_ack`
- `provider_heartbeat`
- `provider_sequence_gap`
- `market_ticker`
- `market_candle_update`
- `market_candle_finalized`

### Paper Execution Adapter

Paper execution should continue to implement the existing `ExecutionAdapter`
boundary. It can remain a deterministic simulated fill adapter while the
runtime loop changes from historical series iteration to live market-event
driven iteration.

Do not add Coinbase order methods to the paper adapter.

## Full Paper Runtime Flow

1. Resolve bot config, strategy config, instrument metadata, wallet, risk, and
   paper execution profile.
2. Build provider stream subscriptions for the selected symbols.
3. Start provider stream and append connection/subscription events.
4. Convert provider messages into canonical market events.
5. Update current market state and candle builder from canonical events.
6. On closed or usable candles, advance the runtime timeline:
   `initialize -> apply_bar -> snapshot`.
7. Evaluate decisions, risk, fills, fees, wallet effects, and trade lifecycle
   through existing runtime semantics.
8. Emit runtime events to the retained ledger.
9. Project BotLens state from runtime facts and current market state.
10. On stop/failure, close streams, persist lifecycle state, and leave reports
    rebuildable from persisted facts.

## Coupled Layers To Watch

### Provider Capability Metadata

Coinbase currently advertises `supportsOrders: true`. That is not the same as
having a runtime live order adapter. Split provider capabilities later into
data, metadata, stream, paper-compatible, and live-order capabilities so UI and
CLI do not imply execution support that does not exist.

### REST Provider And Stream Provider

The current Coinbase provider is REST/SDK oriented. WebSocket streaming should
be added as a sibling provider capability or provider stream component, not by
making candle fetch code own long-running connection state.

### BotLens Current Candle State

BotLens can visualize live current-candle movement, but it should consume
canonical projection events. It should not subscribe to Coinbase directly.

### Paper Adapter Naming

The current `PaperAdapter` is really a simulated fill adapter. The provider-
backed paper engine needs a runtime runner around it. Avoid replacing a simple
adapter with a large engine object unless the runtime loop requires it.

## Options Considered

### Option 1: Coinbase-Specific Paper Runtime

Fastest to implement, but it couples runtime to Coinbase channel names and
payload details. This makes the second provider expensive and risks semantic
drift.

Recommendation: do not choose this as the main design.

### Option 2: Provider-Agnostic Paper Runtime With Coinbase Stream Adapter

Coinbase owns WebSocket parsing. Runtime consumes canonical events and
instrument metadata. This keeps strategy, risk, wallet, and reporting semantics
portable.

Recommendation: choose this for v1.

### Option 3: Full Live Execution Adapter First

This would add live order placement before paper stability is proven. It raises
wallet/order/trade safety risk and does not answer the immediate operational
research need.

Recommendation: defer.

## Implementation Phases

### Phase 1: Coinbase Read-Only Stream Adapter

- Add a Coinbase market-data stream component.
- Support `heartbeats`, `ticker`, and `candles`.
- Normalize messages into canonical market events.
- Track sequence gaps, reconnects, and heartbeat gaps.
- Do not touch runtime execution behavior.

Status: implemented for public Coinbase Advanced Trade streams.

### Phase 1.5: Observe-Only Bot Intake

- Start a bot in `paper` mode with `execution_behavior=observe-only`.
- Reuse normal bot/strategy/instrument resolution.
- Subscribe to provider streams derived from strategy instruments.
- Record lifecycle and compact run summaries.
- Preserve zero wallet/order/fill/trade effects.

Status: implemented as the first no-fill paper validation path.

### Phase 2: Market Event Recording And Current Candle Projection

- Persist or retain bounded canonical stream facts.
- Build current-candle state from candle updates.
- Feed BotLens through backend projection contracts, not direct provider
  payloads.

### Phase 3: Provider-Backed Paper Runtime Runner

- Wire provider streams into `paper` mode.
- Keep paper fills behind the existing execution adapter boundary.
- Preserve normal runtime event emission.
- Add explicit lifecycle states for stream connect, subscribed, running,
  degraded, stopped, and failed.

Status: implemented for Coinbase public candle streams feeding closed
strategy-timeframe candles into the existing runtime loop. Current-candle
BotLens projection and REST aggregation audits remain future work.

### Phase 4: 24-Hour Operational Run

- Run Coinbase paper mode for selected CDE products.
- Evaluate uptime, reconnects, heartbeat gaps, sequence gaps, memory, CPU,
  runtime lifecycle, report export, and BotLens health.
- Treat trading results as secondary until operational stability is proven.

### Phase 5: Private Streams And Live Preparation

- Add authenticated `user` and `futures_balance_summary` streams only after
  public stream and paper runtime are stable.
- Keep private stream state separate from paper simulated fills unless an
  explicit reconciliation feature is introduced.

## Observability Requirements

Log lifecycle boundaries with structured context:

- `provider`
- `venue`
- `symbol`
- `product_id`
- `run_id`
- `bot_id`
- `stream_session_id`
- `channel`
- `provider_sequence_num`
- `event_kind`
- `heartbeat_counter`
- `reconnect_count`
- `gap_count`

Required events:

- stream connection opened,
- subscription sent,
- subscription acknowledged,
- heartbeat received,
- sequence gap detected,
- reconnect started,
- reconnect completed,
- stream closed,
- paper runtime started,
- paper runtime stopped,
- paper runtime failed.

Do not log secrets or full credential payloads.

## Testing Plan

Add focused tests for:

- Coinbase WebSocket message parsing into canonical events,
- unknown Coinbase message types are ignored or surfaced as diagnostics without
  corrupting runtime state,
- heartbeat gap detection,
- sequence gap detection,
- reconnect state transitions,
- current-candle builder finalization,
- paper runtime consumes canonical candle events only,
- paper runtime preserves existing order/trade/wallet semantics,
- BotLens projection consumes canonical events rather than Coinbase payloads,
- report provenance identifies provider, venue, symbols, and paper mode.

## Things That Should Not Change

- Do not change strategy variants for paper mode.
- Do not change risk/ATM semantics for stream ingestion.
- Do not change wallet/order/trade/fee/slippage semantics.
- Do not make Coinbase payloads strategy inputs.
- Do not make BotLens the source of market truth.
- Do not place live orders in paper mode.
- Do not introduce a distributed queue or new service boundary for v1.

## Final Verdict

The clean path is a provider-agnostic paper runtime fed by canonical market
events, with Coinbase as the first concrete stream adapter. This preserves the
modular monolith shape: provider code normalizes external facts, runtime owns
execution truth, BotLens projects runtime state, and reports remain downstream
research artifacts.
