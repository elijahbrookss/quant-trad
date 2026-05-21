# Coinbase Derivatives Paper Setup

This guide defines the Coinbase-facing setup for the first provider-backed
paper engine path.

Status: first operator contract plus streaming paper execution. Public Coinbase
market-data WebSockets have been smoke-tested for CDE futures symbols, and
Quant-Trad now has both a Docker-backed observe-only intake path and a
provider-backed paper path that feeds closed live candles into the existing
runtime/paper-fill engine.

## Goal

Use Coinbase Advanced Trade market data as the first real venue feed for
provider-agnostic paper trading:

```text
Coinbase product metadata and WebSocket market data
  -> Quant-Trad provider boundary
  -> canonical instrument and market events
  -> paper runtime
  -> runtime event ledger, BotLens, reports
```

Paper mode must not place live orders. The initial objective is a stable
24-hour operational run. Observe-only validates streaming, instrument mapping,
lifecycle recording, and resource behavior without orders, fills, trades, fees,
slippage, or wallet mutations. Streaming paper then uses the same bot route to
run strategy evaluation and simulated fills from closed provider-derived
candles.

## Where The User Is Needed

You need to provide:

- a Coinbase account with access to the target products,
- a CDP API key name and private key if authenticated REST metadata, fees, or
  private streams are used,
- confirmation of the target product universe,
- a paper-run duration, such as 24 hours,
- a decision on whether to use only public market data first or authenticated
  metadata and account streams too.

Do not put Coinbase secrets in plan files, logs, docs, or strategy configs.
Use provider credential refs.

## Credential Storage In Quant-Trad

For authenticated Coinbase calls, store credentials under:

```text
provider_id: COINBASE
venue_id: COINBASE_DIRECT
required secrets:
  COINBASE_API_KEY
  COINBASE_API_SECRET
```

The current provider loads these from the encrypted provider credential store.
`QT_SECURITY_PROVIDER_CREDENTIAL_KEY` must be configured before saving or
loading provider credentials.

```bash
python -m cli.main providers credentials add --provider COINBASE --venue COINBASE_DIRECT
```

Recommended permission posture:

- public WebSocket market data: no API key required,
- authenticated product/candle/fee metadata: API key with read/view access,
- live order placement: trade permission, deferred and out of scope for paper
  v1.

Do not grant trade permission just to run the first paper engine validation.

## Official Coinbase Surfaces

Use the Advanced Trade API surfaces:

- REST endpoint base:
  `https://api.coinbase.com/api/v3/brokerage/{resource}`
- Public market-data WebSocket:
  `wss://advanced-trade-ws.coinbase.com`
- User/order WebSocket:
  `wss://advanced-trade-ws-user.coinbase.com`

Useful Coinbase docs:

- [Advanced Trade REST API endpoints](https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/rest-api)
- [List products](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/products/list-products)
- [Get product candles](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/products/get-product-candles)
- [WebSocket overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)
- [WebSocket channels](https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/websocket/websocket-channels)
- [WebSocket authentication](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-authentication)

## Product Discovery

For public discovery, use:

```text
GET /api/v3/brokerage/market/products?product_type=FUTURE
```

For one product:

```text
GET /api/v3/brokerage/market/products/{product_id}
```

Or with the authenticated product endpoint already used by the current provider:

```text
GET /api/v3/brokerage/products/{product_id}
```

Fields Quant-Trad needs from Coinbase futures products:

- `product_id`
- `product_type`
- `product_venue`
- `price_increment`
- `base_min_size`
- `base_max_size`
- `base_currency_id`
- `quote_currency_id`
- `future_product_details.contract_size`
- `future_product_details.contract_root_unit`
- `future_product_details.contract_expiry`
- `future_product_details.contract_expiry_type`
- `future_product_details.funding_interval`
- `future_product_details.funding_rate`
- `future_product_details.funding_time`
- `future_product_details.intraday_margin_rate`
- `future_product_details.overnight_margin_rate`
- `trading_disabled`, `cancel_only`, `limit_only`, `post_only`, `view_only`

Observed public discovery returned Coinbase FCM products such as:

| Product ID | Display | Venue | Expiry Type | Notes |
| --- | --- | --- | --- | --- |
| `BIP-20DEC30-CDE` | `BTC PERP` | `FCM` | `EXPIRING` | long-dated CDE futures product with funding fields |
| `ETP-20DEC30-CDE` | `ETH PERP` | `FCM` | `EXPIRING` | long-dated CDE futures product with funding fields |
| `XPP-20DEC30-CDE` | `XRP PERP` | `FCM` | `EXPIRING` | long-dated CDE futures product with funding fields |

Coinbase also exposes true `PERPETUAL` products with `product_venue=INTX`, such
as `BTC-PERP-INTX`. Do not mix INTX and FCM products under one research universe
unless the run metadata makes that venue distinction explicit. For a US-focused
v1, treat the `*-CDE` products as the first target and verify access at run
start.

## Candle Backfill

The public candle endpoint is:

```text
GET /api/v3/brokerage/market/products/{product_id}/candles
```

The authenticated product candle endpoint is:

```text
GET /api/v3/brokerage/products/{product_id}/candles
```

Required query fields:

- `start`: Unix timestamp seconds
- `end`: Unix timestamp seconds
- `granularity`: one of `ONE_MINUTE`, `FIVE_MINUTE`, `FIFTEEN_MINUTE`,
  `THIRTY_MINUTE`, `ONE_HOUR`, `TWO_HOUR`, `FOUR_HOUR`, `SIX_HOUR`, `ONE_DAY`
- `limit`: Coinbase documents a max of 350; the current Quant-Trad Coinbase
  provider chunks at 300, which is a conservative internal limit

The candle response shape is:

```json
{
  "candles": [
    {
      "start": "1639508050",
      "low": "140.21",
      "high": "140.21",
      "open": "140.21",
      "close": "140.21",
      "volume": "56437345"
    }
  ]
}
```

Backfill remains source-data ingestion. Runtime should consume persisted
normalized candles or a canonical live stream, not provider-specific REST
payloads.

## Public WebSocket Setup

For v1 public market data, connect to:

```text
wss://advanced-trade-ws.coinbase.com
```

Send one subscription message per channel:

```json
{"type":"subscribe","channel":"heartbeats"}
```

```json
{"type":"subscribe","channel":"ticker","product_ids":["BIP-20DEC30-CDE"]}
```

```json
{"type":"subscribe","channel":"candles","product_ids":["BIP-20DEC30-CDE"]}
```

Observed public messages included:

- `subscriptions` acknowledgements,
- `heartbeats` with `current_time` and `heartbeat_counter`,
- `ticker` snapshots and updates with price, 24h volume, high/low, best bid,
  best ask, and quantities,
- `candles` snapshots with `start`, `high`, `low`, `open`, `close`, `volume`,
  and `product_id`.

Coinbase documents candle WebSocket updates as five-minute buckets with updates
every second. If Quant-Trad needs a different runtime timeframe, aggregation
must be explicit and known-at safe.

Subscribe to `heartbeats` with market-data channels. Coinbase documents that
some channels may close after 60-90 seconds without updates, and heartbeats keep
subscriptions open during sparse updates.

Most market-data channels are public. Coinbase recommends authenticated
subscriptions for reliability, but the public ticker, candles, heartbeats,
status, market trades, level2, and ticker batch channels do not require
authentication.

Paper v1 intentionally subscribes only to `heartbeats`, `ticker`, and
`candles`. Public `market_trades` intake is deferred until there is a concrete
paper-model use for it.

Quant-Trad exposes a bounded read-only smoke command through the API-backed CLI:

```bash
qt providers stream-smoke \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --symbol BIP-20DEC30-CDE \
  --duration 10 \
  --channel heartbeats \
  --channel ticker \
  --channel candles
```

The command calls `POST /api/providers/stream-smoke` and returns a compact
`provider_stream_smoke.v1` payload with event counts, latest heartbeat/ticker/
candle samples, sequence diagnostics, unsupported message counts, and malformed
message counts.

## Observe-Only Bot Intake

After provider stream smoke succeeds, the first bot-level validation runs
through the normal bot CLI surface:

```bash
qt bots start <bot_id> \
  --run-type paper \
  --execution observe-only \
  --duration-seconds 30
```

This requires the bot's selected strategy to use Coinbase Direct as its
datasource/exchange and to have strategy instrument links whose instrument
metadata resolves to Coinbase product IDs. Instruments are the canonical source;
operators should not pass ad hoc channel/product wiring through bot commands.

Observe-only bot runs:

- resolve normal bot startup artifacts and strategy snapshots,
- derive Coinbase stream subscriptions from strategy instruments,
- start through the normal Docker bot runtime route,
- record lifecycle checkpoints and compact market-event counts,
- do not evaluate strategy decisions,
- do not produce orders, fills, trades, fees, slippage, or wallet mutations.

Use observe-only mode to prove that the selected bot can stay connected to live
provider data before trusting a longer paper run.

## Streaming Paper Bot Run

After observe-only succeeds, run the same bot through normal simulated paper
execution:

```bash
qt bots start <bot_id> \
  --run-type paper \
  --execution simulated \
  --duration-seconds 86400
```

This still goes through the Docker bot runtime route. The worker builds the
normal runtime series, uses historical candles only to warm indicator state,
starts the Coinbase public stream, aggregates Coinbase five-minute candle
updates into closed candles for the strategy timeframe, and appends those closed
candles to the existing runtime loop.

Important caveats:

- the first implemented stream provider is `COINBASE` / `COINBASE_DIRECT`;
- the runtime advances on closed strategy-timeframe candles, not every ticker
  update;
- current in-progress candle animation in BotLens is still future work;
- REST-vs-stream candle aggregation audits are still future work;
- public `market_trades` intake is intentionally out of scope for v1;
- no Coinbase order API calls are made in paper mode.

## Authenticated WebSocket Setup

Authenticated WebSocket subscriptions require a short-lived JWT. Coinbase
documents WebSocket JWT expiry at two minutes. The private-key newlines must be
preserved when generating JWTs.

Private channels relevant later:

- `user`: authenticated open order and order update stream
- `futures_balance_summary`: authenticated futures balance updates

These are not required for paper v1 unless paper mode intentionally compares
against actual account state. For a pure provider-agnostic paper engine, keep
private account streams out of the first pass.

## Existing Quant-Trad State

Current code already has a Coinbase direct provider:

- `src/data_providers/providers/coinbase.py`

It currently:

- loads credentials from the active `COINBASE` / `COINBASE_DIRECT` credential ref,
- validates Coinbase products through the Advanced Trade SDK,
- maps `FUTURE` products to `InstrumentType.FUTURE`,
- maps futures contract size, tick size, expiry, funding, margin rates, and
  fee rates into `InstrumentMetadata`,
- fetches candles through REST and normalizes them to timestamp/OHLCV rows.

Current paper execution is provider-backed for the streaming path:

- `src/engines/bot_runtime/adapters/paper.py` delegates to the backtest fill
  adapter.
- `src/engines/bot_runtime/strategy/series_builder_parts/series_construction.py`
  selects `PaperAdapter` for `run_type == "paper"`.
- `src/engines/bot_runtime/live_market.py` aggregates provider stream candle
  updates into immutable closed runtime candles.
- `portal/backend/service/bots/paper_market_stream.py` owns container-local
  Coinbase stream intake for paper workers.
- `portal/backend/service/bots/runtime_composition.py` has a paper composition
  seam while the Docker worker owns live stream wiring.
- `portal/backend/service/bots/observe_only_runtime.py` provides the first
  container-owned `paper` + `execution_behavior=observe-only` bot intake path.
- Simulated paper terminal runs persist stream diagnostics in
  `summary.paper_market_stream`, including per-worker event counts, closed
  candle store counts, ignored snapshot counts, and incomplete aggregate drops.

That means provider-backed paper execution is now a runtime composition slice,
not a new strategy engine. The remaining work is current-candle projection,
REST aggregation auditing, and later private/user stream reconciliation.

## Canonical Event Mapping

Do not let Coinbase payloads leak into runtime, strategies, or BotLens.

Map Coinbase stream messages into small canonical events first:

| Coinbase Input | Canonical Quant-Trad Event |
| --- | --- |
| WebSocket open/close/reconnect | `ProviderConnectionEvent` |
| `subscriptions` message | `ProviderSubscriptionAck` |
| `heartbeats` | `ProviderHeartbeatEvent` |
| `ticker` | `MarketTickerEvent` |
| `candles` | `MarketCandleUpdateEvent` |
| sequence gap/out-of-order message | `ProviderSequenceHealthEvent` |

Canonical event fields should include:

- `provider`
- `venue`
- `product_id`
- `symbol`
- `timeframe` when applicable
- `provider_sequence_num`
- `provider_event_time`
- `received_at`
- `event_kind`
- normalized price/size/candle fields
- `raw_ref` or bounded raw payload reference for diagnostics

The event schema should tolerate new Coinbase message types by ignoring
unsupported messages with a structured debug or warning event, not by crashing
on unknown fields.

## Paper Engine Requirements

The paper engine should be provider-agnostic:

- consume canonical instrument metadata,
- consume canonical market events or runtime snapshots,
- implement the existing execution adapter boundary,
- produce normal runtime order/trade/wallet events,
- never place live orders,
- preserve the same fee, slippage, margin, wallet, order, and trade semantics as
  the selected paper execution profile.

Provider-specific code should stop at:

- product discovery,
- instrument metadata normalization,
- REST candle fetch/backfill,
- WebSocket connect/subscribe/parse,
- provider sequence and heartbeat diagnostics.

Runtime logic should not know Coinbase channel names or Coinbase product payload
details.

## Initial 24-Hour Validation Checklist

For the first sustained Coinbase paper run, collect:

- connection start/end times,
- reconnect count and reasons,
- heartbeat count and largest heartbeat gap,
- provider sequence gap count,
- out-of-order message count,
- ticker update count per symbol,
- candle update count per symbol,
- finalized candle count per symbol,
- duplicate candle update count,
- BotLens live projection latency,
- runtime decision count,
- accepted/rejected order count,
- paper fill count,
- trade lifecycle count,
- wallet update count,
- CPU and memory trend,
- final report export status.

The first run is an operational stability test, not a profitability test.

## What Not To Do In V1

- Do not place live Coinbase orders.
- Do not wire BotLens directly to Coinbase payloads.
- Do not let WebSocket messages mutate strategy state outside the runtime
  timeline.
- Do not invent synthetic candles to hide stream gaps.
- Do not mix FCM and INTX products without explicit venue metadata.
- Do not store secrets in logs, reports, plans, or exported artifacts.
- Do not add exchange-specific behavior to strategy variants.
- Do not change wallet/order/trade/fee/slippage semantics as part of stream
  ingestion.

## Setup Summary

1. Create or verify Coinbase API credentials if authenticated metadata is
   required.
2. Save credentials through `python -m cli.main providers credentials add` for
   `COINBASE` / `COINBASE_DIRECT`.
3. Verify product metadata for the target symbols, starting with
   `BIP-20DEC30-CDE`, `ETP-20DEC30-CDE`, and `XPP-20DEC30-CDE`.
4. Connect to `wss://advanced-trade-ws.coinbase.com`.
5. Subscribe to `heartbeats`, `ticker`, and `candles`.
6. Normalize stream payloads into canonical provider market events.
7. Run a short observe-only bot intake check through `qt bots start`.
8. Use a longer observe-only paper session as an operational stability test.
9. Run normal streaming paper through `qt bots start --run-type paper
   --execution simulated`.
10. Treat the first 24-hour run as an operational stability test before using
   paper/live comparisons to tune the paper model.
