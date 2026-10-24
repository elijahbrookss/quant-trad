# Coinbase Paper Setup

Coinbase market data can drive an observe-only or simulated paper Bot through
the normal container runtime. Paper mode does not submit Coinbase orders.
Use this guide for provider setup; the
[execution boundary](../architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
and [paper design](../architecture/execution-runtime/PAPER_ENGINE_V1_DESIGN.md)
own execution and stream-composition semantics.

## Check product identity and access

Use canonical instruments whose provider is `COINBASE` and venue is
`COINBASE_DIRECT`. Preserve the exact provider product ID, contract size, price/
quantity increments, expiry, funding and margin metadata. A display name such as
"PERP" does not establish contract type: FCM/CDE and INTX products have different
venue/access contracts. Inspect current provider metadata before selecting a Bot.

Public product metadata, candles and supported market-data channels do not need
credentials. For an authenticated workflow, save credentials through:

```bash
qt setup provider coinbase
```

The provider credential store uses `QT_SECURITY_PROVIDER_CREDENTIAL_KEY` and
returns safe references. Do not put Coinbase keys in Bot configuration, manifests,
logs or `secrets.env`. Public market-data setup does not need trade permissions.
The [operator handbook](../operators/README.md#provider-onboarding) describes the
deployed credential importer and private configuration boundary.

## Verify the provider connection

Run a bounded smoke check with the exact product:

```bash
qt providers stream-smoke \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --symbol <coinbase-product-id> \
  --product-id <coinbase-product-id> \
  --auth-mode public \
  --duration 10 \
  --channel heartbeats \
  --channel ticker \
  --channel candles
```

Inspect acknowledgements, heartbeat freshness, sequence diagnostics, malformed
messages and supported channel samples. Smoke success verifies this bounded
connection; it does not prove sustained coverage or execution readiness.

The provider adapter owns connection, authentication, subscription and parsing.
Runtime receives typed canonical market events rather than Coinbase payloads.
Run-scoped Bot streams are separate from the durable collector fleet described
in [collector operations](collector-operations.md).

## Follow arriving data without execution

Configure the Bot's Strategy instrument links first. The runtime resolves product
subscriptions from those canonical instruments; do not pass ad hoc product/channel
wiring to the Bot command.

```bash
qt bots start <bot-id> \
  --run-type paper \
  --execution observe-only \
  --duration-seconds <bounded-duration>
```

Observe-only records lifecycle and market-event diagnostics without Strategy
evaluation, orders, fills, trades, fees, slippage or wallet mutation. Inspect its
run readiness, provider sequence and heartbeat evidence before changing behavior.

## Run simulated paper execution

```bash
qt bots start <bot-id> \
  --run-type paper \
  --execution simulated \
  --duration-seconds <bounded-duration>
```

The existing stream path warms Indicator state from historical candles, receives
Coinbase candle updates, and feeds closed runtime candles through the same
walk-forward engine and configured simulated execution policy. Source code in
`src/engines/bot_runtime/live_market.py` and
`portal/backend/service/bots/paper_market_stream.py` owns its closed/provisional
candle handling. BotLens pacing is not execution mode.

This existing paper composition is not permission to derive a new research input
from other intervals. New research candles must be provider-native at the
requested timeframe; historical internally derived inputs retain their accurate
provenance. Missing provider timeframe/coverage is a blocker, not authority to
synthesize bars. Do not infer REST-versus-stream equivalence from connection health.

Use the declared stream reconnect budget and diagnostics. Recovered disconnects
remain operational evidence; an outage does not manufacture continuous history.
Inspect finalized/ignored/incomplete candle counts and `summary.paper_market_stream`
where available, then compare durable decisions, orders, fills and wallet facts
through normal report/readiness contracts. A completed lifecycle alone does not
certify replay, golden status or provider continuity.

## Troubleshoot at the owning boundary

- Product/access mismatch: inspect canonical metadata and provider response;
  do not substitute another venue or product silently.
- No fresh candles: inspect subscription, heartbeat, sequence and closed-candle
  diagnostics before changing Strategy logic.
- Repeated disconnects: inspect receive-path pressure and configured budgets;
  do not increase queues or hide gaps merely to keep a run green.
- Missing BotLens projection: use readiness/diagnostics; a chart is a projection,
  not proof of missing or valid execution.
- Authentication failure: use the provider credential boundary and a scoped
  authenticated smoke check; never print keys or JWTs.

Provider protocol details belong in the adapter and official references:
[REST](https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/rest-api),
[WebSockets](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview).
Validate the actual deployed CLI help and provider capabilities rather than
assuming every channel, account surface or future broker feature is supported.
