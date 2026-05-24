---
component: adr-budgeted-market-data-stream-reconnect-policy
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - paper
  - live
  - providers
  - market-data
  - runtime
code_paths:
  - portal/backend/service/bots/market_data_stream_policy.py
  - portal/backend/service/bots/paper_market_stream.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/controller/bots.py
  - src/core/settings.py
  - config/defaults.yaml
  - docs/architecture/execution-runtime/PAPER_ENGINE_V1_DESIGN.md
---
# ADR 0020: Use Budgeted Market Data Stream Reconnect Policy

## Status

Accepted on 2026-05-18.

## Context

Provider WebSocket streams are not durable execution clocks. Sustained paper and
live runs will eventually see network blips, provider closes, heartbeat stalls,
or transport reconnects. Treating every recovered disconnect as a terminal run
failure makes overnight paper runs too brittle. Treating disconnects as invisible
would hide market-data continuity risk and damage research trust.

Quant-Trad needs paper/live uptime without weakening runtime semantics:

- strategy evaluation must still advance only from admitted closed candles,
- wallet/order/trade/fee/slippage behavior must not change,
- missing stream data must not be synthesized silently,
- recovered reconnects must be visible as diagnostics,
- prolonged outages must fail loud.

## Decision

Make market-data reconnect behavior a bot-owned runtime policy named
`market_data_stream_policy`.

The policy is not strategy, variant, ATM, or risk sizing configuration. It is
resolved as normal bot config, may be overridden at run start, and is frozen in
`config_snapshot.bot.market_data_stream_policy`.

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

Recovered transient disconnects do not change lifecycle status. The paper
worker reconnects and records diagnostic counters and recent reconnect events.

A worker marks the live candle store failed only when:

- the disconnect remains continuous longer than
  `continuous_disconnect_budget_seconds`,
- reconnect is disabled,
- a fatal provider/config error occurs,
- a canonical provider stream contract error occurs.

The runtime loop still fails loud through the existing `LiveCandleStore` failure
path once the stream policy declares the outage terminal.

## Consequences

- Normal paper/live WebSocket blips no longer terminate runs.
- Operators and agents can inspect reconnect count, attempts, total disconnected
  seconds, max continuous disconnected seconds, and last disconnect reason in
  paper stream diagnostics.
- Runtime status remains honest: recovered reconnects are diagnostics, not a
  degraded lifecycle state.
- Prolonged outages still become terminal failures with actionable context.
- No strategy, wallet, order, trade, fee, slippage, or report DTO semantics
  change.
- Existing databases need the clean schema update for
  `portal_bots.market_data_stream_policy`; runtime schema checking still fails
  loud if the column is missing.

## References

- [Paper Engine V1 Design](../execution-runtime/PAPER_ENGINE_V1_DESIGN.md)
- [Execution & Playback Contract](../../contracts/platform/02_execution_playback_contract.md)
- [Runtime Contract](../../contracts/platform/01_runtime_contract.md)
