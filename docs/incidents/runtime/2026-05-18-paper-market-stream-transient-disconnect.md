# Paper Market Stream Transient Disconnect (2026-05-18)

## Incident

- Scope: paper bot runtime, provider websocket stream, live candle aggregation,
  BotLens live market display, runtime lifecycle diagnostics
- Observed run: `80ecda51-b891-4e5c-b8d4-9f3ee96332d9`
- Symptom: a sustained observe-only paper bot run stopped after several hours
  when the Coinbase websocket connection closed without a proper close frame
- User-facing effect: the run could not serve as a reliable overnight stability
  smoke test, and live candle/BotLens updates stopped for affected symbols
- Engineering effect: a normal transient market-data disconnect was treated too
  much like a terminal runtime failure

This was a runtime resilience incident.

The bot did not fail because of wallet, order, trade, fee, slippage, strategy,
or indicator semantics. The failure mode was the market-data stream boundary:
external websocket connections are expected to drop over long-lived paper/live
runs, but the runtime did not yet have a clear reconnection budget contract.

## What We Observed

The paper bot ran for hours before the provider websocket closed unexpectedly.

The observed provider error was consistent with:

```text
no close frame received or sent
```

The important facts:

- the failure happened at the provider stream boundary,
- different symbol streams failed after hours rather than immediately,
- there was no evidence that the run failed because of memory pressure,
- there was no evidence that strategy decision logic caused the failure,
- recovered short disconnects should be expected in any live/paper market-data
  integration.

## Root Cause

The stream runner did not yet distinguish clearly between:

- fatal provider/config/contract errors,
- transient websocket disconnects,
- and prolonged loss of market-data continuity.

That made the runtime too brittle for sustained paper mode.

The missing contract was not "retry forever no matter what." The missing
contract was:

> keep retrying through normal short-lived disconnects, but terminally fail
> when continuous disconnection exceeds an explicit bot-owned budget.

## What Was Not The Root Cause

This incident did not require changing:

- wallet semantics,
- order semantics,
- trade lifecycle semantics,
- fee or slippage semantics,
- strategy decision semantics,
- indicator signal semantics,
- or BotLens projection truth.

Those boundaries remained correct to preserve.

## Contract Clarification

Paper/live market-data streams are external operational dependencies. They are
not deterministic in the same way as historical backtest bars.

The runtime contract after this incident is:

- transient stream disconnects are operational diagnostics,
- recovered reconnects do not imply degraded strategy continuity,
- reconnect counts and disconnect durations must be observable,
- a stream becomes terminal only on fatal provider/config/contract errors or
  when the continuous disconnect budget is exhausted,
- the reconnect policy belongs to bot runtime configuration,
- the exact policy used by a run must be frozen in the run config snapshot.

## Corrective Action

The follow-up implementation introduced a bot-owned
`market_data_stream_policy`.

The policy includes:

- `reconnect_enabled`,
- `initial_backoff_seconds`,
- `max_backoff_seconds`,
- `continuous_disconnect_budget_seconds`,
- `heartbeat_stale_seconds`.

The paper stream runner now records reconnect diagnostics such as disconnect
count, reconnect attempts, reconnect successes, current disconnected duration,
maximum continuous disconnected duration, and recent reconnect events.

This preserves runtime semantics while making provider instability visible.

## Architectural Decision

The future-proofing decision is recorded in:

- [ADR 0020: Use Budgeted Market Data Stream Reconnect Policy](../../architecture/decisions/0020-use-budgeted-market-data-stream-reconnect-policy.md)

ADR 0020 owns the normative decision:

- use a budgeted reconnect policy instead of fixed retry count,
- keep recovered disconnects diagnostic-only,
- persist the effective policy in bot config and run snapshots,
- keep report/runtime semantics separate from operational stream recovery.

## Follow-Up Items

- Ensure existing databases receive the `portal_bots.market_data_stream_policy`
  schema update before restarting with the new model.
- Keep reconnect diagnostics visible in BotLens and run summaries without
  treating recovered blips as semantic degradation.
- Use the disconnect budget as the terminal failure guard for long-lived paper
  runs.
- When live execution is added, apply the same market-data stream policy at the
  provider stream boundary without changing execution semantics.
- Compare future paper/live behavior using recorded reconnect diagnostics,
  not by assuming zero network interruption.

## Permanent Lessons

- Long-lived paper/live streams must assume provider disconnects will happen.
- Reconnection is runtime resilience, not strategy behavior.
- A recovered provider disconnect is not the same thing as degraded strategy
  continuity.
- Operational continuity needs explicit budgets and diagnostics.
- Run snapshots must record the runtime policy that governed the run.
