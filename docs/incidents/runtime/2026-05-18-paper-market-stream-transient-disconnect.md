# Paper Market Stream Transient Disconnect (2026-05-18)

A sustained observe-only paper run stopped after a provider WebSocket closed
without a close frame. The failure exposed a resilience gap at the market-data
transport boundary; the investigation did not attribute it to strategy,
Indicator, wallet or execution semantics.

## Why an ordinary disconnect became terminal

The stream runner did not distinguish fatal configuration/contract errors,
transient disconnects and prolonged continuity loss. It needed a bounded recovery
policy, not either immediate failure or unbounded retries.

The response introduced a bot-owned market-data stream policy with reconnect
enablement, backoff, continuous-disconnect budget and stale-heartbeat limits.
Run snapshots retain the effective policy. Reconnect counts, attempts, durations
and reasons make recovered operational trouble inspectable.

A recovered connection does not by itself prove complete data across the gap.
Missing source evidence remains visible and consumer readiness still applies.
Fatal contract errors and exhausted disconnect budgets fail loudly.

## Decision and lesson

[ADR 0020](../../architecture/decisions/0020-use-budgeted-market-data-stream-reconnect-policy.md)
records the reconnect choice; platform contracts remain normative.
[The runtime boundary](../../architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
describes current behavior. The incident's schema rollout was a historical
installation step, not a current instruction to alter an operator database.

Long-lived streams need explicit recovery budgets and diagnostics. Reconnection
must preserve the source-data and causal contracts rather than silently filling
the interval or changing trading semantics.
