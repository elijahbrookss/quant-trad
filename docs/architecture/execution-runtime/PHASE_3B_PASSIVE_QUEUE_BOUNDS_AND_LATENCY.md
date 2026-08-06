---
component: phase-3b-passive-queue-bounds-and-latency
subsystem: execution-runtime
layer: boundary
doc_type: architecture
status: active
tags:
  - execution
  - order-book
  - queue
  - latency
  - accounting
  - reporting
  - autonomy
code_paths:
  - src/engines/bot_runtime/core/passive_execution.py
  - src/engines/bot_runtime/core/book_execution.py
  - src/engines/bot_runtime/core/execution_context.py
  - src/engines/bot_runtime/core/execution_order.py
  - src/engines/bot_runtime/core/entry_execution.py
  - src/engines/bot_runtime/strategy/series_builder_parts/series_construction.py
  - portal/backend/service/market/market_structure_service.py
  - portal/backend/service/bots/config_service.py
  - portal/backend/service/bots/startup_service.py
  - portal/backend/service/reports/run_research_dataset.py
  - tests/integration/runtime/test_book_execution.py
---
# Phase 3B Passive Queue Bounds and Latency

## Scope and status

Phase 3B is implemented for deterministic backtests. It adds X5 passive-order
research behind the existing venue-neutral execution context, replay-certified
book tape, canonical order lifecycle, and canonical accounting owners. It does
not open shadow, paper-book, live-order, calibration, promotion, or capital
authority.

X5 means that a result used a named deterministic latency scenario and a named
bounded interpretation of aggregated queue evidence. It does not mean that the
simulator knows the order's exact queue position or venue-realized fill odds.

## Boundary and data flow

```text
replay-certified L2/L3 book states + canonical causal trade prints
  -> execution_book_tape.v2
  -> run-pinned ExecutionBookTapeBundle
  -> immutable PassiveQueuePolicy + ExecutionLatencyScenario
  -> X5 ExecutionModelArtifact parameters and hashes
  -> PassiveBookExecutionModel
  -> bounded queue progress and maker fills
  -> existing CanonicalOrderLifecycle
  -> existing wallet, position, fee, PnL, event, and report owners
```

Provider translation remains in the market-data service. Generic execution
code consumes provider-neutral book snapshots, trade prints, venue rules, and
instrument contracts. It contains no Coinbase, Kraken, or other venue-name
branch and performs no provider access.

## Immutable book-and-trade tape

`execution_book_tape.v2` extends the exact v1 book contract with causal
`execution_trade_print.v1` rows. Each trade pins:

- provider-neutral trade and version identity;
- price and quantity in the same unit as the reconstructed book;
- maker and aggressor side;
- effective and known-at timestamps;
- commit/order source position;
- upstream material hash; and
- a recomputed trade hash.

The tape hash covers every trade row and hash. V1 tapes retain their original
material and hashes and remain valid X3/X4 inputs. A v2 tape is produced only
when causal trade records are available. Market replay reads the paired trade
series only through the session's causal known-at ceiling, includes the exact
version/material identities in the replay fingerprint, and still completes all
existing raw/checkpoint/feature reconciliation first.

## Queue scenarios

`passive_queue_policy.v1` admits three explicit scenarios:

| Scenario | Queue interpretation |
| --- | --- |
| `TAIL_NO_CANCEL_CREDIT` | The order begins behind displayed quantity and cancellations never advance it. |
| `TAIL_OBSERVED_TRADE_PROGRESS` | Only causal trade prints at the exact price and maker side advance the queue. |
| `BOUNDED_CANCEL_CREDIT` | Trade progress plus an explicitly capped fraction of otherwise unexplained displayed decreases may advance the queue. |

All scenarios are deterministic. No random draw, fitted fill probability, or
silent cancellation assumption exists. A bounded-credit fill is labeled
`assumption_dependent`; a trade-only fill beyond displayed quantity ahead is
`definitely_supported`. Unfilled cases distinguish not-supported from merely
possible progress.

Aggregated L2 cannot identify individual orders, hidden liquidity, whether a
cancellation occurred ahead, or exact queue position. Every X5 evidence record
therefore discloses those limitations. L3-capable tapes may use the same
contract, but this version does not claim an order-level L3 queue algorithm.

## Latency and order timing

`execution_latency_scenario.v1` pins non-negative deterministic components for:

- decision latency;
- network latency;
- acknowledgement latency;
- cancellation latency; and
- replacement latency.

Arrival is derived from the original decision known-at plus decision, network,
and acknowledgement components. A caller-supplied arrival that conflicts with
that derivation fails closed. Pending evaluations preserve the original
decision/arrival and advance only their causal evaluation time.

Cancellation, replacement, and expiration compete at deterministic effective
timestamps. Queue progress is evaluated only through the earliest effective
boundary. Cancellation or expiry may terminate the residual. Replacement
exposes a `replacement_ready` boundary while the existing canonical lifecycle
remains responsible for creating and linking the new immutable attempt. The old
attempt can receive only fills known before that boundary.

The standard declared stress grid is 10, 50, 150, and 500 milliseconds. These
are research scenarios, not empirical venue distributions. Later calibration
must create a new versioned model artifact rather than changing v1 semantics.

## Runtime and startup enforcement

X5 is opt-in. A backtest must supply both:

1. a replay-certified L2/L3 v2 tape containing causal trade prints; and
2. a hash-valid `PassiveQueuePolicy` with its latency scenario.

Startup builds `execution_model_artifact.v2`, pins the assumption manifest,
tape hash, policy payload/hash, and composite input hash, and then pins the full
resolved execution-context bundle in the run snapshot. Runtime refuses X5 when
the tape or policy is missing or when the policy hash differs from the model
artifact. Supplying a tape without a passive policy retains X3/X4 behavior.

The API request and backend/container startup path preserve both artifacts.
They remain admitted only for backtests. Existing v1 model/context/tape hashes
and X0-X4 behavior remain unchanged.

## Lifecycle and accounting

Passive fills use maker fees from the resolved fee schedule and execute at the
resting price. Every increment has a deterministic fill ID and enters the
existing lifecycle. The executor injects original requested quantity and
canonical cumulative fill quantity so repeated evaluations cannot refill an
already settled amount. Fill events use the model's causal evaluation time.

No parallel queue ledger owns quantity or money. The canonical lifecycle owns
residual quantity, the wallet owns balances and fees, and the position engine
owns exposure and PnL exactly as in X0-X4.

## X5 evidence and downgrade rules

`passive_queue_evidence.v1` records:

- tape, replay, snapshot, model, policy, and latency hashes;
- original decision, arrival, and evaluation timestamps;
- initial displayed quantity ahead;
- observed exact-price trade quantity and source hashes;
- unexplained displayed decreases and admitted cancellation credit;
- conservative and scenario quantity-ahead bounds;
- definitely-supported and scenario-supported cumulative fill bounds;
- fill-support classification, new fill, residual, and terminal timing; and
- mandatory limitations.

Reports derive the ceiling from the weakest pinned execution context. X5 is
awarded only when passive queue evidence was actually exercised and passes its
causality, capability, lineage, bound, fill, scenario, latency, and limitation
checks. Missing or invalid X5-only evidence downgrades to X4. Existing failures
continue to downgrade through X3/X2 or X0 according to the lower-class rules.
Golden remains solely a reproducibility/reconciliation certificate.

## Tests and acceptance

The Phase 3B suite verifies:

- v1 tape/model backward compatibility and hash stability;
- v2 trade/tape/policy tamper detection;
- causal arrival and future-prefix invariance;
- trade-driven queue progress and cumulative partial fills;
- maker fee application through the existing lifecycle;
- deterministic cancellation and replacement latency boundaries;
- mandatory exact-queue limitation disclosure;
- startup pinning and X5 admission failure without required artifacts; and
- report validation and deterministic X5 downgrade behavior.

Acceptance is deterministic: identical tape, context, policy, lifecycle, and
evaluation inputs produce identical fills, fees, residuals, evidence hashes,
and report classification. No passive fill may exceed its declared scenario
bound, and no X5 claim may imply exact queue truth or calibrated venue behavior.

## Explicitly closed capabilities

- provider fetches from execution or research code;
- shadow or paper execution against a live book;
- external order submission;
- empirical latency or fill-probability calibration;
- exact L2 queue claims;
- autonomous policy choice outside an approved experiment protocol;
- promotion, deployment, or capital authority.

