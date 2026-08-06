---
component: btc-perp-market-structure-campaign-v2
subsystem: research-orchestration
layer: operation
doc_type: campaign-charter
status: pre-activation
tags:
  - research
  - autonomous-campaign
  - perpetual
  - market-structure
  - sealed-holdout
code_paths:
  - config/research_campaigns/btc_perp_market_structure_v2.json
  - src/research_science/autonomous_campaign.py
  - portal/backend/service/research/campaign_runner.py
---
# BTC PERP Market-Structure Campaign V2

## Campaign decision

V2 is the replacement identity for QT's first bounded autonomous research
operation. V1 activated but failed before family creation or any trial because
of an exact protocol/family-name mismatch. V1 was governed to rejection and
archive; it is not retried or rewritten.

The selected instrument remains Coinbase `BIP-20DEC30-CDE`, displayed by its
source definition as **BTC PERP** / Bitcoin Perpetual. It is a perpetual-style
nano BTC future with incomplete derivative economics in QT. It is not spot.

## Claim boundary

Within the frozen captured sessions only, determine whether causal trade-flow
conditions add five-event-minute, fully costed X2 signal value beyond no-trade,
passive exposure, momentum, mean-reversion, price-only, randomized, and delayed
timing controls.

The campaign cannot establish broad profitability, capacity, calibrated venue
execution, funding-inclusive returns, margin safety, liquidation behavior,
collateral adequacy, deployment readiness, or capital eligibility.

## Immutable data fence

Train and validation remain the provider-free, previously disclosed research
assignments. The data steward created a new materially distinct sealed holdout
artifact for V2 by narrowing the still-unopened final session and freezing it
at a new market commit watermark. Its identity and exact window remain private.

| Role | Dataset | Window (UTC) |
|---|---|---|
| Train | `mds_b61dc21c05e991ffe34b08e4061f449f` | 2026-08-05 05:50–06:49 |
| Validation | `mds_6946bb064ffe0802da788075438c1a9d` | 2026-08-05 14:03–15:30 |
| Holdout | sealed as `btc-perp-final-session-v2` | binding withheld |

The assurance remains `PLATFORM_CONTROLLED_HISTORICAL`: QT proves normal
workflow non-exposure, candidate-before-evaluation ordering, and one-use
consumption. It does not claim global technical blindness or that a human could
never know public historical data.

## Search, economics, and stop rules

- Exactly 24 deterministic typed graphs; at most eight validation survivors.
- Forty total attempts and four validation-feedback uses; unused allowance is
  not evidence of incomplete work.
- Three chronological walk-forward scoring folds with ten-bar warm-up, purge,
  and embargo boundaries.
- One candidate, one family closure, and at most one sealed holdout use.
- Pinned X2 next-event execution with 2.5 bps adverse slippage per side,
  research-assumed 4/6 bps maker/taker fees, and two adverse cost stresses.
- X3–X5 are reported unavailable unless their exact frozen inputs exist; they
  are not search dimensions.
- Any post-activation code, protocol, dataset, threshold, or assumption change
  invalidates V2 and requires a new identity.

## Autonomy ceiling

Agents may generate/evaluate bounded graphs, reject weak work, and nominate one
candidate. Separate identities authorize protocol, holdout, certification, and
governance transitions. Even a qualified result remains:

- `instrument_economics_class = incomplete`
- `promotion_eligible = false`
- `external_trading_authority = false`

No shadow, paper, live, deployment, credential, capital, or external-order
state exists in V2.
