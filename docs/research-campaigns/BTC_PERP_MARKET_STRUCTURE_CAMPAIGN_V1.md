---
component: btc-perp-market-structure-campaign-v1
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
  - config/research_campaigns/btc_perp_market_structure_v1.json
  - src/research_science/autonomous_campaign.py
  - portal/backend/service/research/campaign_runner.py
---
# BTC PERP Market-Structure Campaign V1

## Campaign decision

This is QT's first bounded autonomous research operation. It is intentionally a
small proof of the complete research cycle, not a search for a deployable
strategy. The machine-readable charter is canonical; this document explains
its claim boundary.

The selected instrument is Coinbase `BIP-20DEC30-CDE`, displayed by its source
definition as **BTC PERP** / Bitcoin Perpetual. It is a perpetual-style nano BTC
future with a 2030 expiry, hourly funding semantics, and incomplete derivative
economics in QT. It is not spot and is not treated as a generic perpetual.

Selection used only train/validation availability and persisted instrument
metadata; the sealed holdout was not an input. The deterministic rule required
an admitted BTC perpetual-class source instrument with complete canonical
trade-flow, open-interest, and funding series across both non-holdout windows,
then ranked eligible products by complete causal 60-second coverage and stable
instrument identity. No spot fallback was permitted. The selected instrument
and both non-holdout dataset hashes are immutable charter fields.

## Claim

Within the frozen captured sessions only, determine whether causal trade-flow
conditions add five-event-minute, fully costed X2 signal value beyond no-trade,
passive exposure, momentum, mean-reversion, price-only, randomized, and delayed
timing controls.

The campaign cannot establish broad profitability, capacity, calibrated venue
execution, funding-inclusive returns, margin safety, liquidation behavior,
collateral adequacy, deployment readiness, or capital eligibility.

## Immutable data fence

The data steward froze three provider-free datasets at market commit sequence
`183869` before protocol activation:

| Role | Dataset | Window (UTC) |
|---|---|---|
| Train | `mds_b61dc21c05e991ffe34b08e4061f449f` | 2026-08-05 05:50–06:49 |
| Validation | `mds_6946bb064ffe0802da788075438c1a9d` | 2026-08-05 14:03–15:30 |
| Holdout | sealed as `btc-perp-final-session-v1` | binding withheld |

Each dataset pins the same 60-second trade-flow series plus causal open-interest
and funding observations for the same instrument. The research agent chooses a
role, never a dataset binding. Provider fetching, URLs, credentials, paths,
runtime mutation, deployment, capital, and external orders are prohibited.

The holdout assurance is `PLATFORM_CONTROLLED_HISTORICAL`: QT can prove that the
normal campaign workflow withholds its binding and feedback after activation;
it cannot prove that a human or external process never saw public history.

## Search and validation

- Exactly 24 deterministic typed graphs are generated from one declared family.
- At most eight train survivors receive validation attempts.
- The family budget is 40 attempts, four validation-feedback uses, and no more
  than one child mutation per branch. The first implementation does not need to
  spend feedback merely because it is available.
- Three chronological walk-forward scoring folds use a ten-bar context-only
  contamination horizon for purge and embargo.
- One candidate may be frozen. The holdout is opened only after the family is
  closed and every attempt is terminal.
- Failure to produce a validation-qualified candidate is a correct terminal
  rejection; it does not justify weakening the charter or opening the holdout.

## Economic floor

All search and validation use one pinned X2 conservative-bar model: next-event
market entry, five-bar exit, 2.5 bps adverse slippage per side, a versioned
research fee schedule of 4/6 bps maker/taker, and full-fill disclosure. The fee
schedule is a conservative campaign assumption, not a claim that it is the
participant's actual Coinbase tier.

Survivors must also be evaluated under the two pinned adverse cost stresses.
X3–X5 are fixed-candidate feasibility checks only; they are never optimization
surfaces. Existing replay-certified BIP L2 coverage does not overlap the
validation/holdout windows sufficiently, so an X3–X5 upgrade is expected to be
reported as unavailable rather than inferred.

## Stop conditions

The campaign stops and retains evidence when there is no eligible dataset,
frozen identity mismatch, causal boundary failure, budget exhaustion, no
validation survivor, missing benchmark or cost stress, non-reproducibility,
holdout gate failure, or any attempt to enable provider or trading capability.

Once the scientific protocol is active, code and protocol changes invalidate
that campaign identity. A defect requires terminal rejection/archive and a new
family; it cannot be patched in place.

A genuine sealed-holdout gate failure consumes that family's one permitted
holdout evaluation, retains the negative evidence privately, and archives the
family without releasing failed holdout metrics. A malformed runner artifact
does not consume the reservation and must be corrected without examining
holdout feedback.

## Autonomy ceiling

Agents may generate and evaluate bounded typed graphs, reject weak work, and
nominate one candidate. Separate identities authorize protocol, holdout, and
certification transitions. Even a research-qualified result has:

- `instrument_economics_class = incomplete`
- `promotion_eligible = false`
- `external_trading_authority = false`

No shadow, paper, live, deployment, credential, capital, or order-submission
state exists in this campaign.
