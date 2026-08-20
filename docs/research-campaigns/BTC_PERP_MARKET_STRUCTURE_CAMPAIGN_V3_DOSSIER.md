---
component: btc-perp-market-structure-v3-postmortem
subsystem: research-orchestration
layer: operation
doc_type: postmortem
status: historical
tags:
  - research
  - historical-research
  - perpetual
  - invalid-input-contract
  - sealed-holdout
code_paths:
  - src/research_science/replay_availability.py
  - portal/backend/service/research/authority_repository.py
---
# BTC PERP Market-Structure V3 Historical Postmortem

This document preserves the vocabulary and identities emitted by the retired
operation as historical evidence. The executable schema, JSON definition, and
runner facade were deleted without a compatibility parser or migration.

## Decision

QT completed its first end-to-end bounded autonomous research operation and its
immutable records correctly show that the family was rejected before
validation. Repository forensics subsequently established that this must be
classified as an **invalid research-input-contract outcome**, not an economic
or strategy result. The workflow accounted for every attempt, archived its
state, and did not expose the sealed holdout, but its preflight did not prove
that the frozen input could supply causal replay opportunities before opening
the search budget.

The campaign does **not** show that market-structure signals lack value, and it
does not establish an economic non-result. Canonical aggregate `known_at`
correctly reflected delayed post-session finalization, while the frozen raw
receipts show that a pinned continuous transform could have derived most
non-empty minute aggregates in time. V3 had no explicit replay-availability
contract to distinguish those two clocks, and therefore evaluated the wrong
research input semantics.

## Pinned operation

| Boundary | Pinned evidence |
|---|---|
| Campaign | `btc_perp_market_structure_v3` |
| Code revision | `36e08dfdc97354d5162d92c0a8605997d44856d8` |
| Charter hash | `a1ef39f81f2a9c4511e3d5644509223992c7bbe838ed40bc71b4998580066803` |
| Protocol hash | `76753a8bab6af72a818e61e0fa976f6a1739e9ea43288381008994e9b7638fb8` |
| Preflight hash | `8d41627c1e7213e3ff34ce0fe63951c95f853090936615371fccbf60d9029766` |
| Execution context | `bc7be3ce662f2a8df97d934b41b608421172d8d1c0bfe9913500717012284cf3` |
| Fee schedule | `b8bcad6c391434abef43d44fa5b02343c417ad5fa2e684859b253cf42cc3a58f` |
| Execution progression | `22562b56559d5a830fa60e6da84a70ec0c5b2b54548f581932cfbc3932e6aa37` |
| Terminal family event | `aae622a1769bb4c60562f20638297d082f75a3a743d0651445b7f6e0e6603d76` |
| Public dossier-source hash | `ee4549743e6f62a145dfc1ff812444f28584bffbe746ba80c5900f6298c016ad` |

Two consecutive preflight reads produced identical hashes. Two consecutive
terminal evidence reads also produced the same public dossier-source hash.

## Search accounting

| Result | Count |
|---|---:|
| Typed graphs admitted | 24 |
| Train attempts terminally accounted | 24 |
| Invalid for no causal opportunity / no signal | 24 |
| Validation attempts | 0 |
| Candidates | 0 |
| Holdout uses | 0 |
| Certificates | 0 |
| Validation-feedback uses | 0 |

The family used 24 of 40 attempt slots. Every attempt retained its graph,
trial manifest, deterministic result artifact, explicit failure reasons, actor,
request, and append-only events. The remaining budget was not spent after the
entire declared graph family failed the train eligibility floor.

## Retrospective input-contract diagnosis

The original terminal evidence remains true for the clock it consumed: the
train artifact contained 59 complete aggregate rows and every aggregate
`known_at` followed the last possible train-bar arrival. That explains the 24
zero-opportunity attempts, but it does not prove that the underlying market
events were unavailable.

The later raw-to-aggregate reconciliation found:

- 46 of 59 train minutes contained trades and all 46 reconciled exactly to the
  persisted aggregate material;
- raw messages for those buckets had arrived by bucket close;
- 45 of the 46 non-empty buckets had a later market event available for the
  declared execution horizon when availability was derived from receipt
  evidence; and
- aggregate publication lag came from bounded-session canonicalization and
  finalization, not late market delivery.

The evaluator was right not to rewrite canonical `known_at` or substitute
provider event time. The missing boundary was a separate, versioned replay
availability certificate derived from frozen raw receipts, exact coverage,
watermarks, a pinned transform, and deterministic latency. Preflight also
counted nominal indexes rather than the evaluator's actual causal
opportunities. In addition, using the late aggregate clock for cross-fact joins
could select OI/funding samples from after the bucket, a latent look-ahead path
even though V3's zero scoring count prevented a performance result.

## Quality and claim boundaries

- The configured execution ceiling was conservative-bar X2, with explicit
  fees, adverse slippage, and cost stresses. Because no causal trade was
  evaluable, no strategy earned an X2 performance claim.
- No scientific certificate was issued, and the invalid input contract means
  the result must not be represented as S1-S4 evidence for or against the
  economic hypothesis.
- X3 spread, X4 L2 replay, and X5 queue-bounded execution were unavailable and
  were not optimization surfaces.
- Derivative economics remain incomplete. No funding-inclusive, margin,
  liquidation, collateral, capacity, or venue-calibrated claim is made.
- `promotion_eligible = false` and `external_trading_authority = false`
  throughout the operation.

## Governance outcome

The family is `archived`. The governance case reached `ARCHIVED` at state
version 7 through seven separately authorized decisions. The maximum permitted
state remained `RESEARCH_CERTIFIED`; operational trading authority remained
false. The workflow produced no candidate, opened no holdout, created no
certificate, and released no holdout feedback.

V1 and V2 remain archived as separate, immutable activation-defect evidence.
V3 did not rewrite or retry either identity.

## Evidence replay

Check out the pinned code revision and run the preflight in QT's configured
backend environment:

```bash
python -m portal.backend.service.research.campaign_runner preflight \
  --charter config/research_campaigns/btc_perp_market_structure_v3.json \
  --code-revision 36e08dfdc97354d5162d92c0a8605997d44856d8
```

The expected preflight hash is
`8d41627c1e7213e3ff34ce0fe63951c95f853090936615371fccbf60d9029766`.
Read the immutable terminal records without creating a new operation:

```bash
./scripts/qt research authority family-evidence \
  family:btc_perp_market_structure_v3
./scripts/qt research authority governance-case-get \
  governance:btc_perp_market_structure_v3
python -m portal.backend.service.research.campaign_runner evidence \
  --family-id family:btc_perp_market_structure_v3 \
  --governance-case-id governance:btc_perp_market_structure_v3
```

Do not rerun `execute` for V3. Its identity is terminal, and any new economic
attempt requires a new charter, protocol, and holdout assignment.

## Implemented repair and next identity

The runner now requires `autonomous_research_campaign.v2`, a frozen raw trade
series in every role dataset, an immutable replay-availability policy, exact
raw/aggregate/coverage reconciliation, receipt-watermark availability,
decision-time OI/funding joins, and deterministic pre-activation opportunity
floors for train, validation, and the private holdout. V1 charters remain
readable but cannot execute.

Do not rerun V3 or mutate its terminal records. Any future search requires a
new campaign identity, newly frozen role datasets that bind every replay
source, a new protocol, and a separately sealed holdout assignment.
