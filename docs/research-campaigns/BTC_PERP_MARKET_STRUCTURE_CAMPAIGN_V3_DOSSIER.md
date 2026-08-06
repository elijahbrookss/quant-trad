---
component: btc-perp-market-structure-campaign-v3-dossier
subsystem: research-orchestration
layer: operation
doc_type: campaign-result
status: terminal-rejected
tags:
  - research
  - autonomous-campaign
  - perpetual
  - negative-result
  - sealed-holdout
code_paths:
  - config/research_campaigns/btc_perp_market_structure_v3.json
  - src/research_science/autonomous_campaign.py
  - portal/backend/service/research/campaign_runner.py
---
# BTC PERP Market-Structure Campaign V3 Terminal Dossier

## Decision

QT completed its first end-to-end bounded autonomous research operation and
rejected the family before validation. This is an operational success and an
economic non-result: the workflow accounted for every attempt, enforced causal
availability, stopped without selecting a candidate, archived its state, and
did not expose the sealed holdout.

The campaign does **not** show that market-structure signals lack value. It
shows that this frozen train artifact cannot support the declared
five-event-minute causal claim: every persisted trade-flow row became known
after the last possible train-bar arrival, so no signal could be followed by a
causally valid execution event within the train window.

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

## Causal failure evidence

The train artifact contained 59 complete rows. Across those rows:

- zero rows had a later bar whose arrival was at or after the row's `known_at`;
- every row's `known_at` was later than the final train-bar arrival;
- `known_at - bucket_end` ranged from about 67.6 to 3,547.5 seconds;
- all 24 graph evaluations therefore had `sample_count = 0` and
  `trade_count = 0`.

The evaluator did not substitute event time for known-at time, move execution
backward, fetch alternative data, or relax the protocol. This is the causal
fence working as designed.

## Quality and claim boundaries

- The configured execution ceiling was conservative-bar X2, with explicit
  fees, adverse slippage, and cost stresses. Because no causal trade was
  evaluable, no strategy earned an X2 performance claim.
- No scientific certificate was issued, so the result must not be represented
  as S1-S4 evidence for or against the economic hypothesis.
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

## Required follow-up before another economic campaign

Do not rerun this strategy family against the same causal artifact. First
certify a research input whose known-at distribution leaves enough future bars
for the declared horizon. The next charter should include a deterministic
pre-activation causal-opportunity floor by role, while preserving the existing
known-at semantics rather than weakening them. Only then should QT allocate a
new campaign identity, train budget, or sealed holdout.
