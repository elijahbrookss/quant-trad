# Chainlink ETH/USD Breakout V3 Six-Month Campaign Dossier

> Historical limitation notice: the 119-candidate population was calculated
> before an admitted six-month Dataset existed and is not current replayable
> Check evidence. The original blocked result is preserved. See
> [Chainlink Historical Research Boundary Notice](CHAINLINK_RESEARCH_BOUNDARY_LIMITATIONS.md).

## Outcome

```text
CHAINLINK_BREAKOUT_CAMPAIGN_BLOCKED
CHAINLINK_LIVE_ACQUISITION_ACCEPTED
CHAINLINK_FROZEN_REPLAY_NOT_ACCEPTED
INSUFFICIENT_EVIDENCE
```

The six-month amendment passed its blinded numerical event, day, and fold
floors, but the population did not pass dataset admission. Coinbase Direct
returned no candles for a 12-bar interval inside the amended research window.
QT retained two immutable `provider_missing_data` records and did not invent
or reconstruct those bars. Consequently no six-month dataset was frozen, no
provider-disabled replay was attempted, and neither baseline nor
Chainlink-enriched analysis was run.

## Locked Amendment

The population remains ETH-USD on Coinbase Direct at 30 minutes, using the
existing Market Profile v1 raw `balance_breakout` transition. Only the minimum
observation horizon changed: the original July interval was extended to six
fixed, contiguous calendar months before the February-June population or any
Chainlink/outcome relationship was inspected.

- Candle warmup: `2026-01-01T00:00:00Z`
- Study: `2026-02-01T00:00:00Z` through `2026-08-01T00:00:00Z` exclusive
- Outcome tail: through `2026-08-01T06:00:00Z` exclusive
- Chainlink coverage: `2026-01-31T18:00:00Z` through the outcome-tail end
- Indicator: `5303a281-b129-452a-a16f-814a3244a309`
- Hypothesis: `38a599fe-60f2-452f-a602-91d1efa555fc`
- Amended study: `39507bbf-a073-40ac-b88a-e7ba78d200d7`
- Protocol SHA-256:
  `22fb2b223539b5c4ba831aea3137630446ebdbed1f3482372856fd6ef5b7056`
- Protocol commit: `2d1ad161d2d0b5975e59686df139473775228f7b`

The amendment preserves the eight Chainlink features, venue/breakout controls,
2/6/12-bar outcomes, model specifications, uncertainty method, Holm
adjustment, staleness rules, decision thresholds, and three-fold expanding
calendar-time design from protocol v2. No sealed holdout was opened and no
promotion authority was requested.

## July Reproduction

The original July population was reproduced before examining the extended
population. Reproduction checks are
`81c10f19-8973-4256-bf1d-e49dbebffdb4` (long) and
`9ed7be04-e654-47e2-af4e-58793d8e72d4` (short). The six-month warmup and
outcome-tail checks are `426f5b86-3248-49f8-b16d-9271e0d7a43f` and
`dfb72204-3b3e-42bc-99ba-d053f5ef0d3d`.

The July slice reproduced all 16 long and 8 short events exactly, including
timestamps, directions, event and entry closes, indicator parameters and
scope, and every available 2/6/12-bar outcome. Full-material hashes are
`b943f21ecdad4e7290fc511d328753ec98a4b14f8f293db9433ade79d1e4e2e3`
(long) and
`07543f3cd2c88f87116faf0a00fe939c8919ceac17df5d8388af05cc1ca51d35`
(short).

## Blinded Population Gate

Before Chainlink features were aligned to outcomes, the extended checks
produced:

| Measure | Result | Locked floor |
| --- | ---: | ---: |
| Eligible event candidates | 119 | 30 |
| Long / short candidates | 63 / 56 | descriptive |
| Positive / nonpositive at 6 bars | 59 / 60 | 10 per class |
| Distinct UTC event days | 62 | 12 |
| Validation fold candidates | 36 / 13 / 24 | 5 per fold |
| Missing 2/6/12-bar outcome horizons | 0 | 0 |

The blinded population material hash is
`a88dee5cd6e7499da2fbe4772a75dba48aef5441e5f6b24920474eb3ffed3632`.
These numerical floors pass, but the scientific gate as a whole does not pass
because material causal candle coverage is missing.

## Candle Admission Blocker

Candle series 364 contains 10,176 of the 10,188 expected 30-minute rows from
the warmup start through the outcome tail. The exact missing half-open interval
is `2026-05-08T01:30:00Z` through `2026-05-08T07:30:00Z`, inside the amended
study window.

Canonical gap evidence 104 records `provider_coverage_gap` after ingestion run
`cce72fd899b84915b2a07e02fb835587`; evidence 160 records
`provider_response_empty` after an explicit exact-bound repair request. A
wider `00:00Z` through `09:00Z` request returned only the six existing boundary
candles and inserted or corrected nothing. Both records classify 12 expected
and zero observed rows as `provider_missing_data`.

No alternate provider, interpolation, forward fill, or synthetic
reconstruction was used. Changing provider would change the locked Coinbase
population, and fabricating bars would violate the causal dataset contract.

## Chainlink Acquisition

The qualified endpoint was the public read-only Ethereum RPC at
`https://rpc.mevblocker.io`. It reported chain ID 1 and supported the adapter's
required `eth_chainId`, `eth_blockNumber`, historical
`eth_getBlockByNumber`, archive-state `eth_call`, bounded `eth_getLogs`, and
round-resolution calls. No wallet, signer, transaction, credential-bearing URL,
paid account, or onchain write was used. The checked-in reference manifest
remains disabled; only a disposable local manifest and API container were
RPC-enabled, and the container was removed after verification.

The new manifest
`chainlink-ethereum-mainnet-eth-usd-reference-six-month-amendment-local` has
SHA-256
`b5a407bcd2f9178cf98f247fffc1a8c349f26176f9e0311a4461b40d0ce4a0f3`.
It completed `2026-01-31T18:00:00Z` through
`2026-06-30T18:00:00Z` with 6,924 observations and zero gaps. The bounded run
used 21,536 RPC requests, 6,924 logs, and 1,075,637 blocks, within its declared
25,000-request, 10,000-log, and 1,200,000-block budgets. Persisted commits are
265398 through 272322. The earlier 10,000-block attempt failed before acquiring
data and remains immutable `chainlink_budget_exceeded` evidence.

The original July manifest hash
`5f7961c774b18df9d9655f0f9322a7455fdeb4496c791052a265e329f573d84b`
continues to cover the adjacent `2026-06-30T18:00:00Z` through
`2026-08-01T06:00:00Z` interval with 1,037 rows. The two exact manifest
identities therefore compose to 7,961 active facts without repeating the July
RPC scan. Repeating each exact request without network authorization returned
its full cached range and used zero requests, logs, or blocks. The new complete
interval also covers `2026-01-31T18:00:00Z` through `18:10:00Z`, where zero
facts exist, proving that no-event coverage is explicit rather than inferred.

Three canonical new-period rows were checked against direct RPC calls:

| Sample | Proxy round | Raw / exact value | Effective time | Event / confirmation block | Known at |
| --- | ---: | --- | --- | --- | --- |
| Early | 129127208515966885452 | 238528012400 / 2385.28012400 | 2026-01-31 18:18:35Z | 24356598 / 24356610 | 2026-01-31 18:20:59Z |
| Middle | 129127208515966889507 | 235958881407 / 2359.58881407 | 2026-04-16 05:43:23Z | 24890371 / 24890383 | 2026-04-16 05:45:47Z |
| Late | 129127208515966892375 | 156870967450 / 1568.70967450 | 2026-06-30 17:56:59Z | 25432127 / 25432139 | 2026-06-30 17:59:23Z |

For every sample, phase/local/proxy round, raw answer, exact eight-decimal
normalization, update time, event block/hash, transaction/log position,
confirmation block/hash, and confirmation-time `known_at` matched. QT accepted
the historical rows at `2026-08-08T09:23:07.753862Z`, preserving the
difference between answer time, confirmation-time causal availability, and
backfill acceptance. This independent operational conclusion is
`CHAINLINK_LIVE_ACQUISITION_ACCEPTED`; it does not override the candle dataset
blocker or imply predictive value.

## Dataset, Replay, And Research Stop

There is no six-month dataset identity, commit watermark, material hash,
provenance hash, or quality hash to report because the dataset was not admitted
or frozen. The prior July dataset
`mds_af5fc5210c398cadb2cfb5e34b08f565` remains immutable and was not reused as
if it covered the amended interval.

Provider-disabled replay cannot be accepted without an admitted frozen
six-month dataset, so it was not attempted. The baseline and enriched models,
effect sizes, confidence intervals, feature-bin success rates, Holm adjustment,
and temporal-stability estimates were likewise not run. No Chainlink feature
was joined to breakout outcomes. The honest research classification is
`INSUFFICIENT_EVIDENCE`, and the blinded gate status is
`SIX_MONTH_SAMPLE_GATE_NOT_PASSED` because its required coverage condition
failed despite passing the numeric count floors.

## Canonical Records

- Original study: `ec17e7b6-bcce-496a-ba7e-bc07c3d154ec`
- Original dataset: `mds_af5fc5210c398cadb2cfb5e34b08f565`
- Original outcome: `2ddbab5a-04e1-4903-b9ca-76ffe2c15231`
- Hypothesis: `38a599fe-60f2-452f-a602-91d1efa555fc`
- Amended study: `39507bbf-a073-40ac-b88a-e7ba78d200d7`
- July reproduction checks: `81c10f19-8973-4256-bf1d-e49dbebffdb4`,
  `9ed7be04-e654-47e2-af4e-58793d8e72d4`
- Six-month checks: `426f5b86-3248-49f8-b16d-9271e0d7a43f`,
  `dfb72204-3b3e-42bc-99ba-d053f5ef0d3d`
- Campaign outcome observation: `a9c80e25-4b63-4a30-9060-255e276bd4e8`

## Validation

The focused provider, numeric-fact acquisition/storage/dataset/runtime,
Coinbase, and research-check suites passed 127 tests with 3 skips and one
Starlette deprecation warning. Final repository, JSON, documentation, and
worktree validation are recorded with the committed campaign result.

## Smallest Justified Next Action

Do not tune the protocol or inspect Chainlink relationships. Reattempt only the
exact 12-bar Coinbase interval if the canonical provider later makes it
available. If it closes, rerun the unchanged blinded admission gate, freeze one
new six-month dataset, remove provider access, and execute the already-locked
replay and baseline-versus-enriched comparison exactly once.
