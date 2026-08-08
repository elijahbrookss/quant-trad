# Chainlink ETH/USD Breakout V2 Campaign Dossier

## Outcome

```text
CHAINLINK_BREAKOUT_CAMPAIGN_PARTIAL
CHAINLINK_LIVE_ACQUISITION_ACCEPTED
CHAINLINK_FROZEN_REPLAY_ACCEPTED
INSUFFICIENT_EVIDENCE
```

The production-shaped data path passed. The research branch stopped at its
predeclared minimum-sample gate, so baseline and Chainlink-enriched models were
not fit. The overall campaign is partial because the requested comparative
study could not be run honestly, not because acquisition or replay failed.

## Locked Population And Protocol

The population is ETH-USD on Coinbase Direct at 30 minutes, using the existing
Market Profile v1 raw `balance_breakout` transition: a previous
`inside_value` location followed by `above_value` (long) or `below_value`
(short). No suitable canonical ETH breakout result existed, so this accepted
definition and its persisted parameters were locked before any Chainlink
feature/outcome relationship was inspected.

- Warmup: `2026-01-01T00:00:00Z`
- Study: `2026-07-01T00:00:00Z` through `2026-08-01T00:00:00Z` exclusive
- Outcome tail: through `2026-08-01T06:00:00Z` exclusive
- Chainlink acquisition: `2026-06-30T18:00:00Z` through the outcome-tail end
- Indicator: `5303a281-b129-452a-a16f-814a3244a309`
- Hypothesis: `38a599fe-60f2-452f-a602-91d1efa555fc`
- Authoritative study: `ec17e7b6-bcce-496a-ba7e-bc07c3d154ec`
- Protocol SHA-256:
  `02fd160967491ed0472ccdfd63f4627a2e54ac6ca4cd376ec93def2a915de3d4`

The protocol locked eight Chainlink features, venue/breakout controls, 2/6/12
bar outcomes, three expanding temporal folds, 12-bar purge/embargo, a UTC-day
block bootstrap, Holm adjustment, and explicit positive/negative/insufficient
rules. No holdout was opened.

## Breakout Evidence And Scientific Stop

Canonical directional checks are
`17662880-06b5-4163-a670-ec44516763ff` (long) and
`9dc94a55-da3f-404d-af5c-66e4b2b4e768` (short).

| Measure | Long | Short | Combined |
| --- | ---: | ---: | ---: |
| Eligible events | 16 | 8 | 24 |
| Positive / nonpositive at 6 bars | 8 / 8 | 4 / 4 | 12 / 12 |
| Mean 6-bar return | -0.0000981 | -0.0016718 | not pooled |
| Median 6-bar return | 0.0006129 | -0.0002861 | not pooled |
| Mean 6-bar MFE | 0.0066640 | 0.0067801 | not pooled |
| Mean 6-bar MAE | -0.0060435 | -0.0064528 | not pooled |

Only 10 UTC event days were present. The three locked validation folds contain
0, 0, and 3 events. This fails the locked floors of 30 events, 12 UTC days, and
5 validation events per fold. The per-class floor passes, but the overall and
temporal floors do not.

Consequently:

- the baseline model was not fit;
- the Chainlink-enriched model was not fit;
- no direct Chainlink feature test, effect size, confidence interval, Holm
  adjustment, or feature-bin success rate was computed;
- temporal stability is not estimable;
- no Chainlink/outcome relationship was inspected;
- the exact research classification is `INSUFFICIENT_EVIDENCE`.

This preserves the protocol instead of manufacturing an underpowered result.

## Development Database And RPC Qualification

The target was the local Compose development database (`QT_CONFIG_PROFILE=dev`).
The existing manual numeric-fact migration was applied with database writers
stopped. Startup then validated the migration-owned numeric tables, unbounded
PostgreSQL `numeric`, indexes, checks, and immutable triggers without creating
or altering them at runtime.

The qualified endpoint was the public, read-only MEV Blocker Ethereum RPC:
`https://rpc.mevblocker.io`. It returned chain ID 1 and supported the adapter's
required `eth_chainId`, `eth_blockNumber`, historical
`eth_getBlockByNumber`, archive-state `eth_call`, bounded `eth_getLogs`, and
round-resolution calls. No wallet, signer, transaction, private key, paid
account, or credential-bearing URL was used.

Rejected public probes remain qualification evidence rather than being hidden:
PublicNode returned 403 for logs, Llama returned 521 for chain ID, Flashbots
returned 403 for calls, dRPC and Blast returned 400 for logs, 1RPC and Merkle
rate-limited required calls, Gateway.fm returned 503, BlockPI returned 521, the
Alchemy demo endpoint returned 429, and rpc.payload.de did not resolve. None was
substituted with a fixture or described as accepted.

The checked-in ETH/USD reference manifest remains disabled. A disposable local
copy alone was enabled and bound to canonical ETH instrument
`d238523a-9bee-4366-bc43-b797311fbdf0`.

## Live Acquisition And Coverage

Series 369 / source 100 contains 1,037 active Chainlink ETH/USD rows in the
declared historical window. Final composed coverage is complete with zero
unresolved ranges. Repeating the exact full request with network authorization
omitted returned the full interval under `cached_ranges` and used zero RPC
requests, zero logs, and zero blocks. The earlier current read inserted one
newer observation outside the frozen range and did not certify historical
coverage.

The live interval from 2026-06-30 18:00Z through 18:10Z contains zero fact
rows, yet one immutable complete coverage interval spans it. This proves that a
covered no-event range is reusable evidence rather than an inferred omission.

Three canonical rows were independently checked against direct RPC calls:

| Sample | Proxy round | Raw / exact value | Effective time | Event / confirmation block | Known at |
| --- | ---: | --- | --- | --- | --- |
| Early | 129127208515966892388 | 158523220000 / 1585.23220000 | 2026-07-01 02:39:23Z | 25434733 / 25434745 | 2026-07-01 02:41:47Z |
| Middle | 129127208515966892888 | 188213000000 / 1882.13000000 | 2026-07-15 12:03:11Z | 25537931 / 25537943 | 2026-07-15 12:05:35Z |
| Late | 129127208515966893403 | 186582907154 / 1865.82907154 | 2026-07-31 20:33:23Z | 25655259 / 25655271 | 2026-07-31 20:35:47Z |

For each sample, phase/local/proxy round, raw answer, exact decimal, updatedAt,
event block/hash, transaction/log position, confirmation block/hash, and
confirmation-time `known_at` matched.

The immutable quality trail retains failed attempts: one genesis-boundary
failure, seven unpaced phase log-range 429 gaps, and 32 rate-limited unresolved
rounds. Those records are not presented as current missing coverage; later
bounded complete scans closed the range without deleting the diagnostic
evidence.

## Frozen Dataset And Provider-Disabled Replay

- Dataset ID: `mds_af5fc5210c398cadb2cfb5e34b08f565`
- Dataset hash:
  `af5fc5210c398cadb2cfb5e34b08f56542b77f101c3d14043496d0737ad2b559`
- Commit watermark: `262185`
- Candle series 364: 10,176 rows
- Chainlink series 369: 1,037 rows
- Chainlink material hash:
  `c64a6165b3b84e6d87dbed42ab0a82a02a56cffa657fcae1da71ecc27155675b`
- Chainlink provenance hash:
  `769919cf79d6221b4ff1505b48833cd6a483d0fd8ae0a1460f8d570119b88c36`
- Chainlink quality hash:
  `cc71e0135609188635c82d7599a3ac8419985c0f5ae893397a863837e3a881ce`

The only RPC-enabled API container was removed. Replay ran twice in disposable
backend containers where the Chainlink endpoint was absent and the HTTP
provider call was explicitly guarded to raise. Both runs selected facts only
when `fact.known_at <= breakout decision bar close`, produced all eight locked
features for all 24 events, and reported zero missing and zero stale-at-6h
events. They returned identical:

- selected-row hash:
  `b53eecc9e7b5f4cf7dcfcd7201411e5560c1d82d74e9465070f230cdd373dd13`
- feature-matrix hash:
  `ba46b3b905c2b23c45d1ea90fd4c005b56e33ef6ec8e9e7672f33195a866a3a7`
- research-output hash:
  `65a43371c18761c7518ac0cd800cef8e1b38ae550cbb91b1a062b7ba9fac3fd3`

Feature extraction was replay validation only. The matrix was not joined to
outcomes for statistical inspection after the sample floor failed.

## Gaps Found And Fixed

Focused implementation fixes were required:

1. Coinbase page-end normalization now preserves half-open candle bounds and
   does not drop or duplicate inclusive page boundaries.
2. Chainlink block-time search accepts Ethereum genesis block zero's legitimate
   timestamp zero while retaining strict positive timestamps elsewhere.
3. An unresolved round can no longer have its original gap masked by a failure
   while locating the gap time.
4. Historical acquisition reads the proxy phase at the bounded start/end blocks
   and scans only active phases, with a logged all-phase fallback if archive
   state is unavailable.
5. HTTP JSON-RPC calls have a conservative 0.5-second minimum interval and
   bounded exponential retry delay; attempts remain inside declared budgets.

The actual candle study window is complete and continuous. The warmup retains
one provider-sparse gap from 2026-05-08 01:00Z to 07:30Z (12 missing 30-minute
bars); it is disclosed rather than backfilled synthetically.

## Canonical Records

- Initial observation: `b9741aad-9249-4841-bf82-9937f235fb23`
- Hypothesis: `38a599fe-60f2-452f-a602-91d1efa555fc`
- Superseded invalid-timeframe study: `2d105a8a-150c-4ee2-b2ed-50c60e52a783`
- Protocol correction observation: `a3a6ac0b-cd0e-4f0a-a138-a21a053d8dad`
- Authoritative study: `ec17e7b6-bcce-496a-ba7e-bc07c3d154ec`
- Long check: `17662880-06b5-4163-a670-ec44516763ff`
- Short check: `9dc94a55-da3f-404d-af5c-66e4b2b4e768`
- Campaign outcome observation: `2ddbab5a-04e1-4903-b9ca-76ffe2c15231`

## Smallest Justified Next Action

Do not tune features or open a holdout. Extend the same frozen breakout
definition to additional prospectively selected contiguous ETH history until
the locked event/day/fold floors pass, then execute the already-declared
baseline and enriched comparison once. The operational Chainlink path needs no
alpha promotion to remain useful as exact independent reference evidence.
