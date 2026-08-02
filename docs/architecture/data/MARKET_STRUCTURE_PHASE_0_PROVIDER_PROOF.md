---
component: market-structure-phase-0-provider-proof
subsystem: data
layer: operations
doc_type: validation
status: implemented
tags:
  - market-data
  - market-structure
  - coinbase
  - provider-proof
  - capacity
  - replay
  - implemented
code_paths:
  - cli/market_structure_proof.py
  - cli/main.py
  - cli/setup.py
  - scripts/reporting/extract_coinbase_market_structure_fixtures.py
  - src/data_providers/providers/coinbase.py
  - src/data_providers/streams
  - src/engines/bot_runtime/live_market.py
  - tests/fixtures/providers/coinbase/market_structure_phase0
  - tests/test_cli/test_market_structure_proof.py
  - tests/test_data_providers/test_coinbase_phase0_fixtures.py
---
# Market Structure Phase 0 Provider Proof

## Gate Status

Phase 0 is complete for Phase 1 implementation. On 2026-08-02 the operator
accepted the two clean one-hour BIP/BTC proofs as sufficient implementation
evidence and moved the 24-hour capacity/budget gate to after Phase 4. That
deferral authorizes implementation only; it does not authorize production
collector enrollment. ETP/ETH-USD and SLP/SOL-USD passed bounded access/unit
spot checks but remain unenrolled.

Phase 1 subsequently passed its bounded BIP/BTC implemented-path proof on the
same date. Its archive, canonical reconciliation, coverage, aggregate, and
provider-free dataset evidence is recorded in
[Market Structure Phase 1 Trades](MARKET_STRUCTURE_PHASE_1_TRADES.md). This
does not change the deferred production gate below.

| Gate | Status | Evidence |
|---|---|---|
| One-hour unauthenticated BIP/BTC WS + REST | passed | completed 2026-08-02; report SHA-256 below |
| One-hour existing-CDP BIP/BTC WS + REST | passed | completed 2026-08-02 through shared provider credentials; report SHA-256 below |
| Sequence scope and reconnect/resnapshot | passed for public and authenticated proofs | connection-wide counter, reset on reconnect; both L2 streams began each epoch with `snapshot` |
| Maker-side contract and captured schema | passed for public and authenticated proofs | Coinbase documents maker side; exact fixtures preserve `side` verbatim |
| BIP futures units and multiplier | passed for public and authenticated proofs | provider size is contracts; 0.01 BTC/contract; formulas below |
| Public CDE history | resolved unsupported | human-only rolling table has no stable documented machine contract; funding REST returns 401 without CDE credentials |
| Exact sanitized fixtures | passed | 11 public inbound frames; checksummed deterministic gzip bundle |
| ETP/ETH and SLP/SOL access/unit spot checks | passed | 60-second public proof with forced reconnect; report SHA-256 below |
| Phase 1 implementation readiness | passed | authenticated v2 report re-evaluates through the v3 one-hour gate with no reasons |
| 24-hour measured capacity/replay | deferred | required after Phase 4 and before production collector enrollment |
| Explicit operator production budget approval | deferred | must reference the future immutable 24-hour report checksum |

## Reproducible Commands

Run from the repository root. Every output directory is write-once; the proof
refuses to overwrite existing evidence.

```bash
scripts/qt data market-structure-proof \
  --auth-mode public \
  --duration 3600 \
  --reconnect-interval 1800 \
  --sample-limit 2 \
  --rest-limit 2 \
  --output-dir /tmp/quant-trad-phase0-public-1h

scripts/qt data market-structure-proof \
  --auth-mode authenticated \
  --duration 3600 \
  --reconnect-interval 1800 \
  --sample-limit 2 \
  --rest-limit 2 \
  --output-dir /tmp/quant-trad-phase0-auth-1h-v2

scripts/qt data market-structure-proof \
  --auth-mode public \
  --product-id ETP-20DEC30-CDE \
  --product-id ETH-USD \
  --product-id SLP-20DEC30-CDE \
  --product-id SOL-USD \
  --duration 60 \
  --reconnect-interval 30 \
  --sample-limit 1 \
  --rest-limit 2 \
  --output-dir /tmp/quant-trad-phase0-etp-slp-spotcheck-v1

# Deferred until after Phase 4; required before production enrollment.
scripts/qt data market-structure-proof \
  --auth-mode public \
  --duration 86400 \
  --reconnect-interval 43200 \
  --sample-limit 2 \
  --rest-limit 2 \
  --output-dir /tmp/quant-trad-phase0-public-24h

.venv/bin/python \
  scripts/reporting/extract_coinbase_market_structure_fixtures.py \
  --proof-report /tmp/quant-trad-phase0-public-1h/proof-report.json \
  --output-dir tests/fixtures/providers/coinbase/market_structure_phase0
```

The authenticated path builds a fresh short-lived WebSocket JWT for every
subscription through `CoinbaseProvider` and the existing encrypted provider
credential store. No collector or proof command reads Coinbase key material
from a new environment variable or writes a JWT to logs, raw evidence, reports,
or fixtures.

## Evidence Identity And Eligibility

The proof stores exact inbound frames as local Parquet/ZSTD with connection
epoch, local receive ordinal, receipt time, raw byte length, and raw SHA-256.
"Exact" means the WebSocket application-message payload delivered by the
client library; TCP/WebSocket framing and compression bytes are not evidence.
This is capacity/protocol evidence only. It deliberately records
`archive_complete=false` and `dataset_eligible=false`: local proof output has no
object-store acknowledgement and cannot be admitted to a frozen dataset.

`proof-report.json` is canonical JSON with a detached `proof-report.sha256`.
The repository has no configured artifact-signing identity. The checksum is a
content-integrity reference, not a cryptographic signer assertion.
The completed public report and its fixture manifest remain immutable v1
evidence. Subsequent authenticated and 24-hour runs use additive v2, which adds
the explicit trade-side decision, source-specific historical verdicts, and raw
file completion flag; v1 is not rewritten to impersonate v2. Proof schema v3
separates Phase 1 implementation readiness from production capacity admission.

## One-Hour Public Result

Capture interval: 2026-08-02T07:16:29.666159Z through
2026-08-02T08:18:29.261173Z. Measured campaign elapsed time was 3608.877386
seconds; capture status was `completed`.

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| `proof-report.json` | 11,001,748 | `7500a2b52dabf8399768cb0e8afd0e89084dbc0c2f9ae0cf503b3ed913255fe8` |
| BIP exact-frame Parquet/ZSTD | 1,466,646 | `71647a99f9bcafcd6bb21784678cc3e96b504f109fca6f6ae20ab6132d57dd34` |
| BTC exact-frame Parquet/ZSTD | 10,836,512 | `64fa5cb1939438dd9cb21f4e3c6e8efae7817f557f0bba437c54436dc47eabc8` |

All six public REST checks passed: product, bounded product book, and bounded
recent market trades for BIP-20DEC30-CDE and BTC-USD. Recent REST trades are
reconciliation evidence only and do not prove historical completeness.
The observed REST book exposes `product_id`, `time`, bids, and asks but no
WebSocket-compatible sequence position. It cannot splice or repair a WebSocket
book and remains comparison evidence only.

| Product/channel | Requested frames | Trades | L2 mutations | Connections/reconnects | Integrity and replay |
|---|---:|---:|---:|---:|---|
| BIP market trades | 31 | 236 | 0 | 2 / 1 | no gap/out-of-order/duplicate; exact replay fingerprint |
| BIP Level 2 | 11,211 | 0 | 44,425 | 2 / 1 | snapshot first in both epochs; valid final books; exact book replay |
| BIP ticker | 1,750 | 0 | 0 | 2 / 1 | snapshot first in both epochs; exact replay fingerprint |
| BTC-USD market trades | 4,147 | 7,251 | 0 | 2 / 1 | no gap/out-of-order/duplicate; exact replay fingerprint |
| BTC-USD Level 2 | 65,634 | 0 | 685,759 | 2 / 1 | snapshot first in both epochs; valid final books; exact book replay |
| BTC-USD ticker | 8,020 | 0 | 0 | 2 / 1 | snapshot first in both epochs; exact replay fingerprint |

Every stream observed heartbeats. No malformed/non-object frame, provider error,
unexpected disconnect, connection-sequence gap, out-of-order sequence, or
duplicate sequence was observed. Zero observed duplicates is evidence about
this capture, not a promise that duplicates cannot occur; duplicate invariance
remains a required deterministic consumer contract.

## One-Hour Existing-CDP Result

Campaign interval: 2026-08-02T08:29:18.945110Z through
2026-08-02T09:31:50.320290Z. Measured campaign elapsed time was 3612.72085
seconds; capture status was `completed`.

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| `proof-report.json` | 10,994,762 | `81fe668956a794aada0821ee83d202938c5b8ed3c0d1ffb3e83219e83cead032` |
| BIP exact-frame Parquet/ZSTD | 2,547,017 | `560e5f555927a91a75cd987c6cf40588ad712b4d3d51acbf7528998d9ba4daae` |
| BTC exact-frame Parquet/ZSTD | 13,289,415 | `59ff6cae584e2a57e03200fdaa33b1aee12a4fafdbfdf6fa82606e622ca10744` |

All six authenticated REST checks passed through the existing encrypted
provider credential store. Both WebSocket product sessions used the same
provider boundary, deliberately reconnected once, and issued fresh short-lived
JWTs for the second epoch. No credential or JWT entered logs or proof evidence.

| Product/channel | Requested frames | Trades | L2 mutations | Connections/reconnects | Integrity and replay |
|---|---:|---:|---:|---:|---|
| BIP market trades | 153 | 579 | 0 | 2 / 1 | no sequence/heartbeat fault; exact replay fingerprint |
| BIP Level 2 | 20,480 | 0 | 105,207 | 2 / 1 | snapshot first in both epochs; valid final books; exact book replay |
| BIP ticker | 2,791 | 0 | 0 | 2 / 1 | snapshot first in both epochs; exact replay fingerprint |
| BTC-USD market trades | 5,079 | 9,696 | 0 | 2 / 1 | no sequence/heartbeat fault; exact replay fingerprint |
| BTC-USD Level 2 | 68,551 | 0 | 923,083 | 2 / 1 | snapshot first in both epochs; valid final books; exact book replay |
| BTC-USD ticker | 9,813 | 0 | 0 | 2 / 1 | snapshot first in both epochs; exact replay fingerprint |

Every view observed more than 3,700 heartbeat frames. No requested-channel
sequence number was missing. No heartbeat-counter gap, malformed/non-object
frame, provider error, unexpected disconnect, connection-sequence gap, or
out-of-order sequence was observed. Both physical raw files were complete; all
six logical channel views reproduced the exact content fingerprint, and both L2
book fingerprints replayed exactly. Re-evaluating the immutable report with the
current production-capacity code leaves only
`24_hour_capacity_capture_required` and
`operator_annual_archive_budget_required`. The same immutable report passes the
v3 Phase 1 implementation-readiness gate with no reasons.

The authenticated diagnostic measured 133,278,117 raw bytes and 15,836,432
Parquet/ZSTD bytes (8.415918:1), provisionally annualizing to 128.744793 GiB.
Its p99 input rate was 117,389 bytes/s, implying a provisional 3x-p99 six-hour
raw spool of 7.084391 GiB. The largest checkpoint was 1,082,498 canonical bytes
and 316,125 ZSTD bytes; the slowest channel replay was 4.794599 seconds. These
are still one-hour diagnostics, not the budget approval measurement.

## Observed Provider Semantics

Coinbase's Advanced Trade channel documentation defines
`market_trades.events[].trades[].side` as maker side. The proof preserves the
provider value verbatim. The only allowed v1 aggressor transform is
`BUY -> SELL`, `SELL -> BUY`; any other/missing side produces no aggressor-side
fact.

The multiplexed raw timeline proved that `sequence_num` is one connection-wide
counter across subscription acknowledgements, heartbeats, market trades, L2,
and ticker. It reset to zero on reconnect. It is not a per-product/channel
counter. Initial collectors must therefore use one product per connection; a
gap invalidates every channel/coverage interval on that connection.

For `BIP-20DEC30-CDE`, product metadata, the published contract specification,
100 observed trade sizes, and 100 observed L2 quantities agreed on this unit
decision:

```text
provider_size_unit = contract
contract_size = 0.01 BTC
contract_quantity = provider_size
base_quantity_BTC = provider_size * 0.01
quote_notional_USD = price_USD_per_BTC * provider_size * 0.01
```

Spot BTC-USD size remains provider base quantity and is never assigned futures
contract semantics.

The Advanced Trade product payload exposes `funding_rate`, `funding_time`, and
`funding_interval`, but its API contract does not label that rate as projected
or finalized and does not define `funding_time` as a publication timestamp. The
existing typed collector may continue to store it as a provider-reported,
known-at-on-acceptance observation. Predicted-funding and finalized-funding
features remain disabled; no nearby CDE/FIX semantics are imported into the
Advanced Trade field.

## Provisional Capacity Result

The one-hour measurement is sufficient for Phase 1–4 implementation readiness,
but it cannot approve the production storage/cost budget. It measured
98,164,925 raw bytes and 12,303,158 Parquet/ZSTD bytes
(7.97884:1), provisionally annualizing to 100.127002 GiB for the BIP/BTC proof
scope. The 99th-percentile input rate was 91,661 bytes/s, implying a provisional
3x-p99 six-hour raw spool of 5.531714 GiB. The maximum frame was 4,923,600 bytes,
which proves the former 1 MiB WebSocket client default was unsafe.

Observed one-second rates were:

| Measure | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| Frames/s | 26 | 39 | 51 | 537 |
| Trades/s | 1 | 7 | 19 | 104 |
| L2 mutations/s | 148 | 355 | 723 | 44,821 |
| Raw bytes/s | 20,977 | 46,821 | 91,661 | 4,961,336 |

The largest measured final-book checkpoint was 1,081,331 canonical bytes and
315,900 ZSTD bytes. Six channel replays completed; the slowest was 3.597619
seconds. Snapshot bursts dominate the one-hour maxima, which is why the real
24-hour distribution and replay gate remain mandatory.

## Public Historical Coverage Decision

The official public historical page returned HTTP 200 as HTML, 84,527 observed
bytes, SHA-256
`0803e75d54354ff8c0c6eb81b61a532ed9acf00af61db366d2d1cda0d63c0fcc`.
The browser application currently displays a rolling daily table with date,
symbol, expiration, OHLC, settlement, volume, block volume, and open interest.
The HTML contains no rows. Its application loads them through an undocumented
website-internal CMS token, with no stable documented download/API, pagination,
rolling-window, publication-known-at, or correction contract. Quant Trad will
not build a collector against that internal token.

The direct CDE historical funding request for `BIPZ30` returned HTTP 401 with
provider error `missing request header: CB-ACCESS-KEY`; its bounded response
SHA-256 was
`321ee75359f97fbcf0d54b27d4f13af7dd124cb5f27e526ec212bd6a29ab7fb5`.
Existing CDP/Advanced Trade credentials do not satisfy that CDE request-signing
boundary.

Decisions:

- public CDE daily price/volume/OI/settlement: unsupported as an automated or
  dataset source; useful only for bounded manual sanity checks;
- public finalized funding history: unsupported; no stable public source was
  verified;
- CDE credentialed historical funding REST: unsupported under this campaign's
  provider boundary;
- Advanced Trade recent trades: admitted only as bounded reconciliation, never
  as complete history.

## Fixtures

`tests/fixtures/providers/coinbase/market_structure_phase0` contains 11 exact
public inbound frames: heartbeat; BIP and BTC trade snapshots/updates; L2
snapshots and zero-quantity deletions; and ticker frames. The BTC L2 snapshot is
4,921,999 bytes, explicitly exercising the raised 16 MiB client bound. The
security scanner rejects credential-shaped keys/values before publication.
Manifest and payload checksums, raw-frame checksums, parser semantics, units,
and zero deletions are enforced by tests.

## ETP/ETH And SLP/SOL Spot Check

The bounded public campaign ran from 2026-08-02T09:38:29.046555Z through
2026-08-02T09:39:34.869830Z with a deliberate reconnect at 30 seconds. Report
SHA-256 is
`df7bfd366470f6d5609adc5ee0cd3729042a9e8f57ace15d50726997ef9f32ac`.
All twelve REST operations passed.

| Product | Unit decision | L2 epochs | Integrity/replay | Parquet SHA-256 |
|---|---|---:|---|---|
| ETP-20DEC30-CDE | contracts; 0.1 ETH/contract; 100 trade + 100 L2 samples | 2 | snapshot first; no gap; exact book replay | `457a5e387594c33ee1daf273ee449e0a33fcdf547e2379c1ac9136aad2e550a9` |
| ETH-USD | provider base quantity | 2 | snapshot first; no gap; exact book replay | `6e90097d3fdd5f2b473b2792d2c098ba425f89fb4785254f497bfeaaf9fb83c8` |
| SLP-20DEC30-CDE | contracts; 5 SOL/contract; 100 trade + 100 L2 samples | 2 | snapshot first; no gap; exact book replay | `0417c6174faa6cee5a7c2da753dce2dc6023156008a755849d18a35dd2ed638c` |
| SOL-USD | provider base quantity | 2 | snapshot first; no gap; exact book replay | `4eabbdd7e3b9edbc4cda81127ed12340534eee576dcd607af48854e4625e3edc` |

Every product had two connections, one deliberate reconnect, zero unexpected
disconnects, zero sequence or heartbeat-counter faults, valid books in both
epochs, and exact raw/book replay fingerprints. Passing this proof makes these
pairs eligible for later bounded configuration; it does not enroll or start
them.

## Deferred Production Admission Gates

- After Phase 4, complete a real 24-hour BIP/BTC capture with one deliberate
  midpoint reconnect and deterministic full replay.
- Present its annual archive, p99 six-hour spool, file/checkpoint, and replay
  measurements to the operator.
- Record explicit operator byte-budget approval in a new immutable admission
  artifact referencing the 24-hour report SHA-256.
- Measure object-upload latency/backlog and typed-table/index amplification on
  the implemented Phase 1 storage path.
- Do not enroll production collectors until all of these gates pass.

The 24-hour report measures raw/archive rates, local encoder buffering and write
latency, checkpoint build cost, and full replay. Object-upload latency/backlog
and typed-table/index amplification remain explicitly `not_measured` because
their Phase 1 boundaries do not exist. They are required before production
collector enrollment and cannot be represented as Phase 0 successes.

The operator stopped the initially launched 24-hour run after roughly one
minute. It left only two `.partial.parquet` files under
`/tmp/quant-trad-phase0-public-24h`, with no final file or report. It is
explicitly incomplete and cannot become archive-complete or dataset-eligible.

Phase 1–4 implementation may proceed. No report status, local Parquet file, or
passing parser test can bypass the deferred production stop conditions.
