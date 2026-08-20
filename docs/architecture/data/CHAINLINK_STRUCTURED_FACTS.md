---
component: chainlink-structured-facts
subsystem: data
layer: provider
doc_type: architecture
status: active
tags:
  - market-data
  - chainlink
  - smartdata
  - proof-of-reserve
  - structured-facts
  - collectors
  - provenance
  - history
code_paths:
  - config/market-data/structured-facts
  - src/data_providers/facts.py
  - src/data_providers/structured_facts.py
  - src/data_providers/providers/chainlink.py
  - src/market_data/canonical.py
  - src/market_data/fact_registry.py
  - src/indicators/reserve_state
  - portal/backend/service/market/collector_service.py
  - portal/backend/service/market/runtime_market_data.py
  - portal/backend/workers/market_data_collector.py
  - portal/backend/controller/market_data.py
  - cli/main.py
  - tests/test_data_providers/test_chainlink_mvr_provider.py
  - tests/test_market_data/test_structured_fact_research_path_db.py
---
# Chainlink Structured Facts

## Discovery boundary

QT rechecked the current Chainlink data surface on 2026-08-09 rather than
assuming the earlier AggregatorV3 proof represented SmartData. The authoritative
surfaces used were the Chainlink
[SmartData overview](https://docs.chain.link/data-feeds/smartdata),
[Multiple-Variable Response documentation](https://docs.chain.link/data-feeds/mvr-feeds),
[MVR proxy API](https://docs.chain.link/data-feeds/mvr-feeds/api-reference),
[historical data guidance](https://docs.chain.link/data-feeds/historical-data),
and the [SmartData catalog](https://data.chain.link/smartdata).

SmartData currently has two materially different acquisition shapes:

- AggregatorV3 exposes one typed answer per round. Phase/round history can be
  acquired for bounded ranges when an archive-capable RPC endpoint is
  available.
- Multiple-Variable Response (MVR) exposes one atomic latest bundle. Its field
  layout and per-field decimals come from the feed definition. The proxy stores
  only the latest report, so reliable history requires an off-chain indexer or
  a collector that was already running.

At discovery time the official metadata catalog listed 21 MVR feeds: 12
SmartData, six Proof-of-Reserve, and three NAVLink feeds. Catalog presence is not
an implementation decision. QT admits a feed only when its subject, fields,
units, clocks, provenance, update behavior, and access model support a useful
canonical fact.

## Initial production set

The bounded initial set contains one structured feed:

| Property | Selected contract |
| --- | --- |
| Feed | [nxtAssets Bitcoin Direct ETP Proof of Reserves](https://data.chain.link/feeds/arbitrum/mainnet/nxtassets-btc-etp-por) |
| Subject | `DE000NXTA018`, canonical instrument `nxtassets-de000nxta018` |
| Network | Arbitrum mainnet, chain ID 42161 |
| Proxy | `0xf5eA763bbFc7968A27b28bc612a8B89fCF9E0069` |
| Feed ID | `0x02000001050700030000000000000000` |
| MVR layout | `ID:string`, `TotalReserve:uint256` with 8 decimals |
| Canonical type/schema | `asset.reserve_state` / `asset.reserve_state.v1` |
| Canonical payload | report ID, reserve asset, exact reserve quantity, unit |
| Observation clock | `latestBundleTimestamp()` |
| QT known-at | platform acceptance after a finalized-block read |
| Expected cadence | 43,200 seconds; collector polls hourly |
| Staleness policy | append a gap after 259,200 seconds |
| Historical class | B: current/latest only |
| Access | public read-only EVM JSON-RPC; endpoint supplied by environment reference |

The on-chain discovery read confirmed chain ID 42161, proxy version 7, and the
description `nxtAssets Bitcoin Direct ETP Proof of Reserves (DE000NXTA018)`.
The report visible during discovery had timestamp 2026-08-07T19:00:00Z, ID
`DE000NXTA018`, and raw reserve `51432323119`, interpreted under the verified
eight-decimal layout as 514.32323119 BTC. This was discovery evidence, not a
database ingest and not a claim about the current reserve balance.

The official page described historical data as forthcoming. QT does not infer
history from that statement and does not reconstruct prior bundles from later
latest-state reads.

## Canonicalization and provenance

`ChainlinkMvrReserveProvider` performs provider-specific work only at the
acquisition boundary. It reads one finalized block, verifies chain ID,
description, proxy version, field order/types/decimals, and subject ID, then
decodes the ABI bundle. It rejects future or stale reports.

The canonical payload is deliberately small and provider-free:

```json
{
  "report_id": "DE000NXTA018",
  "reserve_asset": "BTC",
  "reserve_quantity": "514.32323119",
  "unit": "BTC"
}
```

Source identity and `market.structured_fact_provenance.v1` retain the provider,
network, chain ID, proxy and aggregator addresses, feed ID, finalized block
number/hash, bundle timestamp/hash/raw bytes, manifest and binding hashes,
verified field layout, adapter/transformation version, request/receipt clocks,
and QT acceptance. Quality evidence retains staleness and finality decisions.

One bundle is one Fact. QT does not split report ID and quantity into unrelated
rows. An identical latest report is an idempotent no-op. A changed bundle or
report timestamp creates a new append-only observation; the collector never
updates the prior row in place.

## Historical acquisition classification

| Family | Class | QT behavior |
| --- | --- | --- |
| AggregatorV3 price/reserve scalar | A: practical on demand when archive RPC and phases are available | explicit bounded acquisition, finality reconciliation, complete/partial coverage evidence, then Dataset freeze |
| MVR reserve, NAV, AUM, or other latest bundle | B: limited/impractical historically | durable scheduled collection from activation forward; preserve every meaningful update and gap |
| MVR feed with a future bounded official history endpoint | C: hybrid only after proof | backfill only the verified available range, record its boundary, then continue durable collection |

No current production MVR binding is classified C. Historical classification
is per feed and can change only after new provider capability is verified and a
new reviewed acquisition contract is committed.

## Collector behavior

The implementation extends the existing scheduled collector service. It does
not create a Chainlink research subsystem. Definitions use the normal lease,
fencing, attempt, retry, restart, gap, and worker capability contracts.

`qt data collectors create-structured` loads the checked-in manifest, validates
that the canonical instrument/source/series agree, and stores a manifest-bound
definition. It remains disabled unless the operator explicitly supplies
`--enabled`. The worker resolves `CHAINLINK_ARBITRUM_RPC_URL` only while running
the claimed definition and applies the shared RPC pacing policy. A repository
or Dataset read never constructs the provider.

Restart recovery is idempotent: rereading the same bundle reuses the existing
Fact identity. Downtime remains visible because no observation is synthesized
for the missing interval. Staleness and provider errors use normal gap and
attempt evidence and are never translated into an invented continuous history.

## Dataset, Indicator, and Check behavior

Dataset planning addresses `asset.reserve_state.v1`, the explicit subject, and
the `reserve_asset=BTC` dimension. Freeze pins schema ID/contract hash, exact
Fact versions, source binding, commit watermark, provenance/quality hashes, and
gaps. Frozen replay sets `provider_access=disabled`.

The `reserve_state` Indicator accepts only a canonical
`asset.reserve_state.v1` record and emits provider-free context containing the
exact and numeric reserve quantity, asset/unit, report ID, observation/known-at
times, and age. Check evidence can apply scalar assertions to the derived
quantity without inspecting Chainlink provenance. The end-to-end database test
replaces provider construction with a fail-fast trap and proves identical
Check result hashes on replay.

## Candidate families and risks

| Candidate | Canonical direction | History | Recommendation | Main unresolved risk |
| --- | --- | --- | --- | --- |
| Mainnet MVR reserve bundles with explicit asset/unit | `asset.reserve_state.v1` or a new schema if fields differ | current-only | add one at a time after subject/custodian review | third-party/self-reported source quality and revision behavior |
| AggregatorV3 reserve balance | `market.reserve_balance.v1` | usually bounded round history | use existing explicit acquisition after feed-specific verification | scalar balance may omit liabilities or supply |
| NAV/AUM/shares bundle | a new atomic `fund.nav`-like schema derived from actual fields | current-only unless proven otherwise | defer until a relevant mainnet feed is available | valuation clock, currency, share class, revision/finality semantics |
| Superstate USTB rich MVR bundle | would require NAV/AUM/shares/net-income fields | current-only | do not enable; catalog deployment observed on Arbitrum Sepolia | test network is not production evidence |
| MetalLink PoR | reserve-state candidate | current-only | do not enable from this review | selected Avalanche feed was stale during discovery |

Likely later research projections include reserve quantity/change, report age,
reserve-to-separately-observed supply, NAV premium/discount, and relationships
with price, volume, open interest, funding, liquidations, issuance, and
redemptions. These are observable features only. QT makes no claim that any has
predictive value.

## Known limitations

- The selected feed reports reserves, not liabilities, token supply,
  collateralization, or beneficial ownership. QT does not derive those values.
- PoR quality depends on the reporting entity, custodian, source methodology,
  and Chainlink feed configuration; on-chain delivery does not make the
  underlying statement trustless.
- MVR proxy schemas can change without a proxy redeployment. The reviewed
  manifest therefore pins exact field names, order, types, decimals,
  description, proxy version, feed ID, and address. Any disagreement fails.
- Pre-collector MVR history is unavailable unless a separately verified source
  later supplies it. QT never fills this boundary with synthetic continuity.
- RPC rate, archive availability, and finality policy remain endpoint and
  network operational concerns above the Fact boundary.

See [Generalized Fact Data Plane](GENERALIZED_FACT_DATA_PLANE.md) and the
[operator guide](../../guides/chainlink-structured-facts.md).
