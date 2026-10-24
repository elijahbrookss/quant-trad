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

## Provider boundary

The adapter separates scalar AggregatorV3 round history from atomic MVR
bundles. Provider contracts, reviewed manifests and the
[Chainlink MVR API](https://docs.chain.link/data-feeds/mvr-feeds/api-reference)
define supported acquisition; catalog presence alone does not establish it.

SmartData currently has two materially different acquisition shapes:

- AggregatorV3 exposes one typed answer per round. Phase/round history can be
  acquired for bounded ranges when an archive-capable RPC endpoint is
  available.
- Multiple-Variable Response (MVR) exposes one atomic latest bundle. Its field
  layout and per-field decimals come from the feed definition. The proxy stores
  only the latest report, so reliable history requires an off-chain indexer or
  a collector that was already running.



## Reviewed structured feed

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

A feed descriptor is configuration, not a current reserve assertion or evidence
of deployment activation. MVR latest-state reads do not reconstruct earlier
bundles. Admission must verify the descriptor against the selected chain and
finalized block before accepting an observation.

## Canonicalization and provenance

`ChainlinkMvrReserveProvider` performs provider-specific work only at the
acquisition boundary. It reads one finalized block, verifies chain ID,
description, proxy version, field order/types/decimals, and subject ID, then
decodes the ABI bundle. It rejects future or stale reports.

The canonical payload is deliberately small and provider-free. This synthetic
quantity illustrates its shape; it is not an observed reserve balance:

```json
{
  "report_id": "DE000NXTA018",
  "reserve_asset": "BTC",
  "reserve_quantity": "1.00000000",
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

A latest-only MVR binding cannot claim class C without verified history. Historical classification
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

## Admitting another structured family

A reviewed feed needs explicit subject, fields, units, effective/publication/
known-at clocks, finality, provenance and gap policy. Add one family at a time
through the existing schema and provider registries. A reserve statement does
not imply liabilities or supply; NAV/AUM and other fields require their own
atomic schema and accounting meaning. Test-network or stale catalog entries
cannot establish production capability. No feature has predictive value merely
because it is observable.

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
