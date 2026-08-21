# Chainlink Numeric Facts

This guide enables one reviewed, bounded, read-only Chainlink AggregatorV3
source and acquires exact numeric facts into canonical market-data storage.

Chainlink acquisition is not a collector daemon and is never a runtime read
fallback. Every network call is an explicit CLI/API operation with actor,
reason, request, log, block, and retry bounds. A wallet, signer, transaction,
websocket, and LINK balance are not used.

## Supported Reference Bindings

The repository includes disabled references:

| Binding | Instrument role | Proxy | Canonical fact | Expected feed metadata | History start |
| --- | --- | --- | --- | --- | --- |
| `eth-usd` | `benchmark` | `0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419` | `market.reference_price.v1`, instrument `ETH`, quote/unit `USD` | 8 decimals, `ETH / USD` | `2026-08-07T00:00:00Z` |
| `tusd-reserves` | `primary` | `0xBE456fd14720C3aCCc30A2013Bffd782c9Cb75D5` | `market.reserve_balance.v1`, instrument `TUSD`, reserve/unit `USD` | 18 decimals, `TUSD Reserves` | `2026-08-07T00:00:00Z` |

The 2026-08-07 official-catalog review records a 0.5% ETH/USD deviation
threshold. Its heartbeat remains explicitly unknown in the reference rather
than inferred. The TUSD page records a 5% deviation threshold and describes a
once-per-day standard reporting interval, represented as 500 basis points and
86,400 seconds. These are reviewed metadata, not an availability guarantee.

The files under `config/market-data/numeric-facts/` are references, not
enablement. Both their root `enabled` and binding `enabled` fields are false.
Verify the network, proxy, deployment/history lower bounds, decimals,
description, confirmation and staleness policy, canonical instrument/role,
update/deviation schedule, official catalog, risk tier, and deprecation review
locally before enabling a copy. Requests before `history_start` are rejected.

## 1. Confirm The Current Schema

A clean database creates the current canonical Fact store, acquisition
coverage, schema registry, indexes, and immutable triggers on first startup.
Do not run historical numeric or canonical migration scripts on a clean
database.

An operator preserving a database that predates the canonical Fact hard
cutover may follow the reviewed offline migration and backup/restore procedure.
The alternative is to retain any needed evidence and initialize a new empty
database. Current non-empty databases are validated and fail loud on drift;
startup never patches columns or replays historical scalar-store migrations.

## 2. Prepare An Enabled Manifest

Copy, review, and rename a reference rather than enabling the checked-in
reference in place. For example:

```bash
cp config/market-data/numeric-facts/chainlink-eth-usd.reference.json \
  config/market-data/numeric-facts/chainlink-eth-usd.local.json
```

In the local copy:

- set root `enabled` to `true`;
- set the selected binding `enabled` to `true`;
- verify `instrument_id`, `instrument_role`, fact contract, unit, and
  dimensions;
- verify `schedule`, `quality_policy.max_staleness_seconds`,
  `quality_policy.stale_behavior="gap"`, and all `risk` review fields;
- verify `chain_id`, network, proxy, deployment-block lower bound,
  `history_start`, confirmations, maximum log span, expected
  decimals/description, and optional expected version;
- keep `endpoint_ref` as an environment-variable name, not an RPC URL.

The manifest loader rejects unknown or missing root/binding fields, undeclared
dimensions, unit mismatches, invalid roles/schedule/quality/risk metadata,
duplicate binding IDs, unsupported contracts, and non-exact fact types. The
acquisition service and provider separately validate source/config shape and
live feed metadata. Any edit changes the manifest hash and therefore the
acquisition coverage identity. That is intentional.

Make the referenced endpoint available to the backend process. The references
use:

```bash
export CHAINLINK_ETHEREUM_RPC_URL='<reviewed Ethereum JSON-RPC endpoint>'
```

Do not commit or log the endpoint if its URL embeds a provider token. Durable
provenance records the environment-variable name, not the resolved URL.

HTTP RPC calls use a conservative 0.5-second minimum interval. A reviewed
deployment may override it without changing the manifest or coverage identity:

```bash
export CHAINLINK_RPC_MIN_INTERVAL_SECONDS='0.5'
```

The override must be a nonnegative number. Retries retain the operation's
`max_retries` and `max_requests` bounds and add bounded exponential delay; they
never widen a block or log range.

The endpoint must support `eth_chainId`, `eth_blockNumber`, historical
`eth_getBlockByNumber`, `eth_call`, and bounded `eth_getLogs`. Historical scans
need archive access at the configured deployment lower bound and requested
range. A current-only endpoint is insufficient for historical proof.

## 3. Choose Operation Bounds

Every acquisition requires:

- `max_requests`: all RPC attempts, including retries;
- `max_logs`: total returned logs;
- `max_blocks`: total source block span admitted by the operation;
- `max_retries`: retries after transient RPC failure, default 2.

Choose bounds for one reviewed range. `max_blocks` must cover the entire block
span, while `max_log_span` in the manifest limits each `eth_getLogs` page.
Current mode must also cover `current_lookback_blocks`. Budget exhaustion fails
the operation; it never widens the budget automatically.

Use UTC ISO-8601 times. Historical windows are half-open: `start <=
effective_at < end`.

## 4. Acquire The Newest Confirmed Round

Current mode always requires explicit network authorization:

```bash
qt data acquire-numeric-facts \
  --manifest-path config/market-data/numeric-facts/chainlink-eth-usd.local.json \
  --binding-id eth-usd \
  --mode current \
  --allow-network \
  --requested-by operator-id \
  --reason 'inspect newest confirmed ETH/USD round' \
  --max-requests 256 \
  --max-logs 10000 \
  --max-blocks 50000 \
  --max-retries 2
```

Current mode forbids `--start`, `--end`, and `--repair`. It scans the current
phase's bounded lookback, selects the newest confirmed `AnswerUpdated` event,
and reconciles it with proxy `latestRoundData`. A newer unconfirmed round makes
the result partial and the CLI exits nonzero. If the newest confirmed
observation exceeds `quality_policy.max_staleness_seconds`, the adapter emits a
`chainlink_latest_round_stale` gap, returns partial, and the CLI exits nonzero.

## 5. Acquire A Historical Window

Historical mode reads the proxy phase at both bounded block endpoints, resolves
the inclusive active phase range, and scans every applicable phase aggregator
within the requested block window. If historical proxy-state reads are denied,
the adapter warns and safely falls back to scanning all configured phases:

```bash
qt data acquire-numeric-facts \
  --manifest-path config/market-data/numeric-facts/chainlink-eth-usd.local.json \
  --binding-id eth-usd \
  --mode historical \
  --start 2026-08-07T00:00:00Z \
  --end 2026-08-07T01:00:00Z \
  --allow-network \
  --requested-by operator-id \
  --reason 'prepare reviewed one-hour research input' \
  --max-requests 256 \
  --max-logs 10000 \
  --max-blocks 10000 \
  --max-retries 2
```

The JSON result reports requested, acquired, and cached ranges; inserted,
corrected, invalidated, no-op, and gap counts; requests, logs, and blocks used;
and `complete`. The CLI exits zero only when `complete` is true.

A complete zero-event scan is valid coverage and is cached. A phase, log range,
round, archive read, latest-round reconciliation, or finality gap produces
partial/failed evidence and is not cached as complete.

The range must begin at or after the binding's declared `history_start`. A
request ending beyond the newest confirmed source block keeps the confirmed
prefix, emits a `chainlink_range_unconfirmed` gap for the trailing interval,
and returns partial rather than certifying unavailable future coverage.

## 6. Prove Cached Reuse

Repeat the same historical command without `--allow-network`. Keep the manifest,
binding, confirmation depth, range, and series identity unchanged.

If complete coverage already spans the range, the operation returns it under
`cached_ranges`, reports zero requests/logs/blocks, and succeeds without
constructing a provider. If any portion is missing, default-deny authorization
fails before network use. This is the simplest operator proof that a historical
range is locally reusable.

Changing the manifest, binding source adapter version, confirmation depth,
source, binding, fact dimensions, or canonical series intentionally prevents
old coverage from being treated as the same cache identity.

## 7. Repair A Range After A Suspected Reorg

Repair bypasses complete-cache reuse and rescans the entire bounded historical
range:

```bash
qt data acquire-numeric-facts \
  --manifest-path config/market-data/numeric-facts/chainlink-eth-usd.local.json \
  --binding-id eth-usd \
  --mode historical \
  --start 2026-08-07T00:00:00Z \
  --end 2026-08-07T01:00:00Z \
  --repair \
  --allow-network \
  --requested-by operator-id \
  --reason 'reconcile reviewed source reorg window' \
  --max-requests 256 \
  --max-logs 10000 \
  --max-blocks 10000 \
  --max-retries 2
```

Changed block, transaction, log, confirmation, value, or causal material
appends a correction revision. Only a complete repair may append invalidation
revisions for active source events that disappeared. A partial repair records
gaps and leaves prior events active because disappearance was not proven.

Never delete or update old rows to "clean up" a reorg. The revision chain is
the audit trail.

## 8. Prepare A Provider-Free Backtest Dataset

Numeric acquisition is allowed during dataset preparation only when both
`--acquire-missing` and an explicit acquisition object are supplied. Create a
local JSON file such as:

```json
{
  "authorization": {
    "network_allowed": true,
    "actor": "operator-id",
    "reason": "prepare reviewed exact numeric inputs"
  },
  "budget": {
    "max_requests": 256,
    "max_logs": 10000,
    "max_blocks": 10000,
    "max_retries": 2
  },
  "bindings": [
    {
      "manifest_path": "config/market-data/numeric-facts/chainlink-eth-usd.local.json",
      "binding_id": "eth-usd"
    }
  ]
}
```

Then run:

```bash
qt data prepare-backtest-dataset \
  --bot-id <bot-id> \
  --start 2026-08-07T00:00:00Z \
  --end 2026-08-07T01:00:00Z \
  --acquire-missing \
  --numeric-acquisition-json numeric-acquisition.json \
  --created-by operator-id
```

Each binding must match a required canonical instrument, fact type, contract
version, and exact dimensions. Preparation reacquires only missing numeric
ranges, rejects partial results or remaining required gaps, and freezes the
complete accepted revision chain through its commit watermark. Frozen row,
material, and provenance identity therefore includes corrections and explicit
invalidations, while decision-time delivery selects the causally visible
revision. Backtest startup and execution use the resulting dataset ID and never
receive the manifest, RPC endpoint, or acquisition authority.

Do not pass `--numeric-acquisition-json` without `--acquire-missing`; the CLI
rejects that ambiguous request. Repeated runs should reuse the frozen dataset,
not repeat acquisition.

## Inspect And Troubleshoot

Inspect registered logical series with:

```bash
qt data series --instrument-id ETH
```

Acquisition lifecycle logs include manifest ID, binding ID, series ID,
provider, venue, range, repair flag, status, observation count, and gap count.
Use the returned series/source IDs and those fields when inspecting canonical
facts, ingestion runs, gap evidence, and acquisition coverage.

Common failures:

| Error prefix | Meaning / action |
| --- | --- |
| `numeric_fact_acquisition_denied` | Range is missing and `--allow-network` was not supplied. Review and authorize or rely on complete cache. |
| `numeric_fact_binding_disabled` | Root manifest or binding is still disabled. Review an operator copy; do not auto-enable references. |
| `numeric_fact_endpoint_missing` | Backend environment lacks the variable named by `endpoint_ref`. |
| `chainlink_chain_mismatch` | Endpoint serves a different chain than the manifest. Quarantine the binding. |
| `chainlink_feed_mismatch` | Proxy decimals, description, or pinned version changed. Reverify the feed; do not weaken checks casually. |
| `chainlink_archive_unavailable` | Endpoint cannot read a required historical block. Use a reviewed archive-capable endpoint or narrow to supported history. |
| `chainlink_range_invalid` | The requested start precedes the reviewed `history_start`. Correct the range or review a new manifest lower bound. |
| `chainlink_range_unconfirmed` | The requested end extends beyond confirmed chain history. Accept only the confirmed prefix or retry later. |
| `chainlink_latest_round_stale` | The newest confirmed current observation exceeds the manifest staleness limit. Inspect the feed; do not treat it as current. |
| `chainlink_budget_exceeded` | Requested range/lookback exceeds declared bounds. Split the range or explicitly approve a new bounded budget. |
| `chainlink_*_unavailable` / partial result | The adapter retained a typed gap and did not certify complete coverage. Inspect evidence before retrying. |
| startup schema error on a clean database | Treat it as a bootstrap defect; do not replay historical migrations. |
| startup schema error on an existing database | Stop writers and follow the exact reviewed upgrade or rebuild guidance in the error. |

## Precision And Finality Notes

- `raw_value` is the integer answer emitted by the feed. `numeric_value` is the
  exact decimal obtained with the manifest-verified decimals. No binary float is
  introduced.
- AggregatorV3 `updatedAt` is effective time. The containing block timestamp is
  publication time. The configured confirmation block timestamp is known-at.
  Platform acceptance remains separate.
- Confirmation depth is part of coverage identity. Changing it requires new
  coverage and frozen evidence.
- `history_start` is the reviewed historical lower bound. Earlier requests fail;
  a request beyond the newest confirmed source block returns the confirmed
  prefix with an explicit gap and partial status.
- Manifest instrument role, schedule, quality, and risk fields are review and
  provenance policy. They never manufacture provider observations. A current
  observation older than `max_staleness_seconds` is retained with an explicit
  stale gap and partial status.
- A provider's successful response is not proof of complete history by itself;
  phase, log, round, latest-round, and finality reconciliation determine status.
- OI/funding v1 consolidation is deliberately deferred. Do not point their
  specialized series at this store or fabricate exact values from retained
  floats. See ADR 0061 for the bounded v2 gate.

## References

- [Numeric Facts And On-Demand Acquisition](../architecture/data/NUMERIC_FACTS_AND_ON_DEMAND_ACQUISITION.md)
- [ADR 0061](../architecture/decisions/0061-use-provider-neutral-exact-numeric-facts-and-bounded-acquisition.md)
- [Data Boundary](../architecture/data/DATA_BOUNDARY.md)
- [Data Layer](../engineering/data-layer.md)
