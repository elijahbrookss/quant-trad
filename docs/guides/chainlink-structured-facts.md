# Chainlink Structured Facts Operator Guide

This guide installs the reviewed nxtAssets Bitcoin Direct ETP reserve poll. It
does not acquire history, enable autonomous network access from Dataset reads,
or grant trading authority.

## Prerequisites

- Apply the canonical Fact store/data/hard-cutover migrations.
- Set `CHAINLINK_ARBITRUM_RPC_URL` to a reviewed Arbitrum mainnet JSON-RPC
  endpoint in the worker environment. Do not place the endpoint or credentials
  in the manifest.
- Keep `CHAINLINK_RPC_MIN_INTERVAL_SECONDS` at its default pacing or set a
  reviewed nonnegative override appropriate for the endpoint.

## Install the definition

Review
`config/market-data/structured-facts/chainlink-nxtassets-btc-etp-reserves.json`,
then install the checked-in definition:

```bash
qt data collector-definitions install-structured \
  --manifest-path config/market-data/structured-facts/chainlink-nxtassets-btc-etp-reserves.json \
  --binding-id nxtassets-btc-direct-etp-reserves
```

The installer accepts only a checked-in structured-Fact manifest and
automatically installs the exact research-only canonical subject declared by
that manifest. It cannot accept arbitrary provider configuration.

Inspect the stored definition, source, series, cadence, and staleness policy.
For a new reviewed installation, add `--enabled` to configure it with desired
state `running`. If it was installed disabled first, enable it and then use the
canonical collector `start` action shown by `qt data collectors fleet`.
Operational state changes stay in the collector command path rather than the
definition installer.

## Expected behavior

- Poll every hour for a feed expected to update approximately every 12 hours.
- Read a finalized Arbitrum block and verify the pinned proxy contract and MVR
  field layout before decoding.
- Append `asset.reserve_state.v1` only when a meaningful report identity/bundle
  changes. Repeated latest reads are no-ops.
- Record provider failures and stale data as normal attempt/gap evidence.
- Treat all history before collector activation as unavailable unless a future
  reviewed historical source proves otherwise.

Use normal `qt data collectors` status/history operations and worker logs to
inspect claims, attempts, failures, gaps, and facts. Fact history is read through
the canonical repository and does not call Chainlink.

## Disable or recover

Disable the definition through the normal collector control surface before
changing endpoint, proxy, schema, cadence, or finality policy. Do not edit a
running definition in place. Commit a new reviewed manifest/schema version when
semantic meaning changes.

After a worker restart, the normal lease/retry path resumes polling. The first
identical latest bundle is an idempotent no-op; downtime is not backfilled or
hidden. If the manifest or on-chain metadata disagrees, leave the collector
disabled, preserve the error evidence, and investigate rather than weakening
validation.

For architecture, historical classification, feed semantics, and known risks,
see [Chainlink Structured Facts](../architecture/data/CHAINLINK_STRUCTURED_FACTS.md).
