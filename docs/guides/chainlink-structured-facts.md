# Chainlink Structured Facts Operator Guide

This guide installs the reviewed nxtAssets Bitcoin Direct ETP reserve poll. It
does not acquire history, enable autonomous network access from Dataset reads,
or grant trading authority.

## Prerequisites

- Use a database admitted by the current schema contract. Clean installations
  bootstrap directly; older databases need an exact reviewed upgrade and
  restore-tested backup, not replay of arbitrary historical migrations.
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
  --binding-id nxtassets-btc-direct-etp-reserves \
  --enabled
```

The installer accepts only a checked-in structured-Fact manifest and
automatically installs the exact research-only canonical subject declared by
that manifest. It cannot accept arbitrary provider configuration.

Inspect the returned definition, source, series, cadence and staleness policy.
For a new installation, `--enabled` configures initial desired state `running`.
Reinstallation preserves an existing definition's enabled/desired state; it is
not an enable command. The CLI has no general configuration-enable action for
an already disabled definition. Diagnose that state and arrange a separately
reviewed configuration change; do not edit database flags to bypass it.

After configuration admission, use the actions advertised by
`qt data collectors fleet`. See [collector operations](collector-operations.md)
for exact kind/ID, diagnosis, stop/pause/resume and immutable operation history.

## Expected behavior

- Poll every hour for a feed expected to update approximately every 12 hours.
- Read a finalized Arbitrum block and verify the pinned proxy contract and MVR
  field layout before decoding.
- Append `asset.reserve_state.v1` only when a meaningful report identity/bundle
  changes. Repeated latest reads are no-ops.
- Record provider failures and stale data as normal attempt/gap evidence.
- Treat all history before collector activation as unavailable unless a future
  reviewed historical source proves otherwise.

Use `qt data collectors detail scheduled_fact <collector-id>`, `events`,
`gaps` and worker logs to inspect claims, attempts, failures, gaps and Facts.
Fact history is read through
the canonical repository and does not call Chainlink.

## Stop or recover

Stop or pause collection through the normal collector control surface before
a reviewed change to endpoint, proxy, schema, cadence or finality policy. Do not
edit a running definition in place. Commit a new reviewed manifest/schema version when
semantic meaning changes.

After a worker restart, the normal lease/retry path resumes polling. The first
identical latest bundle is an idempotent no-op; downtime is not backfilled or
hidden. If the manifest or on-chain metadata disagrees, keep collection stopped, preserve the error evidence, and investigate rather than weakening
validation.

For architecture, historical classification, feed semantics, and known risks,
see [Chainlink Structured Facts](../architecture/data/CHAINLINK_STRUCTURED_FACTS.md).
