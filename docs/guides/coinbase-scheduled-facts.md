# Coinbase Scheduled Open Interest And Funding

Coinbase open-interest and funding collectors poll the public Advanced Trade
product endpoint and append canonical Facts. They share one scheduled lifecycle.
No credential is required for these public reads. Their history begins at
collection; restart does not reconstruct earlier samples.

## Enroll the scheduled definitions

Start the backend, database and collector with `make stack-up STACK_PROFILES=core`,
or use the deployed CLI wrapper in the [operator handbook](../operators/README.md).
Supply an exact Coinbase futures product ID; enrollment validates provider
metadata and creates the canonical instrument and registered definitions.

```bash
qt data collector-definitions enroll-product \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --product-id <coinbase-product-id> \
  --collector open_interest \
  --collector funding_rate \
  --poll-interval-seconds 60 \
  --actor-id <operator-id> \
  --reason 'Approved scheduled market-data coverage' \
  --confirm
```

Repeat `--collector` for the desired types. Omitting it enrolls the full supported
pack, including trades and Level 2. Funding requires canonical metadata declaring
funding support. Enrollment cannot add an adapter or Fact contract absent from
the running release.

New definitions start running. Reapplying enrollment is idempotent and preserves
a later audited stop or pause. Copy each `scheduled_fact` ID from
`qt data collectors fleet`, then inspect it:

```bash
qt data collectors detail scheduled_fact <collector-id>
qt data collectors events scheduled_fact <collector-id> --limit 50
qt data collectors gaps scheduled_fact <collector-id> --limit 50
qt data collectors diagnose scheduled_fact <collector-id>
```

Use [collector operations](collector-operations.md#safe-actions) for lifecycle,
confirmation and request-ID semantics. Stop preserves accepted history.
Configuration enablement is a separate reviewed gate; a lifecycle action cannot
enable a disabled configuration.

## Read stored observations causally

These commands read canonical storage and never poll Coinbase:

```bash
qt data open-interest-latest \
  --instrument-id <canonical-instrument-id> \
  --decision-time <utc-iso-time> \
  --max-staleness-seconds 120

qt data funding-rate-latest \
  --instrument-id <canonical-instrument-id> \
  --decision-time <utc-iso-time> \
  --max-staleness-seconds 180
```

Choose staleness from the consumer contract. Use `--optional` only when that
consumer permits structured unavailability; required missing or stale input fails.
Backtests consume already stored Facts frozen into a Dataset, without provider
calls during execution.

## Interpret the two Fact types

| Fact | Payload meaning | Time semantics |
| --- | --- | --- |
| `derivatives.open_interest.v1` | Venue/product-specific open interest | `sample_time` is the scheduled sample, not an invented exchange event time. |
| `derivatives.funding_rate.v1` | Signed fractional rate, provider funding time and interval | `funding_time` and `interval_seconds` are payload semantics, not proof of publication time. |

A funding rate of `0.00001` means `0.001%`. Receipt and platform acceptance
determine causal visibility; provider funding time does not substitute for
`known_at`. Existing v1 floating payloads cannot recover original exact decimal
text. Preserve their versioned material rather than inventing extra precision.

Definitions, attempts, pacing, retries and claims are durable in PostgreSQL.
A stale lease cannot publish or complete an attempt. Provider failures, exhausted
retries and missed schedules remain attempt/gap evidence. All observations use
the canonical Fact repository; OI and funding are distinct payload contracts,
not separate physical truth stores.

The [data boundary](../architecture/data/DATA_BOUNDARY.md) owns canonical meaning.
The [collector control plane](../architecture/data/COLLECTOR_OPERATIONS_CONTROL_PLANE.md)
owns lifecycle and diagnosis. Collector support does not imply arbitrary
historical backfill or that every Indicator/Strategy consumes its output.
