# Coinbase Funding-Rate Collector

This continuous typed-fact collector polls Coinbase Advanced Trade for one
explicit perpetual futures product and stores append-only
`derivatives.funding_rate.v1` observations. It uses the public product endpoint
and does not require Coinbase credentials.

## Preconditions

- The canonical instrument has datasource `COINBASE`, exchange
  `COINBASE_DIRECT`, and `has_funding=true`.
- The operator supplies the exact provider product ID. The platform does not
  infer BIP, ETP, or SLP products from display symbols.
- The backend, TimescaleDB, and collector worker are running:
  `make stack-up STACK_PROFILES=core`.

## Create And Operate

Definitions are disabled unless `--enabled` is supplied:

```bash
qt data collectors create-coinbase-funding \
  --instrument-id <canonical-instrument-id> \
  --provider-product-id <coinbase-product-id> \
  --poll-interval-seconds 60 \
  --enabled
```

Inspect schedules, attempts, leases, and failures through the shared collector
commands:

```bash
qt data collectors list
qt data collectors attempts <definition-id> --limit 100
qt data collectors disable <definition-id>
qt data collectors enable <definition-id>
```

Disabling a definition does not delete its accepted history.

## Read Causally

The read command uses canonical storage only and never polls Coinbase:

```bash
qt data funding-rate-latest \
  --instrument-id <canonical-instrument-id> \
  --decision-time 2026-08-02T03:32:30Z \
  --max-staleness-seconds 180
```

Use `--optional` only when structured unavailability is allowed. Required
missing or stale funding fails loudly.

## Semantics

- `sample_time` is the scheduled observation identity.
- `rate` is a signed fraction, not a percentage. For example, `0.00001`
  represents `0.001%`.
- `funding_time` and `interval_seconds` are preserved from Coinbase.
- Coinbase does not define `funding_time` as a publication timestamp in the
  product contract used here. It does not determine causal visibility.
- `known_at` is platform acceptance after receipt.
- Collection definitions never contain credentials or credential references.
  Public funding acquisition does not consult the provider credential store.
- Provider or payload failures become auditable failed attempts; exhausted
  retries and missed schedules create gap evidence.
- One scheduled sample is idempotent and append-only. A stale lease cannot
  publish or complete an attempt.

## Current Boundary

Current live-forward collection, causal operator reads, frozen repository reads,
provenance, gaps, and restart recovery are implemented. Historical funding
backfill is not implemented, so history begins when collection starts.
Funding is not yet delivered into indicator or strategy requirements.
