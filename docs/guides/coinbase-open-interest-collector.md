# Coinbase Open-Interest Collector

The first continuous typed-fact collector polls Coinbase Advanced Trade for the
current open interest of one explicitly mapped futures product. It stores
append-only `derivatives.open_interest.v1` observations. It does not provide
historical backfill, funding, basis, aggregated OI, or live trading.

## Preconditions

- The canonical instrument already exists with datasource `COINBASE`, exchange
  `COINBASE_DIRECT`, and a futures instrument type.
- `secrets.env` contains the Coinbase credentials already used by the platform.
- The operator knows the exact Coinbase provider product ID. The platform does
  not infer it from the canonical symbol.
- The backend, TimescaleDB, and collector worker are running. The repository
  command is `make stack-up STACK_PROFILES=core`; the `core` preset includes the
  database profile.

## Create And Enable A Definition

Definitions are disabled by default unless `--enabled` is supplied:

```bash
qt data collectors create-coinbase-oi \
  --instrument-id <canonical-instrument-id> \
  --provider-product-id <coinbase-product-id> \
  --poll-interval-seconds 60
```

Inspect the returned definition ID, schedule, and status, then enable it:

```bash
qt data collectors list
qt data collectors enable <definition-id>
qt data collectors attempts <definition-id> --limit 100
```

Disable collection without deleting its history:

```bash
qt data collectors disable <definition-id>
```

## Read Causally

This command reads canonical storage only. It does not poll Coinbase:

```bash
qt data open-interest-latest \
  --instrument-id <canonical-instrument-id> \
  --decision-time 2026-08-01T18:00:00Z \
  --max-staleness-seconds 120
```

Use `--optional` only when the consumer contract permits structured
unavailability. Required missing or stale input fails loudly.

## Semantics And Operations

- `sample_time` is the scheduled polling instant; it is not a fabricated
  exchange event time.
- `known_at` is platform acceptance after receipt because Coinbase does not
  expose an event timestamp for this field.
- A scheduled sample is idempotent. Missed schedules and exhausted retries are
  recorded as gap evidence.
- Definitions, attempts, provider pacing, retries, and ownership leases are
  durable in PostgreSQL. The append transaction rejects a stale worker lease.
- Worker concurrency is bounded by settings under `workers.collectors`. Scale
  only after measuring provider and database behavior; database fencing remains
  authoritative across processes.
- Backtests can use only OI already accumulated in canonical storage and frozen
  during dataset preparation. Backtest execution never calls Coinbase.
