# Getting Started

This guide gets a local QT stack running and shows where to go next. You do not
need provider credentials to inspect QT or to run research over an existing
frozen Dataset. Provider setup is needed only for workflows that acquire or
stream external data.

If you want the research story before the installation steps, read the
[core research workflow](guides/research-workflow.md).

## Prerequisites

- Docker
- GNU Make
- Python 3.12+

## Start The Local Stack

From the repository root:

```bash
make deps
./scripts/qt setup env
make up BUILD=1 STACK_PROFILES=core
./scripts/qt setup doctor
```

`./scripts/qt` dispatches through the repository-managed Python environment.
You can keep using it from an unactivated shell, or activate the environment
and use `qt` directly:

```bash
source .venv/bin/activate
qt setup doctor
```

If Python 3.12 is not your default interpreter:

```bash
make deps PY=python3.12
```

If an older unsupported `.venv` already exists, preserve it before creating a
new one:

```bash
mv .venv .venv.old
make deps PY=python3.12
```

## Confirm It Worked

`qt setup doctor` should report readiness or name the exact missing
prerequisite. The main local surfaces are:

| Surface | Address |
| --- | --- |
| Frontend V2 | <http://localhost:5174> |
| Backend API | <http://localhost:8000> |
| TimescaleDB / PostgreSQL | `localhost:15432` |
| pgAdmin | <http://localhost:8080> |

Useful first checks:

```bash
./scripts/qt setup doctor
make ps
make logs SERVICE=backend
./scripts/qt strategies list
./scripts/qt bots list
```

The UI is for human operation and inspection. The `qt` command is the primary
repeatable workflow surface.

## Get To A First Research Run

An existing frozen Dataset lets you work without contacting a provider. Start
by inspecting the available data and commands:

```bash
./scripts/qt data series
./scripts/qt data prepare-backtest-dataset --help
./scripts/qt data freeze-dataset --help
./scripts/qt strategies list
```

With known IDs, the core backtest path is:

```bash
./scripts/qt strategies compile <strategy_id>
./scripts/qt strategies preview <strategy_id> \
  --start <iso> \
  --end <iso> \
  --interval <timeframe> \
  --instrument-id <instrument_id>

./scripts/qt bots start <bot_id> \
  --run-type backtest \
  --dataset-id <dataset_id>
./scripts/qt runs wait <bot_id> <run_id>
./scripts/qt reports summary <run_id>
./scripts/qt reports diagnostics <run_id>
```

A backtest replays the frozen evidence in time order and simulates execution
under declared assumptions. It is not a forecast. Follow the
[research workflow](guides/research-workflow.md) before interpreting or
comparing results.

## Optional Provider Setup

Backtests over canonical stored data do not require Coinbase credentials.
Provider-backed acquisition, streaming, and paper workflows do.

After the core stack is healthy:

```bash
./scripts/qt setup provider coinbase
./scripts/qt providers credentials list
```

For non-interactive setup, pass secrets from environment variables:

```bash
LOCAL_COINBASE_KEY=... LOCAL_COINBASE_SECRET=... \
./scripts/qt setup provider coinbase \
  --secret-env COINBASE_API_KEY=LOCAL_COINBASE_KEY \
  --secret-env COINBASE_API_SECRET=LOCAL_COINBASE_SECRET \
  --no-input
```

Secret values are written through the backend credential API into the encrypted
credential-reference store. They are not stored directly in `secrets.env`.

Test the configured stream explicitly:

```bash
./scripts/qt providers stream-smoke \
  --provider COINBASE \
  --venue COINBASE_DIRECT \
  --symbol <symbol> \
  --auth-mode authenticated
```

## Optional Observe-Only Paper Run

With the required provider and bot configuration in place:

```bash
./scripts/qt bots start <bot_id> \
  --run-type paper \
  --execution observe-only \
  --duration-seconds 30
```

This example follows arriving data without creating simulated orders, fills,
trades, fees, or wallet mutations. Other declared paper behavior remains
simulation. QT does not submit external exchange orders.

## Useful Local Commands

```bash
make help
make ps
make logs SERVICE=backend
make test
make check
make down
```

Start Grafana, Loki, and the remaining observability services with:

```bash
make up BUILD=1 STACK_PROFILES=all
```

## Tool Boundaries

- `make deps` creates the local Python environment and editable install.
- `qt setup` checks readiness and owns local operator/provider onboarding.
- `qt providers` handles provider metadata, credential references, and smoke
  checks.
- `qt` runs repeatable data, research, Strategy, bot, report, and comparison
  workflows.
- Make owns the Docker stack, tests, logs, documentation sync, database helpers,
  and local forensics.
- `PG_DSN` is the single runtime persistence DSN.
- `secrets.env` is the local infrastructure configuration file and contains the
  provider-credential encryption key, not raw provider credentials.

## Continue

- [Core research workflow](guides/research-workflow.md)
- [Six core promises](core-promises.md)
- [Current system](current-system.md)
- [Operator handbook](operators/README.md)
- [Architecture internals](architecture/README.md)
