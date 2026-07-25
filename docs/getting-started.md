# Getting Started

This is the local onboarding path. Make owns Python dependency installation and
local infra. `qt setup` owns readiness checks, local operator env values, and
provider onboarding.

## Prerequisites

- Docker
- GNU Make
- Python 3.12+

## First Setup

From the repo root:

```bash
make deps
./scripts/qt setup env
make up BUILD=1 STACK_PROFILES=core
./scripts/qt setup doctor
```

`./scripts/qt` is only a dispatcher. After `make deps`, keep using
`./scripts/qt` from an unactivated shell, or activate the venv and use `qt`
directly:

```bash
source .venv/bin/activate
qt setup doctor
```

If Python 3.12 is not the default interpreter:

```bash
make deps PY=python3.12
```

If an old unsupported `.venv` already exists:

```bash
mv .venv .venv.old
make deps PY=python3.12
```

## Provider Credentials

Backtests from local or cached data do not require Coinbase credentials.
Provider-backed paper/streaming workflows do.

After the core stack is running:

```bash
./scripts/qt setup provider coinbase
```

For non-interactive setup, pass secrets from environment variables:

```bash
LOCAL_COINBASE_KEY=... LOCAL_COINBASE_SECRET=... \
./scripts/qt setup provider coinbase \
  --secret-env COINBASE_API_KEY=LOCAL_COINBASE_KEY \
  --secret-env COINBASE_API_SECRET=LOCAL_COINBASE_SECRET \
  --no-input
```

Secret values are written through the backend provider credential API into the
encrypted credential-ref store. They are not stored directly in `secrets.env`.

## Useful Commands

```bash
./scripts/qt setup doctor
./scripts/qt providers credentials list
./scripts/qt providers stream-smoke --provider COINBASE --venue COINBASE_DIRECT --symbol <symbol> --auth-mode authenticated
make help
make logs SERVICE=backend
make test
make down
```

## Boundaries

- `make deps`: local Python venv and editable install.
- `qt setup`: readiness checks, local operator env values, and provider onboarding.
- `qt providers`: provider metadata, credential refs, and provider smoke checks.
- `make`: Docker stack, tests, logs, docs sync, DB helpers, and forensics.
- `PG_DSN`: the single runtime persistence DSN.
- `secrets.env`: the one local operator env file for infrastructure values and
  the provider credential encryption key.
