# Getting Started

This page covers a local stack run. It does not replace the platform contracts or provider-specific setup details.

## Prerequisites

- Docker
- GNU Make
- Python 3.12+ for local tooling outside Docker

## Local Secrets

Create the one local operator env file:

```bash
cp secrets.env.example secrets.env
```

Fill the local values required by the stack:

```bash
POSTGRES_DB=quanttrad
POSTGRES_USER=quanttrad
POSTGRES_PASSWORD=<local-db-password>
PG_DSN=postgresql+psycopg2://quanttrad:<local-db-password>@localhost:15432/quanttrad
PGADMIN_DEFAULT_PASSWORD=<local-pgadmin-password>
```

If you plan to save provider credentials, also set a credential encryption key:

```bash
QT_SECURITY_PROVIDER_CREDENTIAL_KEY=<fernet-key>
```

Generate one with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Provider credentials belong in the encrypted provider credential store, not in
tracked defaults. `secrets.env` should hold only local database values,
credential-store encryption, and deliberate operator overrides.

## Start Core Services

```bash
make up BUILD=1 STACK_PROFILES=core
```

Core endpoints:

- Frontend: `http://localhost:5173`
- Backend API: `http://localhost:8000`
- TimescaleDB: `localhost:15432`
- pgAdmin: `http://localhost:8080`

## Add Observability

```bash
make up BUILD=1 STACK_PROFILES=all
```

Observability endpoints:

- Grafana: `http://localhost:3000`
- Loki: `http://localhost:3100`

## Coinbase Credentials

Coinbase Direct is the active provider-backed paper/streaming path. Backtests
from local or cached data do not require Coinbase credentials, but provider
streaming and authenticated provider calls do.

After the core stack is running and `QT_SECURITY_PROVIDER_CREDENTIAL_KEY` is
set, store Coinbase credentials with:

```bash
qt providers credentials schema --provider COINBASE --venue COINBASE_DIRECT
qt providers credentials add --provider COINBASE --venue COINBASE_DIRECT
```

Required Coinbase credential fields:

```text
COINBASE_API_KEY
COINBASE_API_SECRET
```

## Useful Commands

```bash
make deps
make help
make ps
make logs SERVICE=backend
make restart BUILD=1
make test
make check
make down
```

Use `qt` for normal bot, provider, report, and experiment workflows. Use
`qt mcp serve` when an MCP host needs the same workflow boundary over stdio.

## Configuration Notes

- `secrets.env`: the one local operator env file.
- `secrets.env.example`: the critical-path template for local DB plus Coinbase credential storage.
- `.env` / `.env.test`: ignored scratch only; do not rely on them for normal operation.
- `portal/frontend/.env`: optional ignored Vite override for frontend-only local debugging.
- `portal/frontend/.env.example`: optional frontend override examples; Docker compose injects the normal frontend values.
- `PG_DSN`: the single runtime persistence DSN.

The root `pyproject.toml` owns the monolithic Python package surface. `make deps`
uses `uv pip` when `uv` is installed and falls back to `pip`; Docker images also
use pinned `uv` for faster dependency layers. The project is not adopting a
`uv.lock` workflow yet.

## Docs Sync

After updating docs in this repo, run:

```bash
make sync-docs
```

If no sync destination is configured, the target exits cleanly after explaining what to set.

## Next

- Read [overview](overview.md) for the project model.
- Read [data layer](engineering/data-layer.md) before changing providers.
- Read [observability](engineering/observability.md) before adding logs or metrics.
