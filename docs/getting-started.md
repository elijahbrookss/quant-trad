# Getting Started

This page covers a local stack run. It does not replace the platform contracts or provider-specific setup details.

## Prerequisites

- Docker
- GNU Make
- Python 3.12+ for local tooling outside Docker

## Local Secrets

Create the local secrets file:

```bash
cp secrets.env.example secrets.env
```

Fill the local values required by the stack:

```bash
POSTGRES_DB=quanttrad
POSTGRES_USER=quanttrad
POSTGRES_PASSWORD=<local-db-password>
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

Provider credentials and operator overrides belong in `secrets.env` or the
encrypted provider credential store, not in tracked defaults.

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

- `.env`: tracked local defaults for Python tooling and tests.
- `.env.test`: Docker test defaults.
- `secrets.env`: private credentials and operator overrides.
- `portal/frontend/.env`: frontend Vite defaults.
- `PG_DSN`: the single runtime persistence DSN.

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
