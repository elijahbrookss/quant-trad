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

Provider credentials and operator overrides belong in `secrets.env` or the provider credential store, not in tracked defaults.

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
