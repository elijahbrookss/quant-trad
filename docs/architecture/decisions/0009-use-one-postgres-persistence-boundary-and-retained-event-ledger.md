---
component: adr-postgres-persistence-event-ledger
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - persistence
  - postgres
  - runtime-events
  - ledger
code_paths:
  - portal/backend/db/session.py
  - portal/backend/db/models.py
  - portal/backend/service/storage
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/bots/botlens_event_retention.py
  - scripts/db
  - docs/architecture/persistence/PERSISTENCE_BOUNDARY.md
---
# ADR 0009: Use One Postgres Persistence Boundary And A Retained Event Ledger

## Status

Accepted, backfilled on 2026-05-13.

## Context

Replay, reporting, BotLens rebuilds, and operator recovery need durable runtime
truth. At the same time, BotLens and observability can emit high-volume live
facts that would make the database unusable if every transport message became a
permanent ledger row.

The project also needs one infrastructure persistence path. Additional DSNs or
mapper layers would make failures and migrations harder to audit.

## Decision

`PG_DSN` is the only runtime persistence DSN. Durable storage is accessed
through explicit database/repository boundaries.

The runtime event ledger persists retained material truth and compact research
context. High-volume live UI/projection transport and raw observability samples
are bounded, summarized, aggregated, or kept live-only according to retention
policy.

Schema changes use clean definitions or explicit out-of-band migrations. Fresh
database bootstrap creates the current model-declared schemas, tables, and
indexes from `portal/backend/db/models.py`; provider market-data persistence
uses its own explicit SQLAlchemy table contract for configured candle,
derivative-state, and closure tables. Bootstrap inspects before creating and
does not use blanket `IF NOT EXISTS` DDL as the contract. Missing tables may be
provisioned once with a warning, missing model-declared indexes are provisioned
during bootstrap, and missing required indexes after bootstrap fail loud.
Missing required columns fail loud instead of being patched by runtime code.
Provider credential helpers validate `portal_provider_credential_refs`; they do
not own a duplicate table-creation path.

## Consequences

- Runtime, BotLens rebuilds, reports, and forensics share one durable source.
- Durable rows carry typed hot fields such as `run_id`, event name, series
  identity, decision/trade IDs, known-at time, and replay cursors.
- Unknown event classes need explicit material classification before permanent
  retention.
- Database growth is controlled without hiding material decisions, trades,
  wallet facts, diagnostics, or lifecycle truth.

## References

- [Persistence boundary](../persistence/PERSISTENCE_BOUNDARY.md)
- [Engineering contract](../../contracts/platform/03_engineering_contract.md)
- [Runtime contract](../../contracts/platform/01_runtime_contract.md)
