---
component: botlens-observability-migration-checklist
subsystem: storage
layer: reference
doc_type: architecture
status: active
tags:
  - storage
  - runtime
  - observability
  - migration
  - botlens
code_paths:
  - portal/backend/service/observability.py
  - portal/backend/service/observability_exporter.py
  - portal/backend/service/storage/repos/observability.py
  - portal/backend/service/storage/repos/runtime_events.py
  - portal/backend/service/storage/repos/lifecycle.py
  - portal/backend/service/bots/botlens_symbol_projector.py
  - portal/backend/service/bots/botlens_run_projector.py
  - portal/backend/service/bots/container_runtime.py
  - scripts/db/manual_migration_botlens_observability_persistence_v1.sql
  - scripts/db/manual_migration_botlens_runtime_event_storage_efficiency_v2.sql
---
# BotLens Observability Migration Checklist

This document is now a retirement note rather than an active cutover plan.

Current state:

- live BotLens truth is projector memory plus websocket bootstrap and typed deltas,
- durable BotLens truth is the append-only `portal_bot_run_events` ledger,
- replay continuity is a bounded in-memory ring,
- and the legacy DB-backed live cache plus feed-era compatibility seams are gone.

The remaining operator-facing storage work is limited to:

- keeping observability sink exports healthy,
- maintaining the ledger/lifecycle indexes and views that still back dashboards,
- keeping the typed runtime-event hot indexes aligned with the live query shapes:
  `(bot_id, run_id, seq, id)`, `(bot_id, run_id, series_key, seq, id)`, the partial `CANDLE_OBSERVED` bar-time window index, event-name ordered reads, correlation/root causal reads, and typed bar-time windows,
- applying `scripts/db/manual_migration_botlens_runtime_event_storage_efficiency_v2.sql` before declaring a live database index-ready,
- treating rows that never received typed hot-column values as out of contract for optimized symbol/history/forensics reads unless they are backfilled,
- and pruning any external dashboards or SQL snippets that still assume removed tables or feed surfaces.
