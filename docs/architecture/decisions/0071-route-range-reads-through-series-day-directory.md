---
component: adr-series-day-range-directory
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - postgres
  - storage
  - explicit-migration
code_paths:
  - portal/backend/db/fact_series_day_schema.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/fact_storage_schema.py
  - portal/backend/service/storage/repos/fact_storage.py
  - portal/backend/service/storage/repos/market_data.py
---
# ADR 0071: Route Range Reads Through a Series/Day Directory

Accepted on 2026-09-17 for canonical series/time selection. HDD placement,
representative workload performance and the tiered-v1 migration remain open.

Daily storage partitions cannot be pruned by observation time: late imports and
corrections may store old observations in a recent partition. An unpruned
730-partition selector spent about 348 ms planning and executing a one-day query
in the small synthetic qualification, despite only 731 rows. An array subquery
still visited every partition; a lateral candidate pruned execution but retained
more planning work.

Maintain conservative observation bounds for each series and physical day.
Header insertion widens those bounds in the same transaction. Reject narrowing,
rekeying, removal, truncation and disabled or filtered capture. The directory
routes storage access; global Fact identity and all temporal visibility rules
remain owned by their existing contracts.

Use a STABLE, security-invoker PostgreSQL function to resolve candidate dates
and execute the narrowed header query on the calling statement's snapshot.
Quote only server-produced typed dates; bind request inputs. A new-day insert
committed between the internal reads cannot become a visible header omitted
from the earlier directory snapshot. Separate READ COMMITTED queries would
allow that race. The reader refuses more than 4,096 candidate storage dates.

Retain source filters, latest-revision and invalidation ordering, frozen cutoffs,
causal interval-close logic and verified payload hydration outside the function.
Clean bootstrap owns the model and guards. Existing databases lacking them fail
admission; runtime does not populate an empty directory over historical data.
Explicit copies install capture before their first row. Shadow cutover must
rebind the reader's composite return type to the final canonical table OID.

The tradeoffs are extra insert work, a contended row per series/day, directory
growth, function result materialization and limited outer-plan visibility.
Inventory reports directory bytes separately. Small warmed query qualification
does not establish acceptable HDD or concurrent-ingestion performance. Those
remain measured gates before deployment, along with migration and recovery.

This decision extends [ADR 0070](0070-separate-global-fact-identity-from-dated-headers.md).
