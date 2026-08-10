# Canonical Fact Migration Validation

Status: core facts, trades, trade-flow aggregates, and Level 2 snapshots and
mutations validated on protected backup restores. Remaining derived structured
families, dataset-hash equivalence, final runtime cutover, and legacy removal
remain pending.

## Validation target

The pre-migration custom-format backup recorded in
[`canonical-fact-migration-backup.md`](canonical-fact-migration-backup.md) was
restored a second time into the disposable database
`qt_canonical_migrate_validate_20260809` in `quant-trad-tsdb-1`.

Before restoration, the dump SHA-256 was rechecked as:

```text
d9cd0a04eafbe1abe068ab9cb3e7313ac4482ac9873d580de3e6b7bee8104972
```

The restore used `timescaledb_pre_restore()` and
`timescaledb_post_restore()`. Restored pre-migration counts matched the backup
manifest exactly:

| Relation | Rows |
| --- | ---: |
| `market.candle_versions` | 29,123 |
| `market.open_interest_versions` | 63,848 |
| `market.funding_rate_versions` | 28,281 |
| `market.numeric_fact_versions` | 8,016 |
| `market.datasets` | 54 |
| `market.dataset_series` | 105 |

The source database `quanttrad` was not modified.

## Core transformation proof

The canonical schema migration was applied to the disposable restore. Then
`scripts/db/migrate_canonical_fact_data_v1.py` was run in validation-only mode,
execute mode, and execute mode a second time for idempotency.

All 129,268 source rows passed:

- reconstruction through the original typed Fact validator;
- stored v1 row-hash reproduction;
- source identity reproduction;
- canonical payload contract validation;
- causal clock and state constraints;
- immutable insert enforcement;
- full 32-column old-to-new envelope comparison after insertion.

The second execute pass produced the same 129,268 canonical identities without
updates or duplicate rows.

Independent PostgreSQL aggregation produced:

| Canonical payload schema | Rows | Min commit | Max commit | Distinct IDs |
| --- | ---: | ---: | ---: | ---: |
| `candle.ohlcv.v1` | 29,123 | 1 | 259,773 | 29,123 |
| `derivatives.funding_rate.v1` | 28,281 | 1,744 | 283,767 | 28,281 |
| `derivatives.open_interest.v1` | 63,848 | 1,541 | 283,769 | 63,848 |
| `market.reference_price.v1` | 8,016 | 260,072 | 283,825 | 8,016 |

The per-family counts and commit extrema exactly match the four source tables.

## Structured trade transformation proof

After adding the provider-neutral `market.trade.v1` and
`market.trade_flow.v1` payload contracts, the protected dump was restored
again into the disposable database
`qt_canonical_structured_validate_20260809`. TimescaleDB pre/post-restore mode
was used, and all six source counts matched the backup:

| Source relation | Rows |
| --- | ---: |
| `market.candle_versions` | 29,123 |
| `market.open_interest_versions` | 63,848 |
| `market.funding_rate_versions` | 28,281 |
| `market.numeric_fact_versions` | 8,016 |
| `market.market_trade_versions` | 97,280 |
| `market.trade_flow_aggregate_versions` | 45,941 |

The expanded offline migration completed one validation-only pass, one full
write-and-compare pass, and one idempotency pass. All **272,489** rows passed.
The second execute pass reported `inserted_rows=0` for every family while
revalidating all stored canonical columns.

Trade validation reconstructs the original typed `MarketTradeFact` and proves
both its stored `material_hash` and `row_hash`. Provider product/trade IDs,
delivery kind, source sequence, raw archive identity, product-definition
identity, and the retained legacy provenance hash move into canonical
provenance. The typed payload contains the atomic financial observation:
price, reported quantity and unit, translated contract/base/quote quantities,
currencies, and maker/aggressor sides.

Trade-flow validation reconstructs the original typed
`TradeFlowAggregateFact` and proves its stored `material_hash`. Counts,
volumes, CVD, and OHLC remain one atomic typed payload. Coverage/input
identities remain provenance; completeness and late-trade semantics remain
quality evidence. Each derived series was required to resolve to exactly one
upstream canonical source identity. No orphaned or ambiguous source mapping
was found.

Independent PostgreSQL aggregation after the committed write produced:

| Canonical payload schema | Rows | Min commit | Max commit | Distinct IDs |
| --- | ---: | ---: | ---: | ---: |
| `candle.ohlcv.v1` | 29,123 | 1 | 259,773 | 29,123 |
| `derivatives.funding_rate.v1` | 28,281 | 1,744 | 283,767 | 28,281 |
| `derivatives.open_interest.v1` | 63,848 | 1,541 | 283,769 | 63,848 |
| `market.reference_price.v1` | 8,016 | 260,072 | 283,825 | 8,016 |
| `market.trade.v1` | 97,280 | 4,743 | 283,771 | 97,280 |
| `market.trade_flow.v1` | 45,941 | 4,744 | 200,242 | 45,941 |

The canonical total was 272,489 rows and 272,489 distinct canonical version
IDs. The source `quanttrad` database was queried separately after validation
and remained unchanged. The disposable database occupied 2,722 MB before it
was dropped.

## Level 2 transformation proof

The protected backup was restored again into the isolated database
`qt_canonical_l2_validate_20260809`. Its Level 2 source counts matched the
recorded boundary exactly:

| Source relation | Rows |
| --- | ---: |
| `market.l2_snapshot_versions` | 25 |
| `market.l2_snapshot_levels` | 941,816 |
| `market.l2_mutation_batches` | 1,043 |
| `market.l2_mutations` | 11,244 |

The migration completed a validation-only pass, a write-and-full-column-
comparison pass, and a second execute pass with `inserted_rows=0`. Independent
SQL then proved 1,068 canonical parent rows, 1,068 distinct IDs and observation
keys, 941,816 strict snapshot entries, and 11,244 strict mutation entries.
`jsonb_array_length(entries)` matched every parent `entry_count`. Parent commit
sequence, external/platform clocks, state hashes, event hashes, validity IDs,
raw-record IDs, source positions, and entry counts produced zero mismatches.

Snapshots preserve the complete canonical book state in deterministic
bid-then-ask price order. Their retained provider-event material hash remains
separate evidence because the historical child table intentionally stored book
order rather than provider delivery order. Mutation batches retain and verify
their original mutation ordinal and event material hash exactly. This does not
claim that sorted snapshot state can recreate provider delivery order; the raw
archive reference remains authoritative for that evidence.

Two of 1,043 mutation batches had provider event timestamps ahead of QT
`known_at`, with maximum skew 945.058 ms. All retained rows satisfied
`accepted_at >= received_at` and receipt-based `known_at >= accepted_at`.
Canonical validation therefore keeps those platform causal constraints while
allowing external clocks to lead QT's clock. No clock was rewritten.

The restored database was 2,099,860,271 bytes after the L2-only canonical
write; `market.fact_versions` occupied 37,257,216 bytes. This target remains
disposable and does not alter source `quanttrad`.

## Boundary

This checkpoint proves the four active core Fact families plus canonical
trades, trade-flow aggregates, and atomic Level 2 book facts can be migrated
without discarding retained semantics. It does not authorize deletion of the
old relations. The remaining cutover validator must also reproduce frozen
Dataset hashes, Check/Observation links, gaps/quality evidence, and every
retained derived market-state family before legacy removal.
