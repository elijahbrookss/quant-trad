# Canonical Fact Migration Validation

Status: complete on a protected backup restore. The source database was not
modified. The canonical migration, equivalence validation, runtime cutover,
legacy deletion, and provider-disabled structured research replay all passed.

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

## BBO and depth transformation proof

The same protected restore contained 77 BBO revisions and 231 fixed-band depth
revisions. Both families completed validation-only transformation, canonical
write with full stored-column comparison, and a second execute with
`inserted_rows=0`. Every legacy row reconstructed its typed material hash
before canonicalization. Canonical provenance retains that hash, the source L2
series and source position, while payloads retain exact book values, validity,
state, units, and input fingerprints.

The live repository database test now writes BBO and depth exclusively through
the canonical writer, reads their typed projections back from canonical Facts,
and asserts zero rows for the test series in both retired typed tables. Dataset
archive resolution follows canonical source-position provenance rather than a
legacy table lookup.

The restored database was 2,099,860,271 bytes after the L2-only canonical
write; `market.fact_versions` occupied 37,257,216 bytes. This target remains
disposable and does not alter source `quanttrad`.

## Final complete migration

The final offline run used the same verified backup on the disposable target
`qt_canonical_l2_validate_20260809`. The migration validated each source row,
inserted canonical Facts, reread them, and compared the complete envelope before
any old relation was removed.

| Canonical family | Rows |
| --- | ---: |
| Candles | 29,123 |
| Open interest | 63,848 |
| Funding | 28,281 |
| Exact numeric / Chainlink scalar | 8,016 |
| Trades | 97,280 |
| Trade-flow aggregates | 45,941 |
| L2 snapshots | 25 |
| L2 mutations | 1,043 |
| BBO | 77 |
| Depth | 231 |
| Trade-flow features | 9,852 |
| Futures/spot basis | 16 |
| Derivative state | 20 |
| Market response | 1 |
| Normalized features | 41 |
| **Total** | **283,795** |

All 283,795 rows had distinct canonical version identities where required and
matched their source family counts. The final database used 21 fact types and
21 payload schemas from a 27-schema registered catalog. Six obsolete
normalization schemas had zero facts, Dataset references, Check references, or
Observation references and were explicitly skipped rather than fabricated.

The hard-cutover SQL
`scripts/db/manual_migration_canonical_fact_hard_cutover_v1.sql`:

1. asserts canonical/source exclusivity and expected per-table provenance;
2. validates Level 2 parent and child totals;
3. drops all 17 superseded fact-version relations in one transaction;
4. verifies that every retired relation is absent before commit.

After cutover, startup rejects any remaining retired relation and never creates
one. Static tests scan runtime source for all retired names. The repository has
one canonical writer and one generalized frozen Fact read; no dual-write,
fallback, compatibility flag, or legacy repository remains.

## Frozen research proof

The isolated PostgreSQL proof writes one `asset.reserve_state.v1` observation,
freezes it with an exact source binding, and verifies that the Dataset pins both
the payload schema ID and contract hash. Network/provider access is replaced by
a fail-fast trap during replay. The generalized runtime selector returns the
same `CanonicalFactRecord`, the `reserve_state` Indicator derives the same
provider-free quantity/context, and a versioned Check assertion produces the
same evidence/result hash on replay.

The existing durable Check service tests separately prove persistence,
provider-disabled replay, tamper rejection, Observation eligibility, and the
Check-to-Observation evidence link. Together these tests cover:

```text
Provider translation -> canonical Fact -> frozen Dataset
  -> provider-disabled replay -> Indicator -> Check evidence -> Observation
```

Relevant tests are
`tests/test_market_data/test_structured_fact_research_path_db.py`,
`tests/test_indicators/test_reserve_state_runtime.py`, and
`tests/test_portal/test_research_evidence_service.py`.

## Operational migration procedure

This migration is offline and destructive. Operators must:

1. stop all backend, worker, collector, and runtime writers;
2. verify the backup checksum and perform a disposable restore using the
   TimescaleDB pre/post-restore procedure;
3. apply `manual_migration_canonical_fact_store_v1.sql`;
4. run `migrate_canonical_fact_data_v1.py` in validation-only mode;
5. run the migration in execute mode, then repeat it to prove idempotency;
6. compare every family count and semantic validation report;
7. run `manual_migration_canonical_fact_hard_cutover_v1.sql` only after all
   comparisons pass;
8. start QT only after the canonical schema registry and legacy-absence checks
   pass.

Restoration instructions and the exact rollback artifact are in
[`canonical-fact-migration-backup.md`](canonical-fact-migration-backup.md).
The backup is operational protection only; it does not authorize reintroducing
the retired runtime architecture.
