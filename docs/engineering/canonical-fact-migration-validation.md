# Canonical Fact Migration Validation

Status: core historical Fact transformation validated on the protected backup;
runtime cutover, structured-family migration, dataset-hash equivalence, and
legacy removal remain pending.

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

## Boundary

This checkpoint proves the four active core Fact families can be migrated
losslessly. It does not authorize deletion of the old relations. The remaining
cutover validator must also reproduce frozen Dataset hashes, Check/Observation
links, gaps/quality evidence, and every retained structured market-state family
before legacy removal.
