# Canonical Fact Migration Pre-Cutover Backup

Status: verified and safe to use as the rollback artifact for the generalized
Fact cutover.

## Backup identity

| Field | Value |
| --- | --- |
| Backup timestamp | `2026-08-09T19:58:28Z` |
| Source database | `quanttrad` |
| Source database role | `quanttrad` |
| PostgreSQL | `15.6` |
| TimescaleDB | `2.14.2` |
| Docker container | `quant-trad-tsdb-1` |
| Docker volume | `quant-trad_tsdb-data` |
| Git migration boundary | `77f4d41805031ad30cffd5b03983bdd09e76acba` |
| Active branch at capture | `feat/market-data-plane` |
| Logical schema boundary | market-data v2 hard cutover, shared fact commit clock v1, exact numeric fact store v1, and the current market-structure ORM schema; before generalized Fact storage or data migration |
| Prior recorded migration | `market_data_v2_hard_cutover`, completed `2026-07-26T10:02:12.974605Z` |
| Backup mechanism | PostgreSQL custom-format logical dump plus globals-only SQL |
| Local artifact directory | `output/db-backups/pre-canonical-fact-20260809T195828Z/` |

Docker Desktop's named-volume deployment does not expose a repository-owned,
portable native snapshot mechanism. The rollback artifact is therefore a
complete PostgreSQL logical backup made with PostgreSQL-native tooling:

```text
pg_dump -Fc --compress=6 --create --verbose
pg_dumpall --globals-only --no-role-passwords
```

The globals file deliberately contains no role passwords. It is an operator
aid, not a credential backup.

## Artifacts and checksums

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| `pre-canonical-fact-20260809T195828Z.dump` | 208,893,462 | `d9cd0a04eafbe1abe068ab9cb3e7313ac4482ac9873d580de3e6b7bee8104972` |
| `pre-canonical-fact-20260809T195828Z.globals.sql` | 376 | `fba8b83314b1b4320a6934449518fc0eaef49edd3421319d2dde2ccb96cbba32` |
| `pre-canonical-fact-20260809T195828Z.archive-list.txt` | recorded with backup | `0d34377760a361cd4a58c53a07602fb8476947ee6f6e3878def13c6741873c47` |
| `pre-canonical-fact-20260809T195828Z.restore-verification.txt` | recorded with backup | `a7d3fc51d24b05ecdc168b0c6d737b5ba1959df09f6fbc1d946ecfcbf0427174` |

`SHA256SUMS` in the artifact directory is the machine-readable checksum
manifest. Verify it before restoration.

## Verification performed

`pg_restore --list` parsed the entire archive successfully. A full restore was
then performed into the disposable database
`qt_restore_verify_20260809195828` using the required TimescaleDB restore mode:

1. create an empty target database;
2. create the TimescaleDB extension in the target;
3. call `timescaledb_pre_restore()`;
4. restore serially with `pg_restore`;
5. call `timescaledb_post_restore()`;
6. run `ANALYZE`;
7. query restored schema and evidence counts.

The first trial without TimescaleDB restore mode was rejected while recreating
hypertable foreign keys. No source data was changed. Repeating with the required
pre/post-restore sequence completed successfully. The verified disposable
database was dropped after validation.

Verified restored values:

| Evidence | Restored value |
| --- | ---: |
| User tables | 249 |
| `market.candle_versions` | 29,123 |
| `market.open_interest_versions` | 63,848 |
| `market.funding_rate_versions` | 28,281 |
| `market.numeric_fact_versions` | 8,016 |
| `market.datasets` | 54 |
| `market.dataset_series` | 105 |
| `market.gap_evidence` | 165 |
| `public.portal_instruments` | 68 |
| Restored database size | 2,066,953,007 bytes |

## Restoration procedure

Do not restore over the running production/local source database. Stop QT
backend, workers, collectors, and runtimes, then restore to a newly created
database first. The commands below assume the dump has been copied into the
PostgreSQL container and that `<restore-db>` is a disposable or explicitly
approved target:

```bash
docker cp \
  output/db-backups/pre-canonical-fact-20260809T195828Z/pre-canonical-fact-20260809T195828Z.dump \
  quant-trad-tsdb-1:/tmp/pre-canonical-fact-20260809T195828Z.dump

docker exec quant-trad-tsdb-1 \
  createdb -U quanttrad '<restore-db>'

docker exec quant-trad-tsdb-1 \
  psql -U quanttrad -d '<restore-db>' -v ON_ERROR_STOP=1 \
  -c 'CREATE EXTENSION IF NOT EXISTS timescaledb'

docker exec quant-trad-tsdb-1 \
  psql -U quanttrad -d '<restore-db>' -v ON_ERROR_STOP=1 \
  -c 'SELECT timescaledb_pre_restore()'

docker exec quant-trad-tsdb-1 \
  pg_restore -U quanttrad -d '<restore-db>' --exit-on-error \
  --no-owner --no-privileges \
  /tmp/pre-canonical-fact-20260809T195828Z.dump

docker exec quant-trad-tsdb-1 \
  psql -U quanttrad -d '<restore-db>' -v ON_ERROR_STOP=1 \
  -c 'SELECT timescaledb_post_restore()'

docker exec quant-trad-tsdb-1 \
  psql -U quanttrad -d '<restore-db>' -v ON_ERROR_STOP=1 -c 'ANALYZE'
```

Do not use parallel `pg_restore -j` for this TimescaleDB restore. Inspect
`pre-canonical-fact-20260809T195828Z.globals.sql` before applying it; the normal
local deployment already has the required `quanttrad` role.

After restoration, compare the counts above, run `make db-status`, and execute
the canonical repository/database smoke tests against the restored target
before considering any source-database replacement.

## Protection boundary

This backup protects the pre-generalized-Fact state only. It does not justify a
runtime compatibility path, dual writes, fallback reads, or a rollback-shaped
new schema. The equivalence validator passed on the disposable restore before
the superseded tables were removed there.

The source database captured by this artifact remained unchanged during restore
validation. It was subsequently cut over offline on 2026-08-09 CDT only after
the checksum, source boundary, validation-only migration, execute migration,
zero-insert idempotency pass, and transactional hard-cutover gates were repeated
against the source. The artifact remains the rollback boundary. Recovery means
restoring it into a new database and performing an explicitly approved database
replacement; it does not mean reintroducing a legacy runtime path.
