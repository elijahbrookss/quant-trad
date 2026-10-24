# Storage Qualification And Recovery

Qualify the exact source and intended layout before a preserving migration or
unattended storage policy is activated. Functional tests, actual-drive workload
measurements and deployment admission answer different questions; a passing tiny
fixture cannot establish production throughput, capacity or migration duration.
The [storage boundary](../architecture/persistence/STORAGE_MANAGEMENT.md) owns
placement and policy semantics. [Server deployment](server-deployment.md#storage-move-and-recovery)
owns activation, holds and runtime recovery.

## Establish the source and recovery boundary

Record the full application revision, PostgreSQL/TimescaleDB versions, logical
schema, database/cluster identity, archive roots, registered filesystem UUIDs,
policy and intended transition. Inspect starting schema and dependencies before
selecting an upgrade. New empty databases bootstrap directly from the current
model; historical SQL is not clean-install input.

Stop all application writers when the selected offline cutover requires it:
backend, workers, collectors and bot runtimes. Verify drained processes and
leases; a stopped UI is not a writer fence. Preserve the original source and
archive bytes until restoration and post-switch reads have been accepted.

A recovery point must include the database and every raw/canonical archive object
referenced by its snapshot, plus protected operator configuration and required
keys. A live copy of PGDATA is not a valid database snapshot. A database dump
alone cannot restore cold Facts. Coordinate database consistency with archive
publication/expiry using the reviewed recovery procedure. Keep independent key
recovery available; copies on the same disk do not survive loss of that disk.

## Restore a logical backup in isolation

For a reviewed logical backup, retain its dump, globals/role metadata, machine-
readable checksum manifest, source identity and restore receipt. Verify hashes
before restore. `pg_restore --list` validates archive readability, not recovery.
Use a compatible PostgreSQL/TimescaleDB image and an empty isolated target with
collectors, external-order submission and provider enrollment disabled.

The normal PostgreSQL custom-format backup primitives are `pg_dump -Fc` and
`pg_dumpall --globals-only --no-role-passwords`. Choose output paths and access
controls explicitly; do not log credentials or assume globals contain passwords.
Inspect role SQL rather than applying it blindly to an existing cluster.

The restore sequence is:

1. Create an empty target database and install its required extensions.
2. Call `timescaledb_pre_restore()` in that target.
3. Restore pre-data, data and post-data with `pg_restore --exit-on-error`, using
   `--no-owner --no-privileges` only when the selected ownership plan permits it.
4. Put the unique `market.fact_schemas` TABLE DATA entry first in the data TOC:
   hot payload CHECKs call `market.validate_fact_payload`, which reads that
   catalog. PostgreSQL does not infer this data-order dependency from a function.
5. Restore serially for this schema; do not assume `pg_restore -j` is supported.
6. Call `timescaledb_post_restore()`, including cleanup of restore mode after an
   unsuccessful attempt, then run `ANALYZE`.
7. Admit the restored schema through QT and compare the source snapshot's exact
   counts, identities and hashes, representative current/corrected/frozen reads,
   book replay and copied archive checksums. Prove readers use the independent
   restored files rather than the original archive root.

Use `pg_restore --list <dump>` to prepare the reviewed TOC ordering; do not disable
CHECKs or immutability guards to force a restore through. Run schema/database
smoke checks against the isolated target before considering any source replacement.
A successful historical restore receipt applies to its recorded schema and data,
not every later release. Never restore over the running source to test recovery.

## Verify encrypted incremental recovery

Use the [encrypted activation procedure](server-deployment.md#encrypted-incremental-recovery-activation)
for the serving installation: pinned native tools, independent database/archive
keys, admitted key/socket binds, PostgreSQL WAL archiving and a paired archive
inventory. Restore the selected consistency endpoint; a paired archive snapshot
does not authorize arbitrary later WAL recovery. Include complete native chains,
WAL, archive packs and rotation peaks in capacity accounting.

`scripts/ci/rehearse_incremental_recovery.py` provides a native-tool fixture using
explicit local binaries and its own temporary cluster, data and keys. It rejects
root, clears inherited connection settings and accepts no operational DSN or
existing PGDATA. The CI image `docker/test/incremental-recovery.Dockerfile` runs
without host volumes or networking under resource limits. Verify correct-key
restore, wrong-key refusal, backup-process interruption, dependency-chain expiry
and archive pruning. Native-tool correctness remains separate from QT publication,
current-layout recovery admission and application acceptance.

## Select an explicit legacy cutover

Manual migrations are reviewed operator procedures for exact starting contracts.
Inspect the SQL and source schema, verify a restore-tested backup, and use
`psql -v ON_ERROR_STOP=1`. Respect each script's transaction and writer-fence
requirements; concurrent index scripts must run outside a transaction.

For the pre-canonical Fact transition, the reviewed sequence is:

1. Stop all writers and verify the starting relation inventory.
2. Apply `scripts/db/manual_migration_canonical_fact_store_v1.sql`.
3. Run `scripts/db/migrate_canonical_fact_data_v1.py` in validation-only mode,
   then execute mode, then repeat execute to prove zero-insert idempotency.
4. Compare every old/new envelope field, family count, revision, commit/known-at
   selection, provenance/material/quality hash, Dataset identity and record link.
5. Run `scripts/db/manual_migration_canonical_fact_hard_cutover_v1.sql` only after
   every comparison passes. It verifies exclusivity and removes retired relations
   transactionally. Current startup rejects dual Fact ownership.
6. Admit startup and provider-free frozen replay before releasing writers.

Collector control-plane upgrades use
`scripts/db/manual_migration_collector_operations_v1.sql` only for their admitted
starting schema. Preserve desired intent, control generations, immutable operation
history and referenced definitions. Evidence constraints blocking cleanup are a
reason to investigate, never permission to bypass them. The current
[collector guide](../guides/collector-operations.md) owns operation semantics.

Rollback across an incompatible cutover means restoring the verified point into
a new isolated database, checking it, and making an explicitly reviewed database
replacement. It does not mean reintroducing retired runtime tables or fallback
readers. Retained pre-switch rows alone cannot recover writes made after switching.

## Measure the actual layout

Use disposable roots and data, then actual hardware only under an explicitly
reviewed measurement plan. Record source, UUIDs, sizes, cache state, query
parameters, concurrency and timings. Compare a matched baseline with movement:

- recent p50/p95/p99, historical-only and boundary-range reads;
- collection acceptance rate, transaction latency, lag, CPU and I/O pressure;
- ordered identities, corrections, known-at cutoffs and frozen results;
- heaps, indexes, TOAST, routing/global identity metadata and raw mappings;
- copy/index/catch-up/final-verification time and the complete writer pause;
- WAL, temporary files, ongoing growth, recovery chains, scratch and reserve.

Set workload latency and total-duration budgets before execution. Demonstrate
sustained catch-up above incoming writes with headroom. A short warmed result,
synthetic tmpfs history volume or sequential throughput sample does not qualify
cold HDD random reads or whole-system capacity. Forecast time to reserve using
measured dated growth and uncertainty, rather than archive compression alone.

## Exercise interruption and bounded retry

Interrupt copy, verification, location commit, source retirement, runtime
activation and recovery publication separately. Verify retained sources,
transaction rollback, durable progress and lease fencing. A lost COMMIT response
is an uncertain result: inspect persisted completion before retrying a mutation.
Never reset the original attempt deadline or change bound inputs to bypass refusal.

Test missing/read-only/wrong-UUID mounts, reserve exhaustion, changed indexes,
corrupt source/destination bytes, stale policy and changed worker ownership.
Require explicit blocked outcomes and independently readable frozen results.
Recovery rotation must preserve the last completed point after failure and retire
only dependency-safe completed generations. Restore the selected retained pair;
backup creation or a checksum alone is not a restore proof.

## Use the existing disposable proofs

Select the topology for the seam under review. Do not run these commands against
an operational DSN or overlap owned local stacks:

```bash
./scripts/ci/run_test_suite.sh db tests/test_market_data/test_fact_header_horizon_db.py -s
./scripts/ci/run_test_suite.sh db tests/test_market_data/test_header_catalog_db.py tests/test_market_data/test_header_namespace_db.py tests/test_market_data/test_header_history_move_lock_db.py -x -s
./scripts/ci/run_test_suite.sh storage-demo
python scripts/ci/test_server_core_recreation.py --storage-layout
python scripts/ci/test_server_promotion.py --storage-layout
```

The horizon fixture uses sparse synthetic rows to expose partition fan-out and
late-correction selection. FunctionScan hides nested executed-partition counts;
unavailable is not zero. Namespace fixtures use owned private PostgreSQL clusters
and separate disposable filesystems. Storage-demo composes canonical ingestion,
archive/reclaim, movement and frozen reads. These establish bounded correctness,
not actual-drive durability or a full-volume performance guarantee.

Use the [testing policy](testing/testing-strategy.md) and
[CI topology](testing/ci-test-topology.md) for current coverage, optional topology
requirements and exact-source evidence. Retain release-specific qualification
and installation receipts privately; this manual does not certify a deployed host.
