# Storage tiering implementation and validation

The target is a bounded SSD working set and growing database history on enrolled
HDDs, including historical metadata and indexes. Recent and historical queries
must preserve the same identities, revisions, ordering, known-at cutoffs, gaps,
and frozen dataset results. Adding a drive should enroll capacity for new work;
it should not require rewriting all existing objects.

Current implementation covers drive identity, allocation rules, database-backed
enrollment and review, and the compact V2 settings page. It is not an executed
storage migration. Policy application is deliberately blocked until the
physical executor exists and its prerequisites are verified.

## Prove the physical design

Use disposable local data to verify schema and worker behavior. Benchmark the
actual server HDD only after its identity and contents are audited and the
filesystem is prepared. Test files and databases must have separate, explicitly
named roots; never overwrite live relations or existing recovery artifacts.

Compare the current layout with candidate layouts using the same workload and
record source revision, drive UUIDs, row counts, byte counts, query parameters,
cache state, concurrency and timings. Small cached samples establish correctness,
not two-year performance. Include a sample larger than memory when feasible,
and distinguish sequential throughput from random indexed reads.

- Measure representative recent queries at p50/p95/p99 while normal collection
  and movement compete. Initial acceptance target: recent p95 within 10% of the
  matched baseline, with no sustained collector backlog.
- Query historical-only and SSD/HDD boundary ranges. Compare ordered IDs and
  content hashes, revisions, known-at boundaries, frozen datasets and missing
  data behavior. Set a cold-query latency budget from actual user workloads
  before declaring performance acceptable.
- Replay representative collector batches and measure accepted throughput,
  transaction latency, CPU, I/O wait and backlog. Require movement throughput
  above the observed arrival rate with headroom; a short burst is not proof.
- Interrupt copying, verification, location commit and source retirement
  separately. Retry after process restart and lease expiry. Verify no lost,
  duplicated, replaced or unreadable object and no stale worker can commit.
- Remove or make a test target read-only, exhaust its reserve and add a second
  HDD target. Expect explicit blocked work; new work can use eligible capacity
  while existing reads remain bound to recorded locations.
- Restore a completed, rotated recovery copy into an isolated database and
  verify referenced archive objects and representative frozen dataset results.

## Two-year partition-horizon check

Run the selection experiment with the isolated database runner:

```bash
./scripts/ci/run_test_suite.sh db tests/test_market_data/test_fact_header_horizon_db.py -s
```

It runs 32-day smoke and 730-day full-horizon cases with one synthetic row per
day plus a late correction, and exercises the actual canonical range selector. An observation on the first day receives a
correction stored on the last day. Latest selection must return that correction;
commit-frozen and known-at-frozen reads must retain the original. Payload
hydration is replaced only in this selection fixture and remains covered by the
separate storage integration tests. The fixture uses a partitioned hot table
covering its most recent 32 days and reports executed hot partitions as well.
A latest correction must join its hot payload; an older frozen row outside that
window must remain selected for cold hydration.

The report includes PostgreSQL planning/execution time for the production
directory reader and an unpruned reference using the same selector and filters.
Directory rows come from the actual ORM table and insertion trigger, not a
fixture backfill. Late/direct writes, rollback, concurrent range expansion,
missing/incompatible capture and a new-day commit between internal reads are
covered by the directory guard tests. PostgreSQL's
[STABLE function snapshot semantics](https://www.postgresql.org/docs/15/xfunc-volatility.html)
keep directory resolution and the dynamically planned header read on the calling
statement's snapshot. Separate READ COMMITTED statements do not provide that
guarantee.

The selection/guard fixtures omit extension installation in their disposable
databases because they exercise native PostgreSQL behavior. Existing migration
fixtures retain their TimescaleDB and pgcrypto setup. A function scan hides its
nested execution plan, so its executed-partition metric is unavailable rather
than zero; report total execution time, including internal planning.

These small, warmed local results reveal partition fan-out and selection errors.
They do not establish cold HDD latency, concurrent ingestion performance, full
payload hydration, realistic index size, or two-year capacity. Do not turn the
reported wall-clock times into machine-independent test thresholds.

## Time the migration before promising the cutover

Measure copy, index construction, catch-up, verification, required lock windows
and rollback separately. Use sustained rates under collection load. The user's
limit is at most 24 hours for the complete migration sequence, not just the
initial copy. If catch-up throughput does not exceed incoming writes, or total
duration exceeds that limit, stop before cutover and compare a different layout
or an explicitly approved clean start. A service restart does not imply a data
wipe. Preserve the existing data and recovery material by default.

## Finish the operational boundary

Implement the physical layout, schema admission, durable fenced movement jobs,
multi-target archive publication/read/delete, and automatic recovery-copy
scheduling before enabling Apply. CLI and UI must call the same backend-owned
actions. Add drive enrollment must include administrator-prepared filesystem
identity and stable container mounts; it must not accept shell text from a UI.

Capacity forecasts must sum database heaps, indexes, retained raw/canonical
objects, bounded recent storage, recovery copies, migration scratch and reserve.
Use measured daily deltas, label sample dates, show an uncertainty range, and
report time to reserve as well as time to full. An archive-only estimate is not
a whole-system forecast. Local HDD recovery copies do not protect against loss
of that HDD; off-server/S3 copies are deferred by the user.

No seven-day waiting gate applies. Completion requires the concrete correctness,
performance, recovery and migration evidence above, actual deployment, physical
placement verification, and updated capacity evidence.

## Prepared HDD initialization

The host helper scripts/automation/storage_host_prepare.py consumes a reviewed
JSON plan with device (stable by-id path), expected_serial, expected_size_bytes,
filesystem_uuid, mountpoint and owner. The exact device must pass the existing
read-only audit and a SMART health check. It refuses partitions, foreign mount
points, existing signatures and conflicting fstab entries.

Initial execution requires explicit --initialize-empty-device authorization. It
creates whole-disk ext4 with the plan UUID, a 64 KiB inode ratio and zero
filesystem reserved percentage; QT's own capacity reserve remains separate.
Inode and journal initialization finish before benchmarking. An interrupted run
can reuse only ext4 with that exact planned UUID. No force-format flag is used.

The helper mounts the drive below /srv/quanttrad/storage, creates a private
benchmark directory for the named owner, and preserves an original fstab copy
before atomically adding a UUID mount. The nofail boot option lets the host
boot if this auxiliary drive is absent; eventual storage consumers must still
require UUID admission and the database service's mount dependencies. This
preparation does not activate app policy, change running containers, move the
database, or enable retention. A signature-free disk is not proof that it has
no valuable raw data; the operator must approve initialization of that device.


## Disposable catalog and filesystem qualification

The catalog and namespace verifier changes are pending database qualification.
After any currently running isolated database suite finishes, run these cases
through the supported disposable runner:

```bash
./scripts/ci/run_test_suite.sh db tests/test_market_data/test_header_catalog_db.py tests/test_market_data/test_header_namespace_db.py tests/test_market_data/test_header_history_move_lock_db.py -x -s
```

The test image includes PostgreSQL 15 server tools with distribution-managed
cluster creation disabled. The namespace fixture creates its own private
cluster as a non-root OS user, disables TCP listening and host authentication,
and uses a temporary Unix socket. Its subprocess receives no inherited DSN or
password environment. It stops and removes only its generated cluster.
The normal test database remains under the existing isolated runner.

The fixture uses real PostgreSQL control/process/file observations and a
synthetic udev UUID entry on the disposable filesystem. It is evidence for
catalog and namespace correctness, not actual SSD/HDD performance, backup
durability, migration throughput or deployment readiness. Do not replace its
temporary directory or socket with a server path.


The historical-lock probe holds an exclusive lock on an old header partition,
proves that the lock blocks a direct historical read, then requires the actual
series/day reader and a recent insert to finish within a one-second statement
budget. This is a minimum responsiveness gate before choosing a physical
mover. If it fails, investigate query routing or a different copy/cutover
strategy before enabling movement. A pass still requires actual concurrent
table/index movement, exact-ID paths and hot-join workload qualification.


The namespace fixture also injects transaction failures after the table move
and after the table-plus-index moves. It requires the original file paths,
physical file identifiers and row hashes after rollback. This checks the
PostgreSQL primitive only; a durable worker still needs tests for process loss,
ambiguous COMMIT results, retry idempotency and reservation reconciliation.
