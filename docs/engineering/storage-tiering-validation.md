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
