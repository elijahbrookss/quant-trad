# Dated Fact Header Layout

This branch introduces clean-schema support for dated canonical headers and a
global identity registry. It is a development milestone, not an upgrade-ready
release. Do not deploy it to an existing v1 database: startup deliberately
rejects that layout. The resumable v1-to-v2 operator cutover and physical drive
placement are still under implementation. Continue running the installed
release until that cutover has been rehearsed and reviewed.

A clean database uses market.fact_identities for global Fact IDs and the
series/observation/revision key. market.fact_versions holds detailed immutable
headers in daily storage_day partitions. Each header binds to exactly one
identity and placement day. Database triggers create that identity with the
header and require the matching header before commit. Hot payloads, aliases and
canonical archive dependencies reference global identity. A correction still
appends a revision; placement does not change known_at, hashes or frozen
Dataset identity.

The daily partition catalogue distinguishes a new empty day from a missing
historical table. Runtime may provision a new registered day, but cannot replace
or adopt an unknown table. A missing, detached or incompatible registered
partition fails admission. Startup also verifies identity keys, binding,
functions and enabled guards without backfilling existing rows.

## What the current code does

Clean bootstrap creates the complete v2 schema atomically. Ingestion locates the
latest revision through the global registry and joins its header by ID and
storage day. Ordinary query semantics remain the same. This slice has no
tablespace allocator or mover: newly created tables still use the database's
default tablespace. Successful clean-schema tests therefore prove integrity,
not HDD placement or performance.

The historical full-row-to-tiered offline helper remains explicit and
writers-stopped. In this branch it builds the current v2 clean target, retaining
the original full-row source and comparing every copied field. It cannot
upgrade an already-tiered v1 installation or resume a copy started by the older
v1 implementation. Use the matching installed release to finish an earlier
copy before considering a separate v2 cutover. Never change a certificate to
bypass admission.

Retention inventory counts the complete header partition tree, including
physical indexes and TOAST, instead of sizing the empty parent. It reports
global_identity_bytes separately. Header inventory is bounded independently
of remaining hot payload partitions; reclaiming a payload cannot make retained
header history disappear from the total. These are database-wide component
sizes, not yet a per-drive forecast.

## What must precede an existing-server upgrade

The remaining operator procedure must create a shadow dated header layout,
capture new immutable inserts, and copy bounded pages while collection
continues. Each page and its cursor must commit together. Prevalidated incoming
foreign keys, views, sequence ownership and dependency OIDs must be explicitly
rewired; renaming tables alone does not move those dependencies. Final
catch-up, verification and reader/writer fencing need a measured rollback path.
The complete copy, catch-up and verification must fit the user's one-day limit.

Physical execution also needs registered-target tablespaces, historical index
placement, raw-mapping tiering, recoverable movement, multi-drive archive
resolution and rotated local recovery copies. Adding another drive must direct
future allocations without reinterpreting prior locations. The global identity
registry remains a growing relation; its size and placement cannot be omitted
from capacity planning.

Hardware measurements rejected keeping all active headers and indexes on HDD:
the tested write workload fell behind. A compact identity registry on HDD
sustained the synthetic workload, but mixed recent-query latency exceeded the
initial relative performance target. Full application, cold-history, boundary,
larger-history and interruption tests remain required. These measurements do
not establish a two-year capacity guarantee.

See [storage validation](storage-tiering-validation.md) and
[storage management](../architecture/persistence/STORAGE_MANAGEMENT.md).

The retained concurrent operational-index SQL helpers for v1 require an
unpartitioned header table. Do not run them on the v2 partitioned parent.
A v2 online index repair requires building matching child indexes, validating
them and attaching them to the parent; its reviewed operator helper is still
pending. Startup never treats an invalid parent index as ready.

Exact-ID reads resolve registry dates before fetching headers. A disposable
PostgreSQL execution-plan regression proves unrelated header partitions do not
execute for that path. The initial single-query ARRAY subquery returned correct
rows but did not prune those partitions; the implementation therefore uses two
bounded statements.

This proof does not cover broad series/time queries. Observation time cannot
stand in for storage_day: a late import or correction can belong to a newer
storage partition. Test those queries against the full intended partition
horizon, including planning time and cold cache behavior. If they probe too many
historical indexes, add a transactionally maintained, conservative series/day
range directory and verify late arrivals before enabling physical tiering.
Never prune merely by assuming observation day equals storage day.

## Inspect an existing v1 source without changing it

The operator command is:

```bash
python -m scripts.db.inspect_fact_header_cutover_v2
```

It uses only an explicitly supplied PG_DSN and never loads dotenv or application
settings. Inspection runs in a read-only repeatable-read transaction, with a
two-second lock timeout and a per-statement timeout of 15 seconds by default
(configurable from 1 to 60 seconds). Catalog inventories stop at 4,096 entries.
It does not scan all Fact rows, acquire a writer fence, install capture, copy
data, or create a migration certificate.

The JSON report lists the source columns, constraints, indexes, guard signatures,
incoming foreign keys and dependent views. It identifies the expected payload
and archive consumers, flags unknown dependencies, and checks that the source
has the index needed for bounded copy pages. It reports any existing v2 identity
objects and retained legacy source. Row counts are planner estimates; relation
file sizes exclude partition children and are not a free-space or duration
guarantee. The catalog fingerprint helps compare inspections but does not
authorize execution or replace verification under the eventual writer fence.

Every report is explicitly inspection_only with migration_ready=false. Exact
source-contract admission, capacity, capture, copy verification, physical
placement, performance and rollback remain separate required gates. This tool
makes those future operator decisions concrete; it is not the missing migration
executor.

## Range-directory candidate under qualification

The local two-year baseline on 730 partitions and 731 synthetic rows preserved
late corrections and frozen cutoffs, but executed all 730 header partitions.
Its measured planning time was 448.941 ms and execution time 17.626 ms. Those
warmed, small-row local measurements are evidence of fan-out, not HDD timings.

fact_series_day_schema.py contains an explicit candidate installer for a
per-series, per-storage-day observation range directory and a stable range
reader. It is not wired into clean bootstrap, application range selection or
the existing-server migration. The application still uses its original range
query until candidate qualification and integration are complete.

The candidate insertion trigger expands bounds in the same transaction as the
header insert, including direct child-partition inserts. Bounds cannot narrow,
change identity, be deleted or truncated. Admission checks function/trigger
definitions, stable reader snapshot semantics and child trigger coverage.
Concurrent inserts, rollback and a new-day commit between internal reads must
be tested before adopting it. The directory is not a substitute for global
Fact identity, known-at filtering, latest-revision selection or payload checks.

The [horizon experiment](storage-tiering-validation.md#two-year-partition-horizon-check)
compares alternative query shapes. Its stable-reader function quotes only
server-produced typed dates into the narrowed query; request values remain
bound parameters. Snapshot behavior follows PostgreSQL's
[STABLE function contract](https://www.postgresql.org/docs/15/xfunc-volatility.html).
A missing directory requires explicit repair or migration, never an automatic
empty replacement. Directory population/verification and bytes must be included
in the eventual migration and SSD budget.

The row-trigger directory update adds write work. Its ingestion overhead,
representative historical payload hydration, large result sets, cold cache
behavior and full two-year query timings remain qualification gates. Passing a
small fixture does not establish acceptable production latency or throughput.

The final 32-partition smoke run passed eight selection and directory-guard
checks, including a new-day commit while an existing read was blocked. The
unpruned query planned in 2.326 ms and executed in 0.469 ms. The stable reader
planned in 0.211 ms and executed in 0.602 ms, including its internal planning;
its nested partition count is not exposed by the outer EXPLAIN. The array
candidate still executed 32 partitions; the lateral candidate executed two
but retained more planning work. These are single warmed observations, not
percentiles or production acceptance. The full 730-day candidate comparison
must still complete before selecting and integrating the runtime path.
