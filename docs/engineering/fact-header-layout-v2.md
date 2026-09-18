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

The exact-ID proof does not cover broad series/time queries. Those use the
series/day directory described below and need separate full-horizon, planning,
cold-cache and workload qualification before physical tiering. Observation time
cannot stand in for storage_day: a late import or correction can belong to a
newer storage partition.

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

## Snapshot-consistent range directory

Clean bootstrap now creates market.fact_header_series_days from its ORM model
and installs capture and admission guards. Normal series/time reads use the
stable read_fact_headers_in_range function before their existing source,
revision, known-at, state and payload rules. The legacy full-row offline helper
captures bounds while copying; resuming a copy without that directory is
rejected. This does not implement the existing tiered-v1 server upgrade.

Each header insert expands the series/day observation bounds in the same
transaction, including direct child-partition inserts. Bounds cannot narrow,
change identity, be deleted or truncated. Admission checks function semantics,
unfiltered triggers and child capture. A missing directory is a migration
failure, never permission to create an empty replacement over existing rows.

The function quotes server-produced typed storage dates into its internal query;
caller values remain bound parameters. PostgreSQL's
[STABLE function contract](https://www.postgresql.org/docs/15/xfunc-volatility.html)
keeps its directory and header reads on the calling statement's snapshot.
Observation day cannot substitute for storage day: a correction of an old
observation can live in today's partition.

The earlier 730-partition candidate comparison preserved latest, commit-frozen
and known-at-frozen results. With only 731 synthetic rows, the unpruned query
spent 329.118 ms planning and 18.918 ms executing; the stable reader spent
0.246 ms in outer planning and 1.123 ms executing, including internal planning.
Those single warmed observations justified integrating the query shape. They
do not prove HDD, full-volume, payload-hydration or concurrent-write performance.

The [horizon test](storage-tiering-validation.md#two-year-partition-horizon-check)
now uses the real directory model, capture triggers and runtime selector, then
compares the same filters against an unpruned reference. A partitioned recent
payload window exercises the outer hot join too. Its FunctionScan hides
the nested partition count, which must remain unavailable rather than zero.

Directory updates add write work and serialize modifications to each series/day
row. Large result materialization, cold reads, hydration and representative
concurrent ingestion still require measurement. The directory grows with
series/day combinations; series_day_directory_bytes is separately visible in
retention inventory. Its size belongs in the SSD budget and migration forecast.
No physical placement or hard SSD budget is established by this query change.

## Transactional migration capture

The internal fact_header_v2_capture helper implements only the pending-ID
capture stage. It keeps collection inserts and their migration tracking in one
transaction, including rollback or backend loss. Its private queue retains
late commits even when their sequence would fall behind a backfill cursor.
Installation is caller-transactional and uses a savepoint so a caught setup
failure cannot commit half-installed capture.

It adds no operator command, startup action or cutover permission. The source
must still receive full v1 contract admission by the preserving migration
orchestrator. An intact retry reuses the queue; changed identity, replaced
queue, altered function or disabled trigger blocks resumption. The helper
does not copy headers, drain the queue, rewire dependencies or issue a ready
certificate. Those remain implementation work before a production rehearsal.

The capture stage passed isolated PostgreSQL qualification: original rows remain
unchanged, late IDs are tracked, caller and backend failures roll back source
and tracking together, ordinary restricted writers need no private-queue
access, and broken capture cannot be silently reused. Competing writers or
migration actors cause an explicit preparation retry. This establishes capture
behavior only; it is not a rehearsal of copying or switching the installed
database. The normal backend checks also passed.
