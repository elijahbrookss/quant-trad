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
