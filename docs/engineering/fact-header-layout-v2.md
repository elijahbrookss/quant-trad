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

It adds no operator command, startup action or cutover permission. The preserving-copy preparation now applies the known-v1 source-contract
admission described below. The final operator must repeat that admission
under its writer fence. An intact retry reuses the queue; changed identity, replaced
queue, altered function or disabled trigger blocks resumption. The capture helper itself does not copy headers or drain its queue. The
separate internal shadow-copy stage below owns that work. Neither helper rewires
active dependencies or issues a ready certificate.

The capture stage passed isolated PostgreSQL qualification: original rows remain
unchanged, late IDs are tracked, caller and backend failures roll back source
and tracking together, ordinary restricted writers need no private-queue
access, and broken capture cannot be silently reused. Competing writers or
migration actors cause an explicit preparation retry. This establishes capture
behavior only; it is not a rehearsal of copying or switching the installed
database. The normal backend checks also passed.


## Preserving shadow-copy stage

The internal scripts/db/fact_header_v2_copy.py primitive copies a tiered-v1
source into private v2 header partitions, global identities and a series/day
directory. The target reuses the clean ORM model definitions. Existing payload
bytes and archive objects are not recopied by this stage.

Preparation records a bounded baseline endpoint while installing transactional
insert capture. Each page verifies every persisted header field and global
identity before advancing its cursor or retiring pending IDs. A page failure
rolls back target rows, directory bounds, cursor and queue retirement together.
After the baseline, pending IDs are copied using the source's global ID index.
This also handles a lower sequence that commits after a higher sequence was
already copied; a sequence watermark is not treated as a complete commit set.

Retry refuses changed source columns, a missing copy index, changed target
definitions, replaced capture, incompatible recorded partitions and conflicting
target content. Known-source schema/guard admission runs before preparation commits and on
preparation retry. A complete final verification pass and source re-admission
under the final writer fence still belong to the operator orchestrator.

There is no CLI, active-table rename, ready certificate, placement assignment or
startup wiring. The caught-up report covers only the currently visible queue;
an uncommitted source writer may still add an entry later. It is not permission
to switch. Final cutover requires the coordinated writer fence, queue drain,
validation and explicit FK/view/function/trigger handoff. The original source
is retained and remains authoritative throughout this stage.

The disposable preserving-copy rehearsal passed against a source header layout
pinned to the installed v1 revision. It preserved every header field, existing
hot payloads, archived bytes and frozen dataset metadata; copied a lower
sequence that committed after a higher one; recovered from backend termination;
and refused source-index, target-definition and target-content drift. This
proves private-copy correctness and recovery, not a final switch or production
duration. The target used default disposable storage, so these results do not
qualify HDD throughput or the one-day migration limit.

The queued-write failure rehearsal also passed: a caught failure after row
verification restored the pending ID, removed partial target changes and left
the source intact. Retrying copied the record successfully. The private copy
does not expose a separate acknowledgement path that could discard an
unverified pending record.

## Disposable schema-handoff rehearsal

A tiny owned-database fixture now rehearses the dependency handoff after the
preserving copy. It retains the original header table and v1 certificate,
rewires payload/archive references to global identity, installs the normal v2
guards and view, preserves the existing commit sequence, and closes the
retained source against further inserts.

The application then accepts the schema on a fresh startup, returns identical
recent/historical and frozen results, and continues collection and corrections.
Correcting an archived observation leaves the pre-migration frozen result
unchanged. Terminating the PostgreSQL backend during the schema switch restores
the original active source, certificate and references; retry succeeds.

This switch is deliberately test-only and refuses anything except an owned
disposable database with at most 1,024 headers. It is not a production migration
command. It takes fixture locks and validates populated foreign keys directly;
that does not establish a short cutover at production scale. Full source
admission, bounded final verification, measured lock/capacity limits, physical
placement and rollback after resumed production writes remain release blockers.


## Known-source admission before preparation

The internal fact_header_v2_admission helper admits the installed tiered-v1
header contract against the trusted shadow schema. It uses frozen source guard
bodies from the installed code revision, rather than treating any existing
trigger or view name as sufficient. It checks payload validation/immutability
on the parent and attached children as well as the source-header guards.

Only the known payload/archive references and hot projection are accepted.
Unexpected direct or projection-dependent views/functions, changed foreign
keys, conflicting active v2 objects, changed defaults/indexes and incompatible
ownership or row policies refuse preparation. They require inspection, not
automatic deletion or rewriting. The source commit sequence must remain
standalone. Every check is a catalog read; no source repair is performed.

The check runs inside the preparation savepoint after constructing the empty
trusted target, so refusal cannot commit partial capture or shadow state. An
intact retry re-admits the current source. Source admission does not prove data
copy completion, free space, physical placement, performance, migration
duration or safe final cutover. The final operator must repeat admission while
holding the reviewed writer/dependency fence.

The combined disposable rehearsal passed with admission enabled: known v1
preparation and retry, resumable copying, recent/historical/frozen reads after
the fixture switch, and interruption recovery. Altered guards or dependencies
were refused without changing source rows or leaving partial preparation.
The normal backend and documentation checks passed. This evidence remains
local and does not qualify production cutover duration or physical placement.


## Moving reference validation before the final pause

A disposable PostgreSQL 15 capability rehearsal established that validated
foreign keys on the existing payload children can be adopted by the parent
without replacing their constraint identities. Collection transactions remained
able to insert while the child validation transaction was open. An unvalidated
child with a missing target record blocked parent attachment and retained the
original reference. This supports staging validation ahead of the final writer
fence; it is not a measured production cutover duration.

This matches PostgreSQL 15's [constraint attachment implementation](https://github.com/postgres/postgres/blob/REL_15_STABLE/src/backend/commands/tablecmds.c)
and its [ALTER TABLE restrictions](https://www.postgresql.org/docs/15/sql-altertable.html).
Direct NOT VALID foreign keys on a partitioned parent are unsupported; the
rehearsal uses ordinary child constraints, then their validated attachment.

After baseline copying, an explicit bounded catch-up step installs a fixed
identity capture trigger under a NOWAIT source writer fence. A busy writer or
backlog larger than one bounded page refuses activation. The catch-up, trigger
installation and recorded phase commit or roll back together. Installing this
mirror before backfill was rejected by a disposable rehearsal: an unfinished
source insert then held an identity-table lock needed to create a private
header partition. The initial queue-only phase retains its late-commit behavior.
Each subsequent original-source insert creates and verifies its new global
identity in the same transaction as the source and pending-copy entry. This
allows reference validation to proceed before all corresponding header copies
have caught up. The original source remains authoritative; no new identity
alone certifies copy completion.

The mirror binds to the original source and private target relation identities,
uses a fixed definer function, and requires no private-schema permission for
collectors. Retry checks its exact function and enabled trigger. A conflict
aborts the source insert; source rollback also rolls back identity and queue.
No automatic repair or mirror replacement occurs on retry. All existing source identities require baseline completion and the guarded
activation step before staging new foreign keys. New-day partition creation
during later catch-up still needs the operator's bounded statement budget.
Production FK orchestration, final fencing and verified placement remain
unfinished; the capability test is not an operator command.

The corrected post-baseline activation passed the disposable late-commit,
bounded catch-up, failed activation, terminated collection and restricted-writer
rehearsals. Next-day copying respected a caller timeout while an unfinished
collector held the identity table, retained the committed source/identity/queue,
and succeeded after that writer committed. Enabling identity capture before the
fixture switch preserved normal startup, recent/history/frozen reads and new
collection; terminating the switch restored the original layout and allowed
retry. These remain tiny fixtures, not a production operator or duration proof.


## Known-reference prevalidation

The internal fact_header_v2_references module stages the existing payload-child
and two archive-reference families against the private global identities after
identity capture is active. It does not accept arbitrary tables or drop an
original foreign key. Each preparation and validation is a separate caller
transaction. Preparation takes a short source writer fence; validation uses
PostgreSQL's ordinary online constraint validation. Both obey explicit statement
budgets and a short lock timeout, preserving tighter caller settings.

Progress is the real constraint state, not a second copy of it in a journal.
Retry verifies its target, columns, actions, validation flag, parent attachment
and enforcement triggers. The parent attachment requires complete prevalidated
ordinary references and verifies that every existing child constraint OID was
reused. New payload partitions inherit the parent reference after attachment.

This prepares the known references for the eventual final writer fence and
schema handoff. It never switches active tables, removes source FKs, writes a
ready certificate or claims migration_ready. Physical placement, complete target
verification, capacity and total-duration limits, and post-resume recovery still
belong to the production operator. The disposable handoff helper has an explicit
prevalidated path for rehearsing that final reference rename without rescanning
the copied fixture data through newly added foreign keys.


Copy and reference steps require READ COMMITTED transactions. A snapshot taken
before the source writer fence could miss a record committed before capture was
installed; rejecting older fixed snapshots prevents that gap before preparation
changes anything. The source remains authoritative when this check refuses.


The known-reference rehearsal passed on actual QT payload and archive relations:
collection committed while validation held its locks, validated child constraints
were reused, and incomplete identities or disabled enforcement were refused.
The prevalidated fixture switch preserved populated archive relationships,
recent/historical queries, frozen results and archive bytes after backend
termination, rollback and retry. A separate old-snapshot rehearsal refused
preparation before changing the source and included the concurrent committed
record when retried with READ COMMITTED. These results establish correctness for
small disposable data, not full-volume timing or production deployment readiness.
