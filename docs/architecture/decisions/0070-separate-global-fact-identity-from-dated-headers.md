---
component: adr-dated-fact-headers
subsystem: persistence
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - storage
  - postgres
  - explicit-migration
code_paths:
  - portal/backend/db/fact_identity_schema.py
  - portal/backend/db/market_data_models.py
  - portal/backend/db/market_storage_models.py
  - portal/backend/db/fact_storage_schema.py
  - scripts/db/fact_header_v2_capture.py
  - scripts/db/fact_header_v2_copy.py
  - scripts/db/fact_header_v2_admission.py
  - scripts/db/fact_header_v2_references.py
  - scripts/db/fact_header_v2_placement.py
  - scripts/db/raw_mapping_v2_copy.py
  - scripts/db/archive_reference_v2_placement.py
  - scripts/db/archive_root_v2_copy.py
  - scripts/db/fact_header_v2_handoff.py
---
# ADR 0070: Separate Global Fact Identity from Dated Headers

Accepted on 2026-09-16 for the clean-schema integrity boundary. Physical
placement and migration qualification remain incomplete.

Payload archiving alone leaves detailed Fact headers and their lookup indexes
growing in the active database. Daily header partitions make that history
independently placeable, but including a date in every unique key would weaken
global duplicate protection. The same Fact could otherwise appear twice on
different days.

Keep a compact immutable global registry with a primary key on Fact ID and a
unique key on series, observation key and revision. Store the ID's immutable
storage day there. Detailed dated headers reference the ID and day together,
while ID-only payload and archive relationships reference global identity.
A header insertion registers identity in the same transaction, and a deferred
guard rejects identity without its matching header. Header/payload atomicity
remains enforced by the existing deferred payload guard.

This keeps storage placement out of Fact identity and market clocks. The
registry and detail partitions remain behind PG_DSN; they are not independent
databases or application-level sources of truth. Known-at selection, exact
frozen revisions, material hashes and correction semantics remain unchanged.

Runtime provisions new empty days under an advisory transaction lock and
records them in an immutable catalogue. A recorded missing partition is a
failure requiring inspection, not a request to recreate an empty table.
Startup validates the layout and refuses older or partial schemas without
performing an implicit migration.

A global registry still grows and requires indexes. Its bytes and write cost
must be measured and included in the whole-system storage forecast. Dated
headers do not themselves choose a drive. The initial implementation uses the
default tablespace pending registered-target placement and recovery tests.

The [v2 layout notes](../../engineering/fact-header-layout-v2.md) distinguish
implemented clean-schema support from the remaining operator cutover. This
decision extends [ADR 0069](0069-bind-storage-objects-to-registered-targets.md).

## Capture during the preserving upgrade

The one-time tiered-v1 upgrade uses a private pending-ID queue so writes
committed during backfill cannot fall behind the copy cursor. The internal
capture primitive in scripts/db/fact_header_v2_capture.py installs the queue
and source INSERT trigger in one caller-owned transaction and savepoint.
Every queued ID commits or rolls back with its original Fact. It does not
filter by commit sequence or a wall-clock watermark.

Preparation requires a brief writer-free boundary: a busy source is refused
immediately for coordinated retry, while ordinary readers remain permitted.
The source stays immutable, including against truncation, during capture.
The queue is logged PostgreSQL storage under the same PG_DSN. A narrowly scoped
trigger function writes it without granting collectors access to migration
internals. Retry verifies the original database, source and queue identities,
function bodies and enabled triggers instead of silently resetting progress.

This is a capture primitive, not full source admission or an executable
cutover. No CLI/runtime path calls it. The preserving orchestrator still needs
complete v1 schema admission, bounded copy and catch-up, capacity and one-day
rehearsal, correct FK/view handoff, rollback and final verification before
activating v2. Capturing IDs alone never makes migration_ready true.


The internal shadow-copy stage builds the existing v2 models in the same private
migration schema. It reads bounded pages using the installed source's
(storage_day, market_commit_seq, id) index, preserves every persisted header
field, builds global identity and conservative series/day bounds, and commits
the verified page and cursor together. New IDs drain from the transactional
queue without a sequence watermark. An ID leaves that queue only in the
transaction that verified its matching target header and identity.

The original header table, hot payloads, archive objects and frozen dataset
records remain authoritative and untouched. A private copy is not an admitted
runtime layout. No v2 ready certificate is written. Source admission, physical
placement, complete final verification and dependency handoff are separate
release requirements; the private target is never selected by application
queries. This primitive adds no alternate DSN, runtime writer or placement UI.


Before preparation commits, the known-v1 source admission compares persisted
columns/defaults/collations, checks, references and secondary indexes with the
trusted empty shadow model. It separately requires the original global ID and
revision keys, frozen v1 guard bodies, the source and payload-partition triggers,
the existing hot projection, known incoming references and the standalone commit
sequence. Unexpected active v2 objects or dependents are refused, including
views/functions attached through the hot projection.

Admission performs no repair. A refused preparation rolls back both capture and
shadow DDL, while a retry rechecks the current source. This is source-contract
admission only; the eventual operator must repeat it under the final writer
fence and still qualify physical placement, capacity, timing and rollback.

After baseline copying, a bounded catch-up step takes the source writer fence
and enables a fixed trigger that mirrors new global identities in the source
transaction. A busy writer or excess backlog refuses activation; a failed
activation rolls back catch-up, trigger and phase together. Enabling this
mirror at initial preparation caused private-partition DDL to wait on unfinished
source writers in the disposable rehearsal, so the initial phase remains
queue-only. This supports validating old
payload/archive references against the new identity registry while collection
continues. The original source is still the authoritative header during this
stage; an identity whose private header is still queued is not an active v2
record. The normal identity-to-header guard is installed only at the reviewed
handoff after draining and verifying the copy. Source, identity and pending ID
commit or roll back together. Retry refuses a changed mirror instead of repairing
it. This does not introduce a second runtime writer or a new storage policy.


The internal reference stage prepares and validates only the known payload
children and archive references against the mirrored private identities.
Original source FKs remain in force. Preparation briefly fences writers;
validation is separate and permits normal collection. Validated child
constraints are attached to a parent constraint without replacement. Changed
definitions, disabled enforcement or incomplete coverage refuse advancement.
These steps do not remove original references, activate v2 or qualify capacity
and final cutover duration.


The internal preserving copy can bind a fixed recent SSD and history HDD before
creating its shadow tables. It reuses the storage boundary's PostgreSQL process
and namespace checks. New global identities and their indexes go directly to the
verified history tablespace; dated headers use that tablespace before the fixed
cutoff and pg_default afterward. The current database, small routing catalogs and
temporary capture state remain on the verified recent filesystem. Copy progress
records the binding and refuses a changed destination rather than relocating
already copied data. This is not a general placement API or runtime policy.


The same fixed migration now has a bounded copy for the installed immutable
raw archive record lookup. This table is a measured source of SSD growth; it
cannot be left behind when the historical headers move. Its replacement and
all indexes are created directly on the already bound history HDD. The source,
small cursor and transactional composite-key queue remain on the recent SSD
during the copy.

Preparation admits the known model, original immutable guard, permissions,
indexes and foreign-key enforcement, and refuses unexpected dependents or
replication arrangements. Each page rechecks bound schema, trigger/function
definitions and physical placement. Every field is compared before its exact
(raw_record_id, manifest_id) queue key is retired. Cursor, copied rows and queue
retirement commit together. Source writes capture keys without waiting for an
HDD mirror. Changes and truncation of the original immutable table are refused.

This adds no generic table mover or runtime writer. It requires the prepared
header migration and does not switch the authoritative raw lookup, delete its
source or emit readiness. The production operator must still qualify final
verification, dependency handoff, capacity, throughput and the full one-day
migration budget. The optional raw lookup switch in the tiny disposable fixture
is test evidence only, never an operator entrypoint.


## Fixed migration timing boundary — 2026-09-18

Capture already records a durable preparation timestamp. The preserving header,
identity-mirror, reference and raw-lookup steps now use that same timestamp as
the start of their 24-hour attempt window. A reconnect, another page or repeated
preparation cannot reset it. An expired or future-dated start refuses further
migration work without rewriting the capture record, source or prior progress.
Read-only capture inspection still exposes the original start and deadline.

Each step also has one cumulative time budget. Before a SQL statement it reduces
PostgreSQL statement_timeout to the remaining allowance, preserving a stricter
caller or enclosing-step timeout. The existing savepoint owns partial DDL, copy
rows and cursor changes. Timeout rolls that step back while earlier committed
progress and source collection remain intact; transaction cleanup is never
blocked by the expired guard. The connection listener is removed before the
step leaves, and caller settings are restored or rolled back with the savepoint.
Timeout configuration uses the normal database connection path, with reentry
suppression, so a dropped connection is invalidated before it can return to the
pool. The guard must not bypass the existing disconnect and rollback behavior.

This uses the existing migration boundary and clock, not another journal,
scheduler or generic migration framework. Python filesystem probes retain their
own bounded checks, and an over-budget step cannot return success after such a
probe. The caller still owns commit. A future final-switch operator must check
the same deadline through its commit boundary and qualify physical capacity,
copy/catch-up throughput, final verification and recovery. These guards enforce
admission and per-step SQL time; they do not prove completion within one day.
No runtime startup migration or live cutover is introduced.


## Final copy verification boundary — 2026-09-18

The fixed migration's existing header and raw-lookup modules now expose an
internal verification context for the eventual final handoff. It takes nonwaiting
writer fences on the admitted source, reference owners and private target
relations while permitting ordinary queries. The subsequent rename phase must
acquire its own exclusive fence without waiting. Verification then repeats copy/source/placement admission before comparing bounded
ordered pages. Header fields, global identity fields and raw lookup fields must
match exactly in both directions. Equal counts alone do not qualify. Every
header must be covered by conservative series/day routing bounds, and registered
partitions must match their attached relations and physical destinations.

Verification and the caller's handoff share the existing cumulative step budget
and original one-day clock. A failure rolls back the verification savepoint and
its newly acquired locks, preserving outer work and allowing source collection.
Successful verification keeps transaction locks until caller commit or rollback;
its report is not a reusable readiness token. The raw-lookup context requires
the header fence on the same database connection.

The tiny disposable handoff now exercises these checks instead of whole-table
EXCEPT comparisons. It retains its 1,024-row limit. No operator command, ready
certificate, data deletion, new storage policy or generic mover is introduced.
Full exact verification still reads every source and target record while writers
are fenced (ordinary readers continue); its duration must be measured. Bounded memory and timeout do not
establish acceptable downtime, commit supervision, post-resume recovery or the
complete migration's one-day qualification.


## Preserving archive-reference placement — 2026-09-18

The fixed upgrade also needs its material-alias and canonical-dependency
catalogs and indexes on the history HDD. An internal operator step moves one of
these two known ordinary tables in place, retaining its OID, columns, constraints,
index identities/definitions and triggers. PostgreSQL owns atomic file relocation
and rollback; no logical rows are copied or deleted by this step. Only that
catalog takes a nonwaiting exclusive fence. Source header collection remains
permitted, and the v1 layout stays authoritative.

The operation uses the copy's existing fixed physical binding, migration clock
and storage ownership lock. It reuses physical WAL/temp observation, the existing
resource-limit format and headroom calculation, and the movement watcher through
commit/rollback. The caller must supply qualified WAL, temporary, growth and
maintenance allowances. Existing durable claims and policy reserve remain
protected; future source frees are never credited. No new reservation ledger,
storage policy, scheduler, placement UI or generic table mover is introduced.

All heap/index files must be wholly on the admitted recent filesystem or wholly
on the bound history filesystem. Mixed, unknown or unavailable placement refuses
movement. Changed registered identities, inactive targets, changed saved policy,
insufficient capacity, timeout and cancellation refuse work or roll back before
commit. An error around commit may have an unknown outcome; inspect verified
placement instead of assuming rollback. A committed already-history table is verified and acknowledged without copying
again. Read-only placement inspection remains available after attempt expiry;
reconciliation does not reset the original one-day clock.

The watcher bounds observed net filesystem consumption, not each producer's
WAL/temp attribution or instantaneous allocation. Real allowances, catalog
sizes, exclusive-lock duration and complete migration time still need rehearsal.
This step is not the final schema switch, archive-file migration or complete
production operator. Reusing the retained v1 source after new v2 writes resume
is still not a valid rollback strategy.

## Preserving archive-file copy — 2026-09-18

The fixed upgrade can copy bounded pages from the existing raw archive,
checkpoint and canonical archive catalogs onto the bound history HDD. Portable
object keys and checksums remain unchanged. The existing immutable writer
publishes each file without replacing a conflicting key, verifies its bytes and
durability, and reuses verified completed objects on retry. Optional budget
checks now run between checksum and copy chunks; default publication is unchanged.

Each page owns the existing migration/storage locks and shared archive-expiry
fence, binds both roots to the admitted SSD/HDD filesystems, and uses the original
one-day attempt clock and existing resource watcher. Policy reserve, existing
claims and declared WAL/temp/growth/maintenance allowances remain protected.
A wrong root, symlink, corrupt object, changed root, insufficient headroom,
cancellation or elapsed budget refuses work. Source records and files remain
untouched. Ordinary source collection is allowed during copying.

A failed page can leave valid durable destination objects; rolling back its SQL
transaction does not erase those files. Retry the unacknowledged page and let
immutable publication verify/reuse them. A cursor is only page progress: concurrent
publication can add IDs before it. It is never a completeness or activation
certificate. The final operator still needs fenced inventory reconciliation,
archive-root cutover and the full preserving migration/recovery procedure.
No runtime configuration, manifest location, new scheduler or generic placement
API is changed by this internal step. Filesystem watching bounds observed
consumption, not instantaneous allocation or per-producer IO attribution.

The internal final inventory context takes nonwaiting write fences on all three
manifest catalogs and the shared expiry fence before scanning from their starts.
It verifies every required destination object's size, complete checksum and stable
file identity, with bounded pages, object/byte limits and the same migration clock,
physical binding, declared resource budget and cancellation watcher. Copy cursors
cannot substitute for this complete inventory. Missing or changed destination
bytes, a busy publisher or an exceeded bound refuse admission; the verification
savepoint releases its own locks without losing earlier caller work.

Ordinary catalog reads continue while manifest writes are fenced. The caller can
perform its admitted database handoff inside the verification context. Successful
transaction locks remain until caller commit or rollback. This is still an
internal prerequisite: the caller owns commit supervision and must stop/drain
in-flight file publishers before changing the active root. Files uploaded but
not yet cataloged cannot be certified by a database fence. No root activation or
durable/reusable readiness token is issued.

The complete file hash scan currently occurs while catalog writers are fenced.
Its duration must be measured alongside the final header/lookup scans before
claiming a short pause or a complete migration within one day. The bounded
implementation is not evidence that a full-size verification fits that budget.


The fixed internal database handoff now owns the complete header, global identity,
raw lookup and archive-inventory verification contexts and requires the two
archive reference catalogs and indexes already on HDD. It shares only the
admitted schema-switch DDL with the existing tiny fixture; that fixture retains
its isolation and size guards. The internal handoff adds an outer resource and
original-attempt deadline watcher through actual commit/rollback. Source tables
remain closed to inserts in the retained schema.

The existing v2 layout certificate records the exact source/target relation
identities, physical binding, archive roots, policy and verified inventory in
the same transaction as the switch. A lost commit reply is resolved by bounded
read-only inspection of that evidence and the actual layout, never by blindly
repeating the switch or starting the old image. Inspection first acquires the
existing migration ownership fence without waiting. If another attempt still
holds it, the outcome is pending: an invisible uncommitted certificate cannot
be mistaken for a completed rollback. Inspection works after the
original migration deadline, and does not rehash archives or claim ongoing
integrity. A mismatched relation, root or retained-source guard refuses the
reported committed outcome.

This remains a database boundary, not a complete deployment operator: the
caller must drain publishers and hold them stopped, activate the matching
runtime/root, and coordinate recovery for subsequent writes. No service,
configuration, device, collection state or storage policy is changed here.
A committed database result explicitly does not authorize collection resumption.
Full-volume scan/commit duration and hardware workload qualification remain open.


Composing the verification contexts exposed recursive timeout-listener SQL: each
nested listener triggered the others while setting its own timeout and exhausted
the step budget. Migration contexts now share one connection-local listener and
apply the earliest active deadline. Nested rollback removes only its deadline;
the outermost exit removes the listener and its metadata, including disconnect
paths. This preserves cumulative/attempt/caller limits without query amplification.
