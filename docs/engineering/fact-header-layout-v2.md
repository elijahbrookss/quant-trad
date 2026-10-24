# Dated Fact Header Layout

Canonical Fact identity is independent of physical placement. `market.fact_identities`
registers global Fact IDs and series/observation/revision identity;
`market.fact_versions` stores immutable detailed headers in daily `storage_day`
partitions. Payloads, aliases and archive dependencies refer to global identity.
A correction appends a revision without changing earlier hashes or frozen inputs.

The [persistence boundary](../architecture/persistence/PERSISTENCE_BOUNDARY.md)
owns this schema contract; [storage management](../architecture/persistence/STORAGE_MANAGEMENT.md)
owns registered targets and policy. This page explains the engineering dependencies
needed to inspect or preserve an older layout.

## Route by storage day without changing observation time

The partition catalog distinguishes a new empty day from a missing historical
partition. Startup verifies registered relations and guards; it does not replace
or adopt an unknown historical table. `market.fact_header_series_days` tracks
per-series observation bounds in each storage day transactionally with insertion.
Bounds cannot narrow or be deleted to hide history.

Range reads use `read_fact_headers_in_range` on the calling statement's snapshot.
A late import or correction may live in a newer storage partition than its
observation time. Exact-ID reads resolve registry dates before fetching headers.
Neither observation-day filtering nor a sequence watermark proves a complete
committed set. Two-year sparse partition tests reveal routing errors; they do
not establish actual HDD throughput or realistic payload hydration latency.

## Inspect the starting contract

```bash
python -m scripts.db.inspect_fact_header_cutover_v2
```

Supply only the explicit `PG_DSN` of the approved inspection target; the helper
loads no dotenv or application settings. It uses read-only repeatable-read
catalog inspection with bounded statement/lock timeouts and inventory size.
Row estimates and relation file sizes are not a complete capacity forecast.
The report is `inspection_only` with `migration_ready=false`: it cannot authorize
capture, copying or a switch.

Retained concurrent operational-index SQL for unpartitioned v1 headers must not
run on a v2 partitioned parent. A partitioned repair requires matching child
indexes, validation and attachment under its separately reviewed procedure.
Do not relax startup's invalid-index refusal.

## Preserve an existing source explicitly

The preserving implementation composes source admission, transactional insert
capture, private shadow copies, reference prevalidation and a fenced handoff.
Use its prepared operator request through the
[storage deployment procedure](server-deployment.md#storage-move-and-recovery);
internal primitives are not interchangeable standalone migration commands.

Source admission checks trusted guards, columns, indexes, reference dependencies,
views and identities. Unknown dependencies block preparation rather than being
deleted automatically. Baseline pages compare every persisted header/global
identity field before atomically advancing progress. Transactional pending-ID
capture retains late commits behind a backfill cursor. Copy/reference stages
require READ COMMITTED so an old snapshot cannot miss committed source records.

After baseline copying, guarded identity capture permits online reference
validation before the final pause. Prevalidated child foreign keys are attached
without replacing their identities. Renaming tables alone cannot rewire foreign
keys, views, sequences, functions and guards. Complete content/placement/inventory
verification and the final writer/dependency fence remain necessary before switch.

Physical placement binds prepared target UUIDs, tablespace identity, namespace,
ownership and the fixed cutoff. Verify heaps, indexes and TOAST together;
changing the binding cannot be treated as an ordinary retry. The original source
remains authoritative until the committed switch. Afterwards, preserve new writes
and use a verified current-layout recovery point; old source rows alone are not
rollback coverage.

Measure the full copy, catch-up, verification and pause against explicit resource
and total-duration limits. See [storage qualification](storage-tiering-validation.md)
for isolation, restore ordering, failure injection and capacity accounting.
Functional fixtures and implemented helpers do not certify any deployed layout.
