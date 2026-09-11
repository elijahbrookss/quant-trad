# QT unattended readiness

## Scope and completion evidence

Keep the existing 17 collectors reliable without daily intervention, preserve
the agreed research history, and demonstrate a measured two-year storage plan.
No new providers or product features are included. The operator explicitly
removed a seven-day elapsed-time gate: completion depends on evidence from
targeted tests, not waiting for a week.

This is a rollout worksheet, not evidence that production is already ready.
The September 11, 2026 review found execution disabled and archives still on
NVMe. Do not infer deployment or retention activation from merged application
code.

## Confirmed baseline

Measurements were taken on September 11 unless a window is given.

| Item | Observed evidence | Consequence |
| --- | --- | --- |
| Collectors | 17 currently healthy; earlier TLS/DNS failures and a trade finalizer exception | Present health does not erase historical gaps |
| Power incident | Host stayed up; eight streams had heartbeat gaps around September 9 13:55 UTC, with TLS and later DNS errors | UPS protected compute; exact network failure cause remains unconfirmed |
| Database placement | PostgreSQL Docker volume on the 2 TB NVMe | Attached HDD is not automatically used |
| Archive placement | Market-structure root also on NVMe | Archiving to the current root does not move bytes off that drive |
| Large disk | 16,000,900,661,248-byte Toshiba disk, no mount/filesystem reported | Verify serial/signatures and intended contents before provisioning |
| Lifecycle | Scheduler enabled; raw and canonical execution flags false; planner times out | Dry-run scheduling does not clean up data |
| Retention planner | DISTINCT family scan across a full storage day exceeds its five-second statement limit | Requires an indexed bounded query, not just a longer timeout |
| Canonical eligibility | 30 hot days; oldest observed partition August 21 | No partition was age-eligible on September 11 |
| Database size | About 423 GiB: 141.6 source preservation, 120.3 hot payloads, 106.8 headers, 47.9 raw mappings, 6.1 other | Payload retention does not reclaim every component |
| Database growth | September 8–11: 20.636 GiB/day total; 8.850 hot, 8.127 headers, 3.440 mappings, 0.219 other | Permanent records need their own placement/capacity plan |
| Fact arrivals | Approximately 4.71 million/day in the same window | Executor defaults cannot keep up |
| Raw archives | Approximately 0.49–0.53 GiB/day compressed on September 8–10 | Measure canonical compression independently |
| Backups found | Two September 5–6 dumps on the same NVMe; no routine schedule found in inspected user/system timers | These are not independently recoverable off-server backups |

A straight-line two-year extrapolation of the sampled database growth is
approximately 14.7 TiB additional allocation before retention. Headers alone
would add about 5.8 TiB and raw mappings about 2.45 TiB. These are short-window
scenarios, not predictions; compression, workload mix, indexes, bloat, WAL,
snapshots, and backup copies must be measured separately. The attached disk
offers about 14.55 TiB before filesystem overhead, not 16 TiB.

The one-time source preservation copy is not recurring growth. Keep it until a
verified independent recovery copy and an explicit retirement decision exist.

## Application fixes and prerequisite

The readiness branch addresses projection-invalidated trade coverage without a
transport restart, removes full detail/Fact hydration from event and gap
catalogs, replaces retention family aggregation with bounded indexed seeks,
and selects recent Facts from per-series indexed acceptance order while excluding
superseded and invalidated observations.

Before deploying the retention query change, apply
`scripts/db/manual_add_fact_storage_family_index_v1.sql` and
`scripts/db/manual_add_fact_series_accepted_index_v1.sql` using psql with
`ON_ERROR_STOP=1`, outside a transaction. Review free space and WAL headroom
first. Concurrent index creation permits writes but adds I/O. Verify successful
completion and rerun a read-only plan under the existing statement/run budgets.
An interrupted invalid index must be investigated; the script does not replace
it automatically. The additive index is compatible with the previous release,
so application rollback does not require dropping it.

## Rollout sequence

1. **Choose and prove an independent recovery destination.** Use another
   machine, NAS, or cloud storage. Confirm available capacity, access, encryption,
   retention, and recovery time. A second disk inside this server is an archive
   tier, not an independent backup. Preserve the database and every referenced
   raw/canonical archive object; a database dump alone cannot restore cold Facts.
   Run a restore in isolation with collectors and external orders disabled, and
   verify hot and cold reads, frozen dataset identities, and archive checksums.
2. **Provision the archive disk with operator access.** Record disk serial,
   existing signatures, and the selected filesystem before any destructive
   action. Mount persistently by UUID; configure the application archive root
   and expected UUID. Quiesce writers for the final copy/switch, compare complete
   manifests/checksums, validate ownership, and retain the original copy until
   recovery checks pass. Test missing/read-only/wrong-UUID mount rejection.
3. **Deploy verified code and measure retention.** Apply the additive index
   first. Rehearse deployment and recovery, then check the scoped history routes,
   finalizer recovery, and dry-run plans. Benchmark archive stage, page verify,
   partition verify, and reclaim on representative isolated data, including
   dependency-heavy Level 2 history. Do not delete production evidence merely to
   benchmark.
4. **Resolve permanent-record capacity.** Measure indexes, headers, mappings,
   WAL, bloat, hot payloads, archive compression, staging overlap, and one-time
   preserved source data. Evaluate moving database tables/indexes to supported
   storage or providing adequate additional fast storage; payload archiving
   alone cannot fit two years of sampled header growth on this NVMe. Select the
   layout from measured read/write performance and the chosen history policy.
5. **Enable measured archival throughput.** The defaults provide at most
   960,000 published rows/day before charging verification steps, versus about
   4.71 million arriving rows/day. Tune interval, page size, steps, and time
   budgets from the benchmark. Demonstrate sustained catch-up capacity with
   workload headroom and successful resume after interruption. Retain all mount,
   dependency, checksum, and frozen-evidence guards.
6. **Prove unattended operation.** Schedule daily independent backups; verify
   an actual restore. Test collector reconnect and retained-spool recovery,
   archive interruption/retry, safe deployment rollback, disk-mount failure,
   capacity alerts, and backup-failure alerts. Confirm a test alert reaches the
   operator and subsequently clears. Record results and remaining limitations;
   no seven-day wait is required.

## Decisions still required

- Independent backup destination and access; nothing has been copied there.
- Two-year history policy: all raw evidence, or research-ready history with an
  explicitly agreed raw retention period. Do not silently shorten retention.
- Administrative access for disk provisioning/mounting. Noninteractive sudo
  was unavailable during the review.
- A measured permanent-record storage layout and headroom threshold before
  claiming a two-year capacity guarantee.

## Acceptance record

For each rollout step record the exact application revision, configuration,
measurement window, byte/row counts, elapsed time, recovery outcome, and any
operator action. A test passing on small fixtures proves that path's behavior,
not its production throughput. Daily backups must reference a successful
isolated restore, and alerts must reference a delivered test notification.
