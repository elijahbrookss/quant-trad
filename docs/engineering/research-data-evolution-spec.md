# Bounded Storage Migrations and Research Compatibility

**Status: implementation authorized; qualification in progress. Updated 2026-10-04.**
Decision: [ADR 0078](../architecture/decisions/0078-evolve-research-within-existing-data-boundaries.md).
This revision replaces this document's earlier H04-first implementation plan.
It does not resume a migration, clear an operational hold, deploy a release or
restart research. Historical approvals and receipts remain evidence.

## What we are solving

QT already supports collection alongside Indicator development, versioned
computations, frozen research inputs and reproducible research. Keep that
architecture. Ordinary research should rarely require a storage migration.

The immediate problem is how to address storage growth without routinely
rebuilding all history or making the period of blocked work grow with the
dataset. The original storage migration had a real capacity and placement goal.
Its full-history copy, final verification and release coupling made the operation
disruptive. This was a design and execution problem, not just poor scheduling.

Migration work can legitimately scale with the amount of affected data. The aim
is to reduce that affected scope and bound the final interruption. No design can
promise that every future canonical identity or schema change will be local.

The storage milestone is a qualified decision and implementation path for the
smallest necessary storage transition. It is not a new research engine, a general
migration framework, or completion of a particular scientific experiment.

The separately authorized research workstream adds bounded historical streaming
and safe cancellation of individual research jobs. These improve resource use and
control of existing capabilities; they do not introduce another research engine.
Both workstreams are in scope for implementation. Neither authorizes production
operations, deployment, migration execution or restarting paused automations.

## Coordinated release decision — 2026-10-04

The user selected one integrated release, replacing the earlier proposed
research-first deployment sequence. Component histories are preserved through
PRs and merge commits into `feature/storage-research-consolidation`; the user
will raise the final PR to `develop`. Work branches use only `feature/` or
`hotfix/` as specified in the [developer workflow](developer-workflow.md).
Historical branch names in immutable receipts remain historical evidence.

The six original PRs were closed by GitHub's head-branch rename and replaced
with linked component PRs. Their commits and discussions are retained:

| Original PR | Integration PR | Preserved component branch |
| --- | --- | --- |
| [#208](https://github.com/elijahbrookss/quant-trad/pull/208) | [#213](https://github.com/elijahbrookss/quant-trad/pull/213) | `feature/research-matched-origin-attribution` |
| [#210](https://github.com/elijahbrookss/quant-trad/pull/210) | [#214](https://github.com/elijahbrookss/quant-trad/pull/214) | `feature/research-first-return` |
| [#209](https://github.com/elijahbrookss/quant-trad/pull/209) | [#215](https://github.com/elijahbrookss/quant-trad/pull/215) | `hotfix/storage-source-preparation` |
| [#211](https://github.com/elijahbrookss/quant-trad/pull/211) | [#216](https://github.com/elijahbrookss/quant-trad/pull/216) | `feature/research-forward-risk` |
| [#212](https://github.com/elijahbrookss/quant-trad/pull/212) | [#217](https://github.com/elijahbrookss/quant-trad/pull/217) | `feature/research-single-attempt-jobs` |
| [#202](https://github.com/elijahbrookss/quant-trad/pull/202) | [#218](https://github.com/elijahbrookss/quant-trad/pull/218) | `feature/storage-target-management` |

The implementation at `fd6a7985` is preserved by
[PR #219](https://github.com/elijahbrookss/quant-trad/pull/219) from
`feature/research-data-evolution-spec`, supplying bounded research execution,
frozen visibility and the reconciled specification. Storage merge reconciliation
`26683956` preserves the exact `3f4c55c6` file tree; it only records the
component merge ancestry. Source branch deletion is disabled.

The integrated source includes both storage and research work. Its startup
guards require the new header/identity layout, so this chosen release waits for
the storage transition and exact combined-package qualification. This is a
release choice, not a new semantic dependency of research. The separately
qualified old-layout composition is retained as evidence, not a second planned
deployment or permanent application fork.

One release still has ordered operations. Before production execution:

1. Confirm the actual serving revision/schema and refresh the capacity horizon,
   peak index/identity/WAL/temp/recovery overlap, collection lag and pause limits
   through the existing M0 owners. Do not count retained copies as free space.
2. Qualify and admit the retained-header route: prepare the two composite keys,
   reconcile identities/references and archive bindings, and catch up concurrent
   writes using the existing owned operations. Do not revive the cancelled
   migration or silently renew its clocks.
3. Prove the selected final constraint/attachment transaction, uncertain-commit
   reconciliation and applicable database/archive recovery fit the admitted
   limits. Historical fixture success is not a production pause measurement.
4. Qualify the exact aggregate package, drain incompatible workers, perform the
   explicitly approved bounded switch/promotion, and verify collectors,
   hot/cold/frozen reads and research cancellation on the resulting layout.
5. Admit representative research workloads within their scientific and resource
   budgets before separately resuming execution. Move legacy history only if
   the measured capacity goal requires it; no cleanup is implied.

The migration remains required for this combined candidate. Retaining historical
headers avoids rebuilding their heap and search indexes, but does not eliminate
key/identity/reference work or prove SSD relief. Production capacity, final
pause/recovery admission and workload measurements remain open. Branch
consolidation does not satisfy or authorize those operations.

## Current state and why the original migration was large

Evidence comes from the prior audit and these inspected checkouts:

- **Q:** `/home/elijah/dev/quant-trad`, runtime source at
  `83c2a7f792f2d666bd666a3d252d5e6cc4c5db9e`; the earlier documentation was
  committed as `6a4203a8a40e4b550fc3f08b40c72ac44ea1ce87`.
- **S:** `/home/elijah/dev/quant-trad-codex-storage` at
  `3f4c55c6a04eb65f969043026cc870feffbbe750`.
- **R:** `/home/elijah/dev/quant-trad-codex-research` at
  `a045816960e36ed1c6096fd3872d37c3808b0b66`.

Q/S/R below identify repositories, not deployment states. The storage candidate
has implemented and tested parts; its end-to-end production cutover was not
qualified in the inspected evidence. No fresh production inspection was made.
Preserved source data, migration copies, journals, archives and recovery material
are not disposable merely because a replacement plan is preferred.

| Evidence | What it establishes |
| --- | --- |
| S `docs/architecture/decisions/0070-separate-global-fact-identity-from-dated-headers.md:29` | Payload archiving leaves headers and indexes growing. Daily header groups were chosen for independent placement, with a global identity registry to retain uniqueness. This records the selected candidate, not proof that it is the only viable solution. |
| S `docs/architecture/decisions/0073-prepare-storage-migrations-with-live-collection.md:87` | The original operator held clients through preparation and scanned historical content under final writer fences. A constrained-fixture projection exceeded 50 hours for some phases; this is not a measured production outage. |
| S `docs/architecture/decisions/0077-retain-legacy-headers-during-forward-cutover.md:36` | Old and proposed headers have identical columns and secondary search indexes, but different primary/revision keys. Retaining the old table can avoid its heap copy and search-index rebuild; identity, composite-key, reference, archive and recovery work remains. |
| S `artifacts/storage-implementation/migration-release-20260926/completion-forward-capacity-inventory-20261003.json` | Recorded source headers including indexes: 316,197,470,208 bytes; raw archive lookup relation: 130,259,869,696 bytes. Capacity admission was incomplete. Database totals include retained migration copies; do not add components to totals or credit unapproved reclamation. |
| S `portal/backend/db/session.py:1666`, `portal/backend/db/fact_storage_schema.py:208` | Combined candidate startup requires the new header/identity layout. This is a real compatibility dependency of that package. It does not make physical cutover a semantic dependency of H04. |

The header-copy stage reused existing payload bytes; the broader rollout also
included raw lookup, archive placement and recovery work. It was not simply a
research computation rewriting all market observations.

## The migration policy

Before implementing a migration, its owning storage/runbook document must state
the problem, intended benefit, affected representation and historical scope.
Classify scope as new writes only, selected historical ranges, or all history.
Justify every required copy, index build and validation scan, especially any work
inside a writer or reader fence. Use the existing owner documentation; add no
universal migration manifest, registry or DSL.

| Change | Normal treatment |
| --- | --- |
| Indicator, parameters or analytical recipe | Existing versioned computation through the canonical state engine. Leave source Facts alone. |
| New provider information or payload fields | Existing provider/Fact registration and an explicit compatible schema version where needed. Uncollected history stays missing; acquisition is separate authorized work. |
| Semantic correction or changed derivation | Append revisions or separately version derived output, with true availability and lineage. Recompute only the explicit scope; old runs retain their original inputs. |
| Physical storage format or placement | Prefer compatible readers and new-write changes. Convert only justified historical units through explicit storage work. Reads never run DDL, acquisition, conversion or backfills. |
| Canonical identity, relationship or schema meaning | Review the owning contract and ADR. Global constraints or reference changes may require broad historical work; state why and measure its impact. |

Preserve Fact IDs, revision history, exact values, provenance, known-at semantics,
Dataset bindings and archive pins. Compatibility belongs in existing storage
readers with explicit supported versions, not scattered application fallbacks.
Do not revive retired semantic stores or add another DSN or data authority.

Partitions help bound physical placement and maintenance; they do not give each
partition an unrelated relational schema. PostgreSQL partitions share their
parent's columns and have constraints on partitioned uniqueness
([PostgreSQL 15 partitioning](https://www.postgresql.org/docs/15/ddl-partitioning.html)).
Date partitioning is therefore one storage tool, not a universal answer to schema
evolution or a promise of faster queries.

## Select the smallest useful route

| Route | Recommendation and limits |
| --- | --- |
| Continue on the serving layout temporarily | Valid only with a measured capacity horizon and compatible releases. Avoid immediate migration cost, but do not describe unresolved storage growth as solved. |
| Retain existing headers; partition new writes | Preferred route to qualify for this migration, reusing S's forward-cutover work. Saves unnecessary historical header copying. Still requires identity/key/reference proof, catch-up, final attachment and applicable recovery work. |
| Rebuild all historical headers into daily groups | Not the default. Consider only if the retained-history route cannot meet a demonstrated requirement and the additional cost/interruption is explicitly justified. Failure of one route does not authorize another. |

Retaining a large legacy table on SSD does not itself reclaim SSD capacity.
Moving that retained table and its indexes to HDD still costs physical I/O and
can block reads. Define separately whether the current milestone must move it,
or whether bounded future SSD growth provides sufficient measured headroom.
Do not claim completed capacity relief from merely changing the table layout.

The small global identity registry is also not free: it grows with Fact count
and adds indexes, writes and references. Reuse existing candidate work where
qualified, but include its real cost before committing to the route.

## Storage growth plan and release acceptance

**October 4 user-selected direction: use a 14-day recent-data window on SSD,
with preserved historical data on HDD through the existing tiering route.**
This is the accepted policy choice for qualification, not an applied production
setting or deployment approval. It changes placement timing, not how long Facts
remain available. No policy, deployment, migration or retention change was
applied by this assessment. Additional HDD capacity or a measured reduction in
bytes per Fact is required to support the earlier two-year planning target under
the current growth proxies. Exact hardware sizing still needs recovery costs.

Keep intake/spool and recent headers/payloads on SSD; keep older eligible groups
and archives on HDD. Existing global lookup/identity placement remains as
accounted for below: this is not a blanket rule moving all PostgreSQL files or
all metadata at day 14. Eligibility follows the saved policy and whole-group
boundaries; verified publication and explicit maintenance precede reclamation.
Fourteen days is neither a research lookback limit nor permission to expire data.

Long-range research uses the existing bounded historical readers. The later C1
follow-up permits a bounded disposable SSD copy of requested archive objects; it
does not stage an entire selected historical range. First reads and scattered
historical lookups may cost more on HDD; a repeated request is not assumed to hit
a cache. PostgreSQL documents the distinction between
sequential and random reads and the limits of cache assumptions in its
[planner cost guidance](https://www.postgresql.org/docs/current/runtime-config-query.html#RUNTIME-CONFIG-QUERY-CONSTANTS).
This supports the tradeoff, not a QT-specific latency estimate.

The existing runtime warms up state and then advances per bar. Future paper/live
qualification must cover uncached initialization, restart and gap catch-up from
HDD, including warmup exceeding 14 days; non-HFT use alone does not prove latency
adequate. Current observe-only paper support does not establish live readiness.
Use the existing workload/release gates below to measure cold reads and collector
lag with competing archival/recovery I/O. This choice adds no new service,
universal result cache or requirement to keep a bot's full history on SSD.

### Fresh state and evidence

Read-only observations at **2026-10-04 17:35–17:41 UTC** found:

| Measurement | Observed value / meaning |
| --- | --- |
| SSD filesystem | 1.967 TB total; **814.24 GB available**. Existing migration floor 496.43 GB leaves **317.81 GB**. The 20% policy reserve is 393.35 GB; these floors overlap, not add. |
| HDD filesystem | 15.936 TB total; **14.867 TB available**. After the 20% reserve, **11.680 TB** remains for additional allocations. Existing HDD allocations are already excluded. |
| Database | **1.136 TB**, spanning both disks. Do not add this total to filesystem consumption. |
| Serving headers / raw lookup | **321.52 / 132.25 GB** including indexes. Indexes account for **153.41 / 78.45 GB** respectively. Neither index usage nor safe index removal was established. |
| Hot payloads | **333.21 GB** in 45 dated groups; 67.75 GB predates September 4. Calendar age alone is not permission or proof of reclaimability. |
| Applied storage management | No policy, targets, plans or moves recorded. All 45 payload lifecycle rows remain `open`; canonical archive-manifest samples show zero inserts and an empty heap. The proposed lifecycle is not running. |
| Recovery / migration | WAL archiving is off; no active index build or vacuum at observation. Original capture remains canceled and the forward schema is absent. Existing copies and keys remain untouched. |

GB/TB here are decimal. The local evidence bundle is
`artifacts/storage-implementation/capacity-assessment-20261004/`: host, database,
relation, policy and physical-series observations; exact read-only queries;
calculation inputs/formulas; and historical-source hashes. Essential results and
assumptions are reproduced here because local artifacts are not checked in.
All nine SQL observations completed in under 0.04 seconds each, with read-only
transactions, a five-second statement limit, 100 ms lock limit, 16 MiB temporary
limit and no parallel query workers. Seven Loki queries each read a two-minute
window. No content scan, recursive walk, benchmark, index build or production
mutation was performed.

Physical SSD use rose **31.0 / 24.8 / 13.3 GB** across the last three daily
endpoints. At those rates, the current margin to the migration floor represents
roughly **10–24 days**, not time to disk failure or a guaranteed future rate.
Earlier endpoints include migration relocation and cannot define normal growth.
The historical log samples match device number and size but lack UUID binding;
this sensitivity is not qualified physical-identity evidence for admission.
The deployed archive resource currently observes the same SSD as Docker; those
two series are one disk, not two budgets. The HDD observation comes from its
actual host path. Deployed logs lack UUIDs, so new forecast panels need identified
samples after rollout before they can show a 24-hour estimate.

### Ordinary growth versus retained migration work

Stored relation snapshots give the following daily physical growth proxies:

| Category | Sep 26 23:55 → Oct 3 23:55 (7 days) | Sep 28 23:55 → Oct 2 23:55 (4 weekdays) |
| --- | ---: | ---: |
| Source headers, including indexes | 8.72 GB/day | 10.04 GB/day |
| Source raw lookup, including indexes | 3.65 GB/day | 4.07 GB/day |
| Payload groups | 8.91 GB/day | 10.11 GB/day |
| Other sampled relations | 0.43 GB/day | 0.36 GB/day |
| **Source/component subtotal** | **21.70 GB/day** | **24.59 GB/day** |
| Retained candidate allocations, excluded from that subtotal | 25.99 GB/day | 45.47 GB/day |

These are component deltas, not a controlled collector benchmark or future upper
bounds. The retained candidate's approximately 181.9 GB growth is migration work,
not recurring daily demand. The older retained namespace stayed flat. Database
WAL growth during these intervals includes preparation and must not be labeled
collection-only WAL. A separate September 24 collection receipt observed a
10-minute WAL rate equivalent to 402.8 GB/day; that is a short observation, not
compressed backup growth or a future bound.

### SSD transition: compare the existing policy choices

The retained header group is dated by its newest day, so the 30-day policy keeps
the entire old group on SSD for approximately 30 days after cutover while new
headers accumulate. See `header_catalog.py` and `storage_header_placement.py`.
Retaining historical headers avoids a full historical rewrite; it does not
immediately free their SSD allocation.

Let H/P/O be daily header/payload/other growth above, W the recent window, and K
new permanent composite-key bytes. Immediately before the legacy group can move:

`SSD available = current available - [K + (H + P + O) × W - current payload bytes]`

This scenario assumes verified archival/reclamation catches up by that day, new
identity/raw lookup writes go to HDD, and the current source/raw and rollback
allocations are retained. It uses **44.49 GB**, the corresponding existing unique
indexes, only as a comparator for K. Actual new-key size, sorting, WAL and elapsed
time remain unmeasured; the comparator is neither a bound nor admission.

| Recent window | SSD available before legacy movement, using the two growth proxies | Margin above 496.43 GB floor, before transient costs |
| --- | ---: | ---: |
| Existing 30-day candidate | **487–561 GB** | **−9 to +65 GB** |
| Selected 14-day policy, awaiting qualification | **816–850 GB** | **319–354 GB** |

The 30-day comparison is too tight to accept: one scenario crosses the fixed
floor and neither demonstrates room for the full transient envelope. Qualify
the selected 14-day policy; 30 days is retained here only as a comparison, not an
alternate rollout instruction. The tradeoff is more 15–30-day reads using
HDD/archive readers. Frozen identities, revisions and history lifetime are
unchanged, and affected cold reads must be checked.

**This comparison grants no unverified reclamation credit to an operation.** With
no payload reclamation, the 14-day scenario has only **483–517 GB available**
before transients; the 30-day scenario has **154–228 GB**. Archival must actually
catch up. Processing the current 333.21 GB payload source within 14 days requires
an average **23.80 GB/day of source progress**, followed by at least the admitted
incoming rate; that is a required-work calculation, not measured throughput.
Verified movement may later release the legacy group, but it cannot be credited
before publication and physical observation.

### HDD horizon: future primary data, before recovery

Reuse the measured header/raw growth as provisional new-format proxies. Add the
prior physical identity mean (**486.90 bytes per Fact**) and canonical archive
sample (**472.82 bytes per Fact**) at the observed source insert-counter rates
(**4.40 / 5.02 million per day**), plus the prior raw-archive observation
(**0.566 GB/day**). These are dated sample-based assumptions, not measured new
production densities. Assume unchanged collection volume and provider scope;
research expansion and traffic growth are not budgeted by these samples. They
imply **17.16–19.49 GB/day** of durable HDD growth.
About **85%** of that modeled durable growth is headers, identity and raw-lookup
metadata, including their indexes. This makes metadata/index cost the main
long-term improvement target. Payload history is represented by compressed
archives here, not counted again as uncompressed historical payload partitions.

With 14 days of future headers retained on SSD, future arrivals alone consume:

| Additional horizon | Additional primary HDD bytes | Remaining from today's 11.680 TB above reserve |
| --- | ---: | ---: |
| 180 days | **2.97–3.37 TB** | **8.31–8.71 TB** |
| 365 days | **6.14–6.98 TB** | **4.71–5.54 TB** |
| 730 days | **12.40–14.09 TB** | **−2.41 to −0.72 TB** |

This deliberately excludes the old header relocation and new keys, current
archive catch-up, missing identity/raw adoption rows, new archive catalog costs,
recovery baselines/increments/WAL, retained failed attempts and temporary overlap.
Existing HDD allocations already reduce current free space. It is an optimistic
planning comparison, not a total-space upper bound. The two-year target does not
fit these proxies even before recovery. One year is conditional on those omitted
costs; this assessment does not certify it. Fourteen days improves SSD pressure
but does not eliminate long-term historical growth.

### Peak budget and the remaining decision

Use the existing placement, archive, maintenance and encrypted-recovery owners;
add no storage service, registry or alternate data authority. For each physical
disk, count existing occupancy once, then additional coexisting demands. Apply
`max(policy reserve + demands + competing claims, operation floor)`, not the sum
of overlapping reserves. Keep source/rollback evidence until separately approved
retirement; no deletion or deduplication saving is assumed.

| Phase / owner | Known quantity | Missing quantity / next qualification |
| --- | --- | --- |
| Forward keys | Original one-hour allowance plus 60-second cancellation grace: 68.72 GB WAL, 34.36 GB temp, 34.36 GB maintenance, 30.70 GB declared growth. Current arithmetic leaves **252.75 GB** before uncovered costs. | The 34.36 GB maintenance term must cover actual key construction; existing key comparator is 44.49 GB. Measure keys, sort/WAL peaks, collector/read impact and elapsed time through the existing explicit preparation boundary. Do not add K twice or treat declared allowances as measured costs. |
| Retained-target adoption / references | Identity/raw baselines are already on HDD; don't charge their 73.51/101.41 GB again. October 3's exact missing-ID count and 8.12 GB allocation comparator remain dated evidence. | New missing rows, raw catch-up, references/catalogs and mirror overhead; reuse completed coverage rather than repeat a whole-history count. |
| Archive catch-up / legacy movement | 333.21 GB payload source; 321.52 GB old header group plus new keys. | Verified processing rate and working space; whole-group copy/locks/WAL and publication. Failure retains the stable source and known ownership; uncertain completion is inspected before retry. |
| Encrypted recovery | Initial full baseline and later dependency-safe replacement are necessary; original per-attempt cap is 1.675 TB. | Actual compressed baseline, changed blocks, retained chains, archives, WAL and partial/replacement overlap. Two retained points are not three recurring full logical dumps. Measure actual repository allocation and separately qualify recovery; earlier disposable acceptance does not establish host-size costs. These results must precede a supported horizon or hardware size claim. |

The eight/twelve MiB-per-second SSD/HDD growth limits in the recorded migration
request are admission inputs, not measured collection rates. Daily averages do
not justify reducing them, renewing expired clocks or reusing an old operation.
All preparation, final-stop and recovery bounds still need to pass their existing
owners. Current free space alone does not admit the migration.

Next qualification should resolve only three questions: can archive catch-up and
the 14-day window protect SSD collection; do key/adoption/movement/recovery peaks
fit and recover within their bounds; and what HDD capacity or measured storage
reduction supports the chosen horizon? Prioritize header/raw index and repeated
metadata costs when evaluating reductions, preserving uniqueness, provenance and
frozen reads. No index removal is authorized by size or low observed use alone.
No finite disk supports append-only history indefinitely.

### Dashboard and release status

Use **QuantTrad Capacity & Database Growth**, UID `quanttrad-capacity-growth`, at
`/d/quanttrad-capacity-growth` on the configured Grafana host. It covers physical
headroom, native 24-hour net growth and estimated days to the 85% usage alert,
database/schema/relation growth, TOAST, WAL, connections and sample freshness.
The 85% estimate is not the policy reserve, migration floor, two-year forecast or
maintenance authorization. Missing/stale/unhealthy/unknown-UUID/replaced/resized
or non-growing evidence produces no runway; shared disks must not be summed.

The forecast reuses two fresh two-minute windows 24 hours apart. The disposable
Loki/Grafana proof exercises actual queries and known numeric/invalid-source
cases; synthetic history needs test-only ingester lookback. The filesystem
observer must also emit its discoverable UUID without a configured expectation,
which the October 4 host check found missing for SSD. That local correction does
not weaken configured UUID admission or deploy the dashboard. Applicable
production capacity, lifecycle throughput, recovery and release gates remain
open; passing tests do not start migration or research.

## Reuse existing owners

The data/storage owner owns models, repositories, archive readers and migration
primitives. The release/operator owner owns exact runtime compatibility,
physical admission and recovery. Research owns its protocols and evidence, and
validates affected reads. These are existing responsibilities.

| Existing work and code areas | Treatment |
| --- | --- |
| Q `portal/backend/db/{market_data_models,market_storage_models,fact_storage_schema,session}.py`; `src/market_data/{frozen,fact_archive,archive}.py`; `portal/backend/service/storage/repos/{market_data,fact_storage,market_lifecycle}.py` | Preserve Fact/Dataset semantics, raw evidence, archive verification and lifecycle ownership. Extend only the actual persistence boundary that needs change. |
| S `scripts/db/{fact_header_forward_keys,fact_header_forward_adoption,fact_header_v2_references,fact_header_v2_handoff}.py`; `portal/backend/db/fact_header_legacy_schema.py` | Reuse implemented retained-history and reference mechanics where exact-source qualification applies. No assumption that production cutover is complete. |
| S `scripts/db/fact_header_v2_capture.py`, `scripts/automation/storage_online_*.py`; `portal/backend/service/storage/{header_movement,header_resource_claims,incremental_recovery}.py` | Reuse capture, ownership, bounded phases and recovery mechanisms. Do not create a second controller or replace their receipts with a generic framework. Retired/cancelled operations remain retired. |
| R research definitions, frozen derivation and single-attempt work; Q `portal/backend/service/research/` | Preserve completed work. H04 has no intrinsic physical schema dependency; evaluate release composition against the actual serving schema. Do not weaken startup guards or maintain a permanent second application fork. |

## Implementation slices

**Order: M0 -> M1/M2 -> M3; M4 only when the admitted capacity goal requires it.**
M1 and M2 may be developed separately, then qualified together. Each slice updates
its existing component document/runbook with the exact source and evidence.

### M0 — Choose the route and account for the work

Replace “finish the storage rollout” with a concrete capacity goal and justified
historical scope. Storage and release owners identify the serving schema/runtime,
selected candidate, archive/recovery dependencies, retained allocations, growth
horizon and required pause. Use existing inspection, inventory and admission code.

No schema/API change is required for this decision. Identify what can stay
unchanged, what must be converted or validated, and what can wait. Preserve
existing deadlines and resource limits; a different proposal requires an explicit
review, not a silently extended operation.

Completion requires a component-by-component byte/work ledger, route comparison,
compatibility decision and numeric admission limits supported by evidence.
Unknown capacity or an unjustified full-history phase blocks that operation.
Cancelling this planning slice changes no serving state. Its cost is bounded
inspection and review, not a production scan or build.

### M1 — Qualify unchanged meaning across retained and new storage

The existing repository must return the same logical Facts whether they live in
the retained table, new daily groups, hot payloads or supported archives. New
writes follow the selected layout; old data need not be reformatted to be read.

Storage owns the model/reader work identified above. If adopting S's layout,
its parent, keys, identity and legacy binding are explicit schema changes with
operator cutover requirements. Reuse those candidate structures; invent no new
compatibility catalog. Preserve SQL source/revision/known-at selection before
hydration, and test ranges spanning the old/new boundary and late corrections.

Validate exact values, identities, ordering, invalidations, provenance, material
and frozen-input hashes. Include empty ranges and corrupt/missing objects.
Do not weaken uniqueness, frozen validation or integrity checks for compatibility.
Retain the source and required readers. Unsupported versions fail explicitly;
application rollback is allowed only against a schema it supports.

Reader support and retained objects have ongoing cost. Document the support and
pin lifetime before retiring either; no automatic deletion is included here.

### M2 — Prepare and verify within bounded storage operations

Use existing migration owners to prepare keys, capture concurrent writes,
catch up identities/references and verify the selected scope while the serving
source remains authoritative. Replace blanket client holds with the specific
brief fences needed by each phase.

Bound rows, bytes, transaction/statement time and total phase time. Commit each
verified unit with its cursor; use transactional capture so a late commit cannot
fall behind a numeric cursor. Prevalidate references outside the final fence
where the actual database mechanics permit it. If a native full scan/build is
unavoidable, identify and admit it explicitly; do not label it a bounded page job.

Ownership and resource admission are separate: one actor may own a job yet lack
space or I/O headroom. Reuse current storage claims/watchers and fixed limits.
Include collection growth, spool, WAL, temporary files, indexes, source/target
overlap and backup dependencies. Release claims only after terminal outcome and
execution are known. Stop/throttle migration before collection exceeds its
declared tolerances; a timeout is not proof the server operation stopped.

Tests cover duplicate actors, late commits, page failure, lost ownership,
cancellation, restart and uncertain completion. No partial output becomes serving
truth. Preserve valid completed work and its provenance. Reusing retained data in
a separately authorized replacement requires ownership and content proof; it
cannot renew an expired or cancelled operation. This slice adds no research
scheduler or cache.

### M3 — Publish through a measured final switch and qualify the release

Catch up, perform the remaining proof and publish the layout through the existing
fenced operator transaction and durable outcome inspection. The final pause has a
declared maximum and measured evidence on a representative physical workload.
Do not put all-history comparison inside that pause by default.

For the retained-history candidate, range validation/attachment and composite
constraints still need explicit timing and lock proof. If they cannot fit, reject
or redesign the route before starting; do not keep writers stopped while inventing
the next step. Include actual affected readers in interruption accounting.

Before publication, cancellation leaves the source authoritative. After a lost
commit reply, reconcile the durable outcome before retrying or reopening writers.
After publication, use the tested forward recovery/rollback procedure for the
actual schema and any new writes. Retaining an old image or table alone does not
make rollback safe; never restore a DB over newer collection automatically.

Qualify the exact runtime/schema pair, preserved archives and usable recovery
dependencies. Keep the technical ability to qualify research independently from physical
cutover; the current coordinated release decision above elects one deployment. This slice does not authorize any deployment or automatic
research restart. Its costs include rehearsals and retained recovery material.

### M4 — Place historical groups only where capacity requires it

If M0 requires physical movement to meet its capacity goal, qualify that movement
separately through existing storage placement and recovery owners. A large retained
table remains a large movement unit; retaining it avoids reformatting, not the
cost of moving its bytes.

Budget the complete heap/index group, WAL, temporary overlap and read locks.
Read while a replacement is prepared only through a supported stable source;
publish verified locations atomically, preserve pins and recover uncertain moves
through existing receipts. Stop new work under pressure without discarding source
data. Test interruption, missing mounts, changed target identity and restore.

No historical cleanup is implied by successful movement. Reclamation is separate
authorized work after reader, pin and backup checks. If the goal requires a large
legacy move and it cannot fit the limits, report the capacity goal unmet.

## Authorized target-state follow-up: history cache and measured research

On October 4 the user authorized implementation toward the selected target,
including a bounded SSD history cache and controlled research measurements. This
extends the prior deployment package; it does not revive any cancelled operation,
relax physical admission, or restart paused automations. Implementation and
qualification status must remain distinct from deployment and observed benefit.

### C1 — Disposable immutable archive copies on SSD

Extend the canonical archive reader and existing storage admission boundary.
HDD objects and PostgreSQL manifests remain authoritative. For example, a frozen
2022 read may populate a verified SSD copy; another read can reuse it without
changing Dataset identity. Eviction removes only that disposable copy: there is
no write-back, Fact deletion, schema migration, historical conversion or result
cache. The 14-day recent-Fact placement policy remains independent of cache use.

Use an explicitly enabled byte/object quota beneath collection/maintenance
headroom. Admission respects the saved policy, filesystem identity and existing
claims; a conflicting maintenance owner prevents new fills. Cache entries use
immutable content hashes. Check current manifest/schema/envelope semantics on
all reads, including hits. Least-recently-used inactive objects are eviction
candidates; market observation age is irrelevant. Expire inactive cache copies
after 14 days when the cache is maintained, with earlier eviction under pressure.
Bound scan/fill work and check cancellation. An oversized request can read HDD
without filling SSD. No read may recursively stage its whole historical range.

Use the existing reader owner, filesystem locks and execution metrics. The new
cache directory contains replaceable bytes and recency metadata only; it is not
a second archive catalog, backup dependency or data authority. Publish complete
verified copies atomically, exclude active readers from eviction, serialize
conflicting fills and recover abandoned partials without touching archive/spool
paths. Cache errors must be explicit; unavailable cache uses the admitted HDD
reader, while invalid authoritative data still fails. Rollback disables caching
and retains durable HDD objects and original frozen references.

Required validation: identical hot/cold/cache outputs and correction visibility;
concurrent same-object fill and active-read eviction; cancellation/crash during
fill/publication; stale partial recovery; quota/reserve/claim and wrong-mount
refusal; corrupt-cache handling; bounded I/O and no source mutation. No latency
benefit is claimed from these correctness fixtures.

### C2 — One measured research operation, across processes

Use the existing research execution and PostgreSQL ownership boundaries to admit
one heavy preparation/Check/replay operation globally during qualification.
Worker count and API-local semaphores alone are not a global resource limit.
Nested operations share admission; owner loss cancels execution, and completion
or cancellation releases ownership only after the operation unwinds. Keep this
separate from scientific attempt accounting and storage-maintenance ownership.

The first admitted workload is the existing H04 BTC 2022 protocol, retaining its
one Check and one replay allocations, exact inputs, gap policy and holdouts.
Record collector-only baseline, then phase timing, physical/read-cache bytes,
RSS, collector lag/spool and disk headroom during preparation, Check and replay.
Check and replay totals are different workflows, not a clean cache A/B. Use an
identical bounded read for isolated cold/repeated-read comparison; never flush
production OS caches. Cache bypass does not establish an OS-cold disk read.
Agenticks' four unvalidated drafts are later candidates after source/protocol
admission. Review/gap-handling chats are supporting work, not additional workers.
Increase concurrency or allocate more scientific attempts only through existing
explicit budgets and demonstrated collector/resource limits.

## Evidence required before an operation is admitted

No row below is a new universal research gate. Each applies to the storage phase,
release or read path being changed. Current status is not a fresh production check.

| Requirement | Pass evidence | Current position / owner |
| --- | --- | --- |
| Necessity and historical scope | Measured capacity goal, explicit affected scope, alternatives and justification for every all-history copy/scan. | Real growth evidence; selected route and fresh horizon still need admission. Storage/release. |
| Meaning and compatibility | Exact candidate/serving-schema inventory; identical logical and frozen inputs across affected old/new/hot/cold paths; strict guards still pass. | Implemented/tested candidate parts, incomplete end-to-end qualification. Data/storage. |
| Concurrent preparation | Real collector-shaped inserts and late commits survive capture/catch-up; references remain valid; no long blanket writer fence. | Existing capture/online work is reusable evidence, not complete admission. Storage. |
| Resource and interruption bounds | Numeric peak space, phase/pause, lock, lag/spool and cancellation limits supported by measurements; pressure stops work within the bound. | Recorded capacity admission incomplete. No invented speedup, zero-interruption or reclaimed-space credit. Storage/operator. |
| Ownership and uncertain outcome | Two actors cannot publish twice; stale owners fail; crash before/after publication and lost reply reconcile without data loss or unaccounted execution. | Candidate tests exist; require coverage of the selected exact route. Storage. |
| Recovery and deployment | Applicable disposable restore/promotion checks pass for actual DB, archives, locations and keys; compatible rollback or forward recovery documented. | Historical receipts have limited scope; cancelled restore is not a pass. Release/storage. |
| Research coexistence | Affected frozen reads/replay retain identities and semantics; compatible research can run within resource limits; any pause names the operation, reason, limit and recovery condition. | No intrinsic H04 schema need; combined candidate does require new storage. Research/release. |
| Capacity outcome | Actual placement and projected growth meet M0's stated horizon, including growing identity/catalog/backup costs. | Retaining old data on SSD alone does not prove relief. Storage/operator. |

Start later qualification with disposable concurrency and failure fixtures, small
representative ranges and plan inspection. Measure header selection, hydration
and decoding separately from research computation when attributing read impact.
Compare a recent series query, a frozen historical range and the cold/legacy path
actually affected. Record collector throughput/lag, spool, DB waits, disk headroom
and relevant write/WAL/temp bytes during the operation.

Small fixtures prove behavior, not production duration. Increase scope only under
admission. Existing full-scan/build receipts can be reused when their source,
hardware and operation match; do not rerun heavy measurements merely to fill a
new template. No heavy measurement is authorized by this documentation change.

## Research continues under its existing rules

No migration is needed merely to add an Indicator or run another versioned Check.
Preserve existing scientific protocols, holdouts, trial budgets and execution
authority. H04's existing preparation and qualification remain useful, but its
completion is not this storage project's universal acceptance gate.

During bulk storage work, compatible research may continue if its reads and the
combined resource use are admitted. Where a particular operation cannot coexist,
defer that operation for a bounded, stated reason instead of freezing all research
until unrelated migration or cleanup finishes. Readers never trigger conversion.

A frozen Dataset protects input identity; it does not retain executable code.
Current Check replay also requires the producing code revision to be running
(Q `portal/backend/service/research/service.py:1437`). A release/migration plan
must account for that when promising replay, without adding historical-runtime
hosting infrastructure. Preserve results and code identity; do not silently replay
under a different version.

Nothing here clears current holds or launches work. Existing research authority
remains necessary, independently of a storage gate passing.

## Bounded research execution

Implement R0 -> R1 -> R2 alongside M0–M4. Passing research tests does not qualify a
physical migration, and unfinished historical placement does not block a release
whose exact runtime/schema pair is independently compatible. Reuse the research
changes already included in storage candidate S; preserve R as evidence for the
independent research release. Do not weaken S's layout guards to pretend that its
combined package supports the serving schema.

### R0 — Individual job cancellation in the existing queue

Extend `portal_async_jobs`, the research worker and `qt research jobs`. Before:
a CLI timeout or shutdown signal can leave a long computation running. After:
an explicit cancellation request stops the individual job at execution checkpoints,
cancels that job's active database statement, and refuses result publication.

Queued jobs can finish cancelled immediately. Running jobs retain their claim and
in-flight request identity until the worker acknowledges that computation and its
database work have stopped. A stale heartbeat is not proof of stopped execution;
report uncertainty and prevent automatic retry of a cancellation request. Under
the existing row lock, completion and cancellation have one definitive winner.
Never terminate the shared backend supervisor to cancel one research job.

Own this in `service/async_jobs`, `workers/research_worker.py`, research dispatch,
controller and CLI. Keep requests immutable and reuse existing job metadata where
possible; no new queue or scheduler. Cancellation does not publish partial Checks,
refund scientific attempts, resume checkpoints, remove source data or cancel
another job. Retrying is an explicit new request with the same pinned semantics.

Validate queued/running/terminal cancellation, duplicate cancellation, concurrent
completion, lost ownership, worker failure, blocked SQL, shutdown and CLI/API
contracts. Measure acknowledgement latency separately from request latency.
An old worker must be drained before enabling the new cancellation surface;
request acceptance must not imply that an unqualified worker can stop.

### R1 — Stream historical input through existing boundaries

Replace unnecessary complete input lists at the repository, hydration and runtime
driver boundaries with bounded pages. One engine instance consumes the ordered
stream using `initialize -> apply_bar -> snapshot`; a page boundary never resets
warmup, gaps, indicator commit clocks or known-at selection. Preserve exact input
hashes and correction choice across page sizes and hot/cold/retained layouts.

Select series, range, revisions and fields before hydration. Bound each page and
close readers on error or cancellation. Preserve materializing APIs for consumers
that need them, but apply explicit total admission limits before unbounded work.
Algorithms and outputs that intrinsically retain history still consume memory:
declare and enforce their bounds rather than claiming constant-memory research.

Owners are `storage/repos/market_data.py`, frozen Dataset validation, runtime
market-data resolution, Indicator runtime validation and Check execution. No
second canonical store, generic artifact platform or read-triggered conversion.
Incomplete computation publishes no durable evidence; retry starts from its
pinned request. Old evidence hashes and exact replay remain compatible.

Validate multiple page sizes, empty ranges, corrections crossing page boundaries,
recorded gaps, late availability, cold corruption and cancellation during reads
and engine work. Disposable concurrency fixtures now reproduce delayed append and correction
leaking through a database-global watermark. New freezes select and store each
series' committed watermark under the canonical writer's existing series lock
ordering, and carry `per_series_committed.v1` selection identity. Existing hashes
are unchanged; re-freeze is required for the stronger guarantee. Both regression
cases passed on the isolated PostgreSQL stack.

### R2 — End-to-end limits and qualification

Use the existing research execution boundary to enforce declared row, byte,
elapsed-time and output limits, including synchronous preview/replay paths.
Cancellation and limit failures are explicit failures, never truncated successful
evidence. Keep worker concurrency separate from ownership and per-job budgets.
No cross-service priority scheduler, persistent result cache or automatic research
launch is included.

The representative workload set is: recent series/time selection; long frozen
input preparation; cold history; the recorded expensive provenance lookup; and
year-scale H04 plus an identical repeat. Use existing receipts first. Record
selection, hydration, decoding, engine, evaluator and persistence time separately,
with peak memory, rows/bytes read and written, archive verification/reuse and
collector lag/spool. Repeating a request is not assumed to be cached, and required
scientific replay must still execute. Small fixtures establish semantics and
limits; they do not establish year-scale speedups or production capacity.

Existing research receipts provide these baselines; do not replace them with
estimates or interpret a timeout as measured successful latency:

| Workload / receipt in R `artifacts/five-year-research/` | Existing evidence | Still missing |
| --- | --- | --- |
| Annual coverage; `year-scale-20260929/calendar-coverage-packet.json` | Client timeout at 46.5 s; server terminated around 125 s without a recovered result. | Successful selection, hydration and decoding measurements. |
| L02; `landmark-dependence-20260924/L02-preparation-progress-20260929.json` | BTC source freeze 48.2 s; 5m/30m derivation 38.1/29.4 s; final preparation/freeze 85.5 s. | Phase costs and input/output byte counts. |
| Frozen L02 replay in the same research receipts | BTC 12.0 s; ETH 14.8 s. | Identical-source repeat, cold/hot split and resource use. |
| Synthetic annual H04; `year-scale-20260929/H04-qualification.json` | Engine 383.7 s; total 428.8 s; peak RSS 2,332,000 KiB. | Database selection, hydration, freezing and persistence; comparable streamed execution. |

The implementation's default resource ceilings are starting configuration,
not measured performance acceptance targets or proof that every historical request
fits. Qualify each representative workload against its actual declared limits
before admitting autonomous execution; never silently expand a budget to finish.

Research qualification requires: equal frozen inputs under concurrency; preserved
known-at/gap/engine behavior; equal supported format reads; enforced limits;
cancel/duplicate/stale-owner/publication tests; exact release compatibility; and
unchanged protocols, holdouts and scientific budgets. Each requirement gates the
affected capability only. Deployment and resumption remain separately authorized.

## Implementation and release ledger

The main candidate integrates S (including its R ancestor) in `89a6ba0f`; it is
local source, not a deployed release. Original candidate checkouts are preserved.

| Slice | Implemented/reused now | Remaining qualification; capability blocked |
| --- | --- | --- |
| M0 | Existing inventory/admission commands and retained-history route; source headers 316,197,470,208 bytes including indexes and raw lookup 130,259,869,696 bytes remain separate allocations, not additions to database totals. | Fresh growth horizon, incremental identity/key/WAL/temp/backup overlap and numeric physical limits remain unmeasured. Blocks production storage admission, not compatible research development. |
| M1 | S's retained/daily headers, identity/references and supported hot/cold readers; frozen series concurrency repair in `7464d7f9`. | Exact release/schema compatibility and required old/new/cold cases must pass. Does not grant old-schema support to the combined candidate. |
| M2 | S's transactional capture, bounded pages/catch-up, fenced ownership, cancellation and resource claims reused unchanged. | Collector-shaped physical qualification and actual lag/space tolerances remain necessary before running maintenance. |
| M3 | S's forward adoption and durable outcome/recovery mechanics retained; no alternative cutover controller. | Native final constraint/attachment scans still need admitted pause and recovery proof on the selected physical route. No cutover performed. |
| M4 | Existing placement owners and recovery receipts retained. | Deferred until the measured capacity goal requires movement. Retained SSD history is not reclaimed capacity. |
| R0 | Individual queue/API/CLI/MCP cancellation, owned SQL interruption and publication fencing in `6b04b9dc`. | Worker drain/version compatibility before enabling the surface. Dead-owner cancellation stays uncertain pending explicit proof of stopped execution. |
| R1 | Paged SQL hydration and candle windows into one continuous engine; incremental semantic hashing. | Frozen validation and statistical outputs still retain admitted history. Year-scale RSS and cold decode costs require measurement. |
| R2 | Shared total limits, per-process synchronous admission, terminal budget failures and phase accounting. | Default ceilings are not demonstrated H04 capacity. No throughput or speedup claim until the representative workload comparison. |
| C1 | Implemented on `feature/history-read-cache`: immutable object copies, private bounded namespace, shared active-reader locks, atomic publication, policy/reserve admission and existing execution accounting. | Disabled until explicit cache bytes/free-space floor and an operator-prepared SSD directory are selected. Local fault/compatibility checks passed; aggregate release qualification remains pending. No deployment or observed speedup. |
| C2 | Implemented on that branch: optional shared PostgreSQL admission across API/worker processes, retained through worker publication. Server composition selects one worker and global serialization. | Disposable PostgreSQL ownership/exclusion/loss tests passed; aggregate release qualification remains pending. Cooperative cancellation is not a hard CPU/RSS limit; one slot does not establish collector headroom. |

Storage allocations retained for recovery, source/target overlap and temporary
migration copies receive **zero assumed reclamation credit**. No route is admitted
merely by merging code or passing small fixtures. Resource ceilings control one
execution; C2 adds one shared research slot in the server composition. Neither
proves combined collector and maintenance headroom or provides a global scheduler.

For autonomous research, require the applicable semantic/concurrency, supported
read-format, cancellation/ownership and deployment-compatibility tests, then a
successful representative workload within its declared scientific and resource
budgets. It may submit bounded previews, frozen Checks and replay under existing
protocol/holdout authority. It may not change schema, expand budgets, acquire
unapproved history, deploy, move/delete storage or restart operations. This task
launches none of that work. Full historical conversion and unrelated cleanup are
not research prerequisites.

### Research acceptance matrix

| Requirement / owner | Exact pass criterion and evidence | Current evidence | Capability affected / present limit |
| --- | --- | --- | --- |
| Frozen selection / data | Delayed cross-series append and correction cannot change a new freeze; the regression fixture must also demonstrate the old global predicate changing. `test_repository_db.py::test_freeze_excludes_late_cross_series_commit`. | Passed native concurrency fixtures. | Stronger guarantees apply to new per-series freezes. Existing manifests remain immutable and validated. |
| Temporal/engine equivalence / research | Equal evidence and hashes for page sizes 1, 37 and 512, with warmup, gaps and delayed availability; real freeze/replay workflows pass in `test_forward_risk_check.py` and `test_candle_only_check_workflow_db.py`. | Passed engine equivalence and native workflows. | Admits the tested Indicator/read paths, not arbitrary new algorithms. |
| Supported physical reads / storage | Equal pinned records across hot/cold and retained/new headers; missing/corrupt archive fails. `test_cold_consumers_db.py`, `test_fact_header_legacy_db.py` and repository fixtures. | Targeted hot/cold tests passed; retained/new qualification recorded below. | A reader/format must qualify before that format is used; full-history conversion is unnecessary. |
| Stop and publication / queue | Queued cancellation is terminal; running request retains identity until acknowledgement; blocked SQL unwinds; completion and cancellation have one row-lock winner; stale owner cannot publish; uncertain helper stop is retained. `test_research_cancellation*.py`, `test_async_jobs_db.py` and heartbeat tests. | Unit and native race/interruption tests passed. | Enabling cancel requires compatible workers. Dead-owner reconciliation remains explicit operator work. |
| Resource bounds / research | Nested calls cannot reset totals; deadline interrupts SQL without relaxing an existing shorter timeout; byte/row failures publish no Check and do not auto-retry; synchronous capacity refuses excess work. Cancellation/limits tests. | Enforcement passed; workload sizing remains open. | These prove enforcement, not a safe annual workload size or a hard process-memory cap. |
| Release composition / release | Exact source passes against its intended clean schema and supported archives. Combined S needs the new layout; independent research composition must retain the old reader/guards and be separately tested. | Separate local compositions; no deployment qualified here. | Blocks deployment of an incompatible package; does not make physical cutover a research prerequisite. |
| Workload admission / research + operator | Record each representative workload's latency, peak RSS, logical and physical I/O, repeated work and collector lag/spool; all must fit the explicitly admitted machine/workload budgets. No fabricated numeric acceptance target. | Open: no new year-scale or production measurement. | Still open for annual H04 and production coexistence. Limits must not be silently increased to obtain a pass. |
| Scientific authority / research | Existing protocol, holdout, family-budget and exact-code replay tests remain passing; execution limits are outside scientific identity and do not refund attempts. | Existing contracts pass; holds unchanged. | Passing engineering tests never starts an experiment or clears an existing scientific hold. |

Small database and engine fixtures support only their tested source and scenario.
Storage-demo filesystem and recovery topology requirements are separate evidence;
a skip is unavailable validation, not a pass. Test commands, revisions and final
results are recorded below rather than spread across operational handoffs.

## Separate follow-ups, outside this implementation

These are recorded findings, not mandatory additions to the migration:

- **Query and hydration performance:** the saved provenance lookup averaged
  2,354.83 ms over 25 calls (S `artifacts/storage-implementation/bip-delay-statement-deltas-20260924.json`).
  S already contributes the keyed hot-provenance indexed witness path in
  `storage/repos/fact_storage.py::material_witness_exists`, retaining exact
  payload verification and legacy/cold fallbacks. This is reused code, not a
  newly demonstrated latency gain. Measure it against the saved workload before
  attributing improvement; no blanket index removal follows from the aggregate.
- **Repeated replay and reuse:** Q `portal/backend/service/research/result_reference.py:192`
  executes replay while resolving scientific result evidence. Account for that
  work before proposing caching; preserve required scientific verification.
- **Persistent computation caches:** require evidence of worthwhile repeated
  computation and preserved scientific verification. Do not introduce a general
  scheduler, feature store, artifact catalog or new research service here.

## Documentation and handoff

Keep this specification and ADR 0078 as the central decision record. Detailed
mechanics and receipts stay with existing storage component docs/runbooks. Update
contracts only if product meaning changes; mark implemented, tested and deployed
states separately. The earlier broader plan remains in Git history, not as a
parallel active implementation mandate.

Documentation changes require index generation, `make validate-docs`,
`make sync-docs` and `git diff --check`. Implementation validation follows the applicable [normal validation matrix](developer-workflow.md),
including disposable DB and recovery tests for affected persistence boundaries.
Unavailable or skipped evidence is not a pass. Runtime implementation and local
disposable validation are authorized. The later target-state goal includes release
qualification and deployment, subject to concrete operational authorization and
admission. This document grants neither; cancelled operations and paused
automations remain unchanged.

## C1/C2 local qualification — 2026-10-04

[PR #223](https://github.com/elijahbrookss/quant-trad/pull/223) targets the preserved
consolidation branch. Runtime source: `e8003eafa48d11bea3d58207fb8844fe59e2f2d8`
(including the ownership-observation hardening).
Local receipts, including failed attempts, are under
`artifacts/storage-implementation/history-read-cache/`.

- Final `make backend-check`: **4,524 passed, 5 skipped**.
- **28** cache tests cover codec equivalence, source preservation, bounds, active
  readers across processes, crash before/after publication, cancellation, replaced
  roots and corrupt-copy fallback. **9** global-admission tests include stale
  ownership observations during a stuck probe, uncertain
  helper termination and ownership through successful/failed worker publication.
- **4** disposable PostgreSQL tests passed: storage-owner exclusion, reserved
  capacity, shared research admission and actual admission-connection termination.
  The existing native research cancellation suite also passed **8** tests; the
  combined final-source run passed all **12**.
- Frontend **240 Node + 50 JSX** tests passed with installed Node 22; a fresh-output
  Vite build passed. Ordinary `make frontend-check` was blocked first by host Node
  12 and then existing `dist` ownership. No permissions or existing output changed.
- Shell syntax, disposable base/storage Compose rendering, documentation/index
  checks passed. `make sync-docs` has no configured destination and was skipped.

These checks qualify tested behavior only. Aggregate CI, production migration
admission, exact-package deployment, cache sizing and measured collector/research
coexistence remain open. No production mutation or research run occurred.

## Earlier R0–R2 local validation — 2026-10-04

That research implementation source is `7354cb58ae2b6c2591cd8e55e63db8d3346e5860`.
Later documentation commits do not change that runtime. Validation logs, including
failed attempts, are preserved under
`logs/research-data-evolution/20261004-7354cb58/` in the main checkout.

- `make backend-check`: **4,482 passed, 5 skipped** on the final runtime.
- Frontend: **240 Node tests and 50 JSX tests passed**, plus a successful Vite
  production build using the available isolated Node 22 image and a temporary
  output directory. Host Node 12 cannot run these tests; the first ordinary build
  could not clear existing output permissions. No permissions were changed.
- Final combined-layout targeted DB run: **38 passed**, covering cancellation,
  deadlines, pool reuse, ownership, frozen concurrency, bounded hydration,
  hot/cold reads and the canonical migration fixture. The earlier retained-header,
  frozen and cold run passed **43** tests at `7464d7f9`. The final runtime also
  passed `test_real_qt_legacy_attach_read_compatibility` (**1** native test),
  exercising reads across retained history and new daily headers.
- The attempted complete DB suite was **not completed**: **17 passed, 22 skipped,
  one failed** before stopping. That failure was the disposable migration database
  cleanup's 15-second timeout, after its test assertions. Its serialized retry
  passed in the 38-test run. Do not present this as a full-suite pass or extend an
  operational timeout from this test result. Topology-dependent storage-demo
  skips do not qualify real filesystem movement or recovery.
- Deployment scripts passed `bash -n`; server Compose configuration rendered
  successfully with generated disposable values, without loading real credentials
  or deploying.
- `make sync-docs` reports **skipped** because no destination is configured on this
  machine. `make validate-docs` passed **10** tests; `git diff --check` passed.
  The intended implementation and documentation are committed; unrelated candidate
  checkouts remain unchanged.

The independent research composition starts at R `a0458169`, with the research,
frozen-visibility and execution-control commits applied while retaining R's old
SQL reader and startup guards. Its only runtime conflict resolution preserves
`CANONICAL_ROW_FROM` at the existing query boundary instead of S's
`CANONICAL_RANGE_ROW_FROM`. No layout guards were relaxed and no historical
conversion was performed. This is a qualification snapshot, not a permanently
maintained second application. At `9da4f8ab`, **41 native DB tests** passed,
including real freeze/replay and cold reads, with **145 focused unit tests**.
The final connection hardening is applied at `e3eb4402`; its focused unit checks
passed **96** tests and its final native cancellation/deadline/pool suite passed
**8** tests. The exact snapshot is preserved by local branch
`feature/research-layout-compat-20261004` (renamed from the original
`verification/` reference without changing its commit), with its disposable qualification
checkout at `/tmp/qt-research-layout-compat-e9cbez_7/checkout`. The original R and S
checkouts are unchanged. This proves a concrete independent implementation path;
it does not authorize deployment, clear scientific holds or demonstrate workload
capacity.
