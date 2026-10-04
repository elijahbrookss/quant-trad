# Research and Data Evolution: Minimum Implementation Specification

**Status: proposed implementation; documentation only. Updated 2026-10-03.**
Decision: [ADR 0078](../architecture/decisions/0078-evolve-research-within-existing-data-boundaries.md).
Platform contracts remain authoritative. No gate in this document is an approval
to execute operations, and passing gates does not restart a paused campaign.

## Problem, solution, and expected consequences

QT has most of the necessary components already. The problem is making their
guarantees hold together under continuous collection: immutable research inputs,
bounded preparation and computation, recoverable job ownership, and an application
release compatible with the actual storage layout. Saved measurements identify
costly provenance reads and H04 engine work; they do not establish that moving all
history or adding date partitions will substantially accelerate research.

Keep Facts stable; version computations and derived outputs in their existing
owners. First qualify frozen selection and a bounded research path on a supported
serving layout. Optimize measured bottlenecks and selectively reuse expensive
results. Evolve physical storage independently through its current boundary.
Benefits should be fewer compulsory migrations, less repeated work, and less
manual scheduling. Costs are targeted correctness tests, budget enforcement,
compatibility support and retained pinned data. Latency improvements remain
unquantified; cold I/O and irreducible engine work remain real costs.

The first market-data milestone is the already prepared **BTC 2022 H04 Check,
followed by a separately admitted exact replay**. A five-year request must be
plannable and rejectable within explicit limits; unrestricted five-year execution
is not the first release criterion. No market run is authorized by this task.

## Evidence boundary and existing owners

Inspected checkout snapshots: **Q** = `/home/elijah/dev/quant-trad` at
`83c2a7f792f2d666bd666a3d252d5e6cc4c5db9e`; **S** =
`/home/elijah/dev/quant-trad-codex-storage` at
`3f4c55c6a04eb65f969043026cc870feffbbe750`; **R** =
`/home/elijah/dev/quant-trad-codex-research` at
`a045816960e36ed1c6096fd3872d37c3808b0b66`.
Paths prefixed Q/S/R refer to these snapshots; line references describe them.
Within owner tables, `db/`, `service/`, and `workers/` are under `portal/backend/`;
`repos/` means `portal/backend/service/storage/repos/`. Bare repository filenames
such as `market_data.py` and `fact_storage.py` refer to that `repos/` directory;
`market_data_models.py` belongs to `portal/backend/db/`, and market service
filenames belong to `portal/backend/service/market/`.
Candidate receipts below are retained local evidence, some outside tracked Git.
Their relevant findings are summarized here so readers need not follow chats.

The repository reading path, contracts, component docs, prior audit, latest owner
assessments, and targeted code/receipts were reconciled. No fresh production schema,
capacity, query, restore or deployment inspection was performed. “Implemented”
means source exists; “tested” describes named historical evidence, never automatic
proof for a different composition. Current deployment identity remains a gate.

```text
provider -> collector/raw spool -> canonical Fact revisions + payload storage
                                -> raw archive manifests and reference mappings
Fact repository + verified hot/cold readers -> frozen Dataset binding
binding + versioned recipe -> canonical state engine -> Check result / explicit Observation
existing research jobs -> admission, fenced ownership, terminal publication
existing storage lifecycle -> archives / explicit maintenance -> recovery evidence
```

The data/storage owner owns Fact and Dataset selection, normalization, archives and
physical recovery. The research owner owns scientific definitions, planning,
evaluation, protocols and result meaning. The async-job owner owns dispatch,
leases and publication. The release/operator owner owns exact deployment and host
admission. These are existing responsibilities, not new teams or services.

### Reuse, change, remove, defer

| Disposition and existing owner/code | State and problem addressed | First milestone; compatibility/recovery dependency |
| --- | --- | --- |
| **Reuse** collection, raw spool and references: Q `src/market_data/archive.py`, `portal/backend/service/storage/repos/collector_operations.py`, `fact_references.py` | Implemented; spool acknowledgements and provenance preserve captured evidence. Raw durability is provider/path dependent, not universal. | Preserve collection priority and acknowledgements; no collector rewrite. Existing recovery references and keys remain required. |
| **Reuse; qualify** canonical Facts: Q `market_data_models.py`, `market_data.py`, `src/market_data/fact_registry.py` | Implemented append-only revision and registered payload boundary. | Required; keep exact values, source identity, corrections and timing. No revival of retired semantic tables or dual writes. |
| **Change if reproduced; prove regardless** frozen selection: Q `market_data.py::freeze_dataset`, `src/market_data/frozen.py`, `backtest_dataset_service.py` | Implemented, serial correction/lifecycle tests exist; concurrent out-of-order commit concern remains untested. | Required; version any changed binding semantics. Never rewrite old manifests/results to make validation pass. |
| **Reuse** normalization and frozen candle derivation: Q `normalization_service.py`, `repos/normalization.py`; R `src/market_data/candle_derivation.py`, `service/market/candle_derivation_service.py`, ADR 0074 | Existing owners plus implemented/tested candidate capability; avoids fetching already derivable information. | Only where the selected recipe requires it. Pin source binding, transformation version, lineage and availability; missing source remains a gap. |
| **Reuse and compose** H04: R `src/research_science/check.py`, `src/indicators/candle_stats/runtime.py`, `service/research/{registry,forward_risk_evaluator,event_fact_evaluator,execution}.py`, ADR 0076 | Candidate implemented; 2026-09-29 qualification includes synthetic annual run and 132 DB passes, one explicit bootstrap skip. Definition 10/evaluator 9 are required by the prepared protocol. | Required for H04; no intrinsic storage schema change. Preserve readiness, clocks, finite seed, no-tail boundary and previous definition behavior. Requalify exact integrated source. |
| **Change narrowly** bounded preparation/read/evaluation: Q `market_data.py`, `fact_storage.py`, `service/research/{planning,execution}.py`, `runtime_market_data.py` | Implemented batch readers but whole-range list materialization remains. Batch size alone does not cap a run. | Required bounds; streaming changes only where admitted workload cannot fit. Preserve ordering and engine state across chunks. |
| **Reuse; extend** jobs: Q `service/async_jobs/repository.py`, `workers/research_worker.py`, `service/research/async_dispatch.py`; R `--single-attempt` route | Fencing/deduplication implemented; single-attempt candidate tested. Current component contract explicitly lacks mid-job cancellation and progress checkpoints. | Required attempt accounting, resource admission and stoppability; no new queue. Replay is presently synchronous and must obey the same admission envelope. |
| **Change after baseline** provenance lookup and cold page hydration: Q `repos/fact_storage.py`, `src/market_data/fact_archive.py` | Measured witness lookup bottleneck; repeated full-page verification/decoding is code-derived. | Conditional: needed if the milestone fails its budget. Preserve legacy predicate meaning and integrity checks. |
| **Reuse selectively; defer general caching** Check results, normalization outputs, reports: Q `service/research/{repository,result_reference}.py`, `repos/normalization.py`, `db/models.py` | Durable owners exist; universal result reuse/retention is not established. | Cache hits not required to start H04. No generic artifact catalog; independent replay bypasses result reuse. |
| **Defer as a research prerequisite** S header layout, global identities and series/day directory: `db/{market_data_models,fact_storage_schema,session}.py`, `repos/market_data.py`, `docs/engineering/fact-header-layout-v2.md` | Candidate implementation and qualification evidence exist; combined startup requires the new layout. Directory/identity growth and retained legacy data still have costs. | Not an H04 semantic dependency. Required only if choosing that exact combined release or actual capacity makes the existing layout inadmissible. |
| **Preserve; defer execution** S live migration, retained legacy-header placement and backup/recovery work: `scripts/db/{fact_header_v2_online,fact_header_forward_adoption}.py`, `service/storage/{header_movement,header_resource_claims,incremental_recovery}.py` | Implemented/experimental operational paths; latest capacity admission incomplete. Cancelled restore work is not a pass. | Do not resume, erase, or assume completion. A compatible research release still needs applicable recovery evidence for the data it uses. |
| **Remove from the proposed critical path** whole-history conversion, migration cleanup, universal precomputation and automatic post-gate restart | Historical sequencing or unselected proposals, not demonstrated scientific dependencies. | Remove dependency assumptions only. Delete no code, data, evidence, backups or automation configuration. |

### Two uncertainties that determine the implementation

**Frozen selection.** Q `market_data_models.py:266` allocates `market_commit_seq`
from a sequence; `market_data.py:1337` reads a global maximum; ingestion at
`:2609` serializes by series; freeze at `:3678` uses a lifecycle-protected snapshot;
reads at `:4330` and `:4405` reselect by range and stored sequence limit. A possible
schedule is: A allocates 100 for series X but does not commit; B commits 101 for Y;
freeze X records 101 without A; A commits; a later X read includes 100. This is a
**code-derived risk, not a reproduced incident**. Existing row/hash validation can
reject changed input, which is different from preserving readable membership.

The lifecycle fence addresses archive publication, not global ingestion order.
PostgreSQL documents transaction-local snapshots and nontransactional sequence
behavior; interpreting a maximum as a lasting committed frontier needs an
application proof ([transaction isolation](https://www.postgresql.org/docs/16/transaction-iso.html)).
Start with three disposable sessions and explicit barriers. Cover new Facts,
corrections, invalidations, multiple selected series, aborts, and archive lifecycle
overlap. `test_repository_db.py` serial correction cases and
`test_fact_storage_tiers_db.py` lifecycle tests are supporting cases, not this proof.

Preferred minimal fix, **only if that test and writer inventory justify it**:
record a closed frontier per selected series using existing Dataset-series
metadata. Prove every online writer allocates monotonically after the series lock,
including sequence cache behavior and multi-series transactions. Q's repository
insert is not the entire writer inventory: manual Fact import/copy scripts also
exist. Forbid concurrent bypass writers through their explicit operational
contract. Do not infer closure from `MAX`, timestamps, `last_value`, or cached
sequence allocation alone. If closure cannot be guaranteed, specify exact revision
membership owned by the Dataset boundary, with its measured size and publication
protocol, before implementing it. No membership table is approved speculatively.

**Release coupling.** S `db/session.py:1666-1687` requires primary key
`(id, storage_day)`; `db/fact_storage_schema.py:208-251` requires the identity
contract and particular indexes. Therefore deploying S unchanged on an older
layout is technically incompatible. H04 itself declares no physical schema
dependency. Earlier research dependency records allowed independent research
release; later packets bundled it with forward storage adoption. Prefer composing
the required research changes onto a base verified against the actual serving
schema. Revalidate all scientific versions and startup guards. Do not weaken guards,
invent a universal dual-layout ORM, or treat Q's checkout as proof of what serves.

## Implementation slices and dependencies

Order: **S0 -> S1 and S2 -> S3 -> bounded milestone admission**. S1/S2 may be
developed independently but both must qualify with S3 in the exact release.
S4 is pulled forward only for a measured budget failure; S5 follows evidence of
repeated expensive work; S6 is an independent, separately authorized storage path.
Each slice is a coherent review/commit, not a new service. “Rollback” below means
compatible application rollback; it never means deleting facts or restoring a DB
over newer collection automatically.

### S0 — Compose the smallest compatible research release

- **Behavior/example:** replace “H04 waits for the storage rollout” with an exact
  candidate/schema compatibility statement. Include definition 10/evaluator 9,
  existing frozen derivation only when needed, and the single-attempt route.
- **Owner/areas:** research and release owners; R scientific paths above, Q/R
  controller/CLI dispatch, existing deployment compatibility checks. No schema,
  public scientific API or physical format change merely to split the release.
- **Compatibility/non-goals:** preserve all existing versions and records. Exclude
  new storage-layout guards unless that layout is actually selected. Do not deploy,
  port the whole migration controller, or claim candidate receipts qualify a new SHA.
  H04 keeps unknown event counts unknown and eligibility specific to each dependency
  and horizon; late outcomes disclose their availability and never become earlier
  decision inputs. This preserves the candidate's scientific corrections.
- **Failure/recovery/cost:** a missing actual-state compatibility receipt blocks
  release; retain the existing runtime and exact images. Composition has review
  and CI cost. Cancel qualification without touching the serving system.
- **Done:** exact source/hash/version inventory; disposable fixture for the serving
  schema passes strict startup and selected scientific tests; relevant normal
  validation and compatible promotion/recovery rehearsal pass. Skips remain explicit.

### S1 — Prove and preserve frozen membership

- **Behavior/example:** after freezing X, a late commit or correction cannot enter
  that Dataset; a new freeze may include it. Run the test above first, then either
  record the proven existing guarantee or implement the smallest demonstrated fix.
- **Owner/areas:** data owner; `market_data.py`, `market_data_models.py` only if
  needed, `frozen.py`, Dataset validation and DB tests. Prefer existing per-series
  metadata; version the binding/selection algorithm explicitly when meaning changes.
  Exact membership persistence is a conditional design decision, not a second authority.
- **Compatibility/non-goals:** no full-history rewrite or global writer stop.
  Retain old binding validation/readers. Never recompute an old manifest's identity
  in place. If old membership cannot be recovered from retained evidence, mark that
  input unreplayable with context and create a new Dataset only for a new run.
- **Failure/recovery/cost:** publish manifest and pins atomically; cancellation
  before commit exposes no completed Dataset. Bound snapshot/lock duration and
  retries. Preserve source objects and existing manifests. Old code must reject
  unsupported new bindings; rollback cannot silently reinterpret them.
- **Done:** deterministic concurrent schedule tests, correction/abort cases, repeated
  reads and replay have identical membership and semantic hashes after later commits;
  lifecycle publication tests still pass. Measure freeze duration and collector impact.

### S2 — Bound the entire input and computation path

- **Behavior/example:** a five-year request first reports exact series/ranges,
  seed, required versions, gaps and estimated work. It executes only within its
  admitted envelope; otherwise it returns a concrete smaller-unit or higher-budget
  requirement. It must not quietly shorten dates, omit revisions, or drop observations.
- **Owner/areas:** data and research owners; repository range readers, frozen
  preparation, `planning.py`, `execution.py`, canonical runtime resolver and evaluator.
  Extend the existing plan/request with operational limits and measured counters;
  keep scientific identity separate from operational limits. No default schema change.
- **Implementation/compatibility:** cap rows/bytes before whole-range materialization.
  Where needed, page in stable semantic order with a unique tie-breaker under the
  pinned selection, hydrate bounded pages, hash incrementally, and feed one continuous
  engine timeline. Preserve pending outcome horizons, seed/readiness and gap policy
  across page boundaries. Declare any state that grows with the data; do not promise
  constant memory for every Indicator. Small bounded list paths can remain.
- **Failure/recovery/cost:** check cancellation/deadlines between bounded units;
  bound DB statement, lock wait and transaction duration too. A too-large result
  fails explicitly before publication. Initial recovery restarts the same pinned
  request only with a fresh authorized attempt; no partial engine checkpoints or
  independent monthly runs masquerading as one annual state history.
- **Done/non-goals:** equivalence on multiple page boundaries, empty/gapped/revised
  inputs and largest admitted fixture; measure selection, decoding, engine and
  serialization peaks including temporary buffers. No new streaming framework.
  Added counters and paging have complexity cost; retain the simpler path if it fits.

### S3 — Admit resources and make interruption explicit

- **Behavior/example:** two identical active requests return the existing job;
  an HTTP timeout returns uncertain completion to be reconciled by job identity.
  A heavy request cannot start merely because it owns a lease. Begin with one
  admitted heavy research operation at a time, including synchronous replay/preparation.
- **Owner/areas:** existing async-job/worker, research service, CLI and host admission
  owners. Reuse request fingerprints, claim generation and atomic job-owned effects.
  Extend existing request/status metadata for limits, counters and stop reasons;
  add a narrow cancellation operation to the existing job API/CLI if required.
  No second job table or queue. Worker-managed cancellation is currently missing.
- **Budget/priority:** declare maximum wall time, RSS, DB connections, statement and
  lock time, I/O/temp/write allowance, output size and attempts before dispatch.
  Also declare collector lag/spool and free-space stop conditions from measured
  baseline and actual host reserve. Reserve collection capacity first. Coordinate
  admission atomically in existing job/maintenance owners; a lock is only the
  serialization mechanism, not the capacity decision. Unknown headroom denies
  heavy work. No overlap with unadmitted maintenance. Throttle/stop research first.
- **Cancellation/publication:** fence cancellation against completion under existing
  row ownership rules; return which terminal outcome won. Use an explicit terminal
  failure/stop reason rather than success with partial evidence; stop retries.
  Workers check stop/lease loss at S2 boundaries; statements have deadlines. Test
  process termination separately from publication fencing: a stale worker unable
  to commit may still consume resources. Do not release its resource reservation
  until execution is confirmed stopped or conservatively unavailable.
- **Compatibility/recovery/cost:** H04 uses `max_attempts=1`; count claims conservatively.
  Restarting a failed computation consumes a new protocol attempt. Query the existing
  job after lost responses before submitting again. Synchronous replay uses the same
  admission/cancellation boundary and an explicit client deadline; no new replay
  endpoint is required if that service can enforce the bounds. Polling/limits add
  small overhead; serial admission sacrifices throughput for predictable collection.
- **Done/non-goals:** duplicate dispatch, cancellation/completion race, stale claim,
  crash before/after commit, lost acknowledgement, collector pressure and exhausted
  budget tests. Verify no duplicate durable effects and no unaccounted execution.
  No distributed scheduler or resumable arbitrary engine checkpoint system.

### S4 — Fix demonstrated read amplification, without changing meaning

- **Behavior/example:** the same provenance witness or selected cold facts return
  identical results while avoiding redundant scans or repeated page decoding.
- **Owner/areas:** storage repository `fact_storage.py`, `fact_archive.py`, existing
  provenance indexes and targeted tests. First separate selection, verification
  and hydration. Prefer a semantically equivalent predicate or byte-capped,
  request-local verified-page reuse keyed by immutable object identity/checksum.
- **Changes/non-goals:** no new persistence by default. An index is a separately
  reviewed schema/operator change with measured write, WAL, space and backup cost.
  Preserve all historical witness key/value forms, including numeric-looking values;
  cold catalog aliases remain locators, not proof. No disabling integrity verification.
- **Compatibility/recovery/cost:** existing format readers remain; reject corruption
  loudly. Dropping an in-memory page cache simply repeats verification. Roll back
  query changes independently of data. Cache memory is part of S3's cap.
- **Done:** old/new predicates agree on positive, negative and legacy fixtures;
  hot/cold identities, gaps, revisions and timing match; paired W1/W3/W4 measurements
  improve the targeted cost without material collector regression. Conditional for H04.

### S5 — Reuse one proven expensive computation

- **Behavior/example:** an identical admitted request explicitly reuses a verified
  frozen derivation or completed Check reference; a changed source revision,
  Indicator version, parameter, seed or clock policy misses. A requested independent
  replay always recomputes and records a distinct attempt.
- **Owner/areas:** existing normalization or candle-derivation producer for its
  derived Facts; Check repository/result reference for completed scientific results.
  Use existing records and versioned recipes.
  Selection key includes binding/selection version and hashes, source revisions,
  graph/Indicator and evaluator versions, parameters, clocks/gaps/readiness, seed,
  range, numeric/output format and relevant execution configuration.
- **Changes/non-goals:** expose reuse origin and verified identity in existing result
  metadata; keep scientific output hashes independent of the hit receipt. A stored
  Indicator-output cache is deferred until repeated engine cost justifies it; then
  its existing owner must specify a bounded format and retention before adding it.
  No generic artifact catalog, broad feature store or speculative precomputation.
- **Compatibility/recovery/cost:** never mutate a completed result or old recipe.
  Missing/incompatible cached output is an explicit miss; corrupt pinned evidence
  is an error. Compute/publish atomically through existing fencing. Retain referenced
  results and input pins; disposable cache eviction must not destroy replay evidence.
  Count cache/catalog bytes and lookup cost, including misses, against the budget.
- **Done:** exact-hit and all invalidation cases, concurrent publishers, independent
  replay bypass and corrupt/missing output tests; W5 repeat measures avoided work
  and extra bytes. Optional after the first milestone, unless budgets require reuse.

### S6 — Evolve physical representation only for a demonstrated need

- **Behavior/example:** an old-format cold page remains readable. If conversion is
  justified, an explicitly requested bounded storage job writes and verifies a new
  representation before publishing it. Ordinary history reads never initiate it.
- **Owner/areas:** existing archive, storage lifecycle, manifest and maintenance
  owners. Reuse their job identity, leases, receipts and pinning; extend a format
  version/decoder only where necessary. No general conversion framework.
- **Compatibility/publication:** key a conversion by source manifest/checksum,
  target format and algorithm version. Duplicate agents join the same owned work.
  Read existing pinned objects while a replacement is prepared. Publish verified
  replacement references atomically with a generation check; preserve logical
  identity, exact numeric content, clocks and revision ordering. Unsupported formats
  fail with the explicit preparation action, never hidden work.
- **Failure/recovery/cost:** before publication, an incomplete target is unreachable;
  after an uncertain commit, inspect the publication receipt before retrying.
  Cancel leaves the source authoritative. Retain old objects/readers while any run
  or recovery point needs them. Reclamation is separately authorized after pin and
  backup checks. Budget source plus target, indexes, WAL, temporary and backup bytes;
  do not credit hypothetical future reclamation as free space.
- **Done/non-goals:** old/new and hot/cold equivalence, read-during-conversion,
  duplicate job, stale owner, crash/cancel on either side of publication, restore of
  both representations and low-space tests. No full-history conversion prerequisite.
  Date partitions can bound lifecycle work, but storage day is not observation
  time; prove query pruning from actual predicates. Partitioning also changes
  uniqueness constraints ([PostgreSQL partitioning](https://www.postgresql.org/docs/16/ddl-partitioning.html)).
  Do not add a global identity catalog or per-series/day directory solely for H04.

## Which changes need which mechanism?

| Change | Minimum mechanism and historical behavior |
| --- | --- |
| A. New Indicator over existing Facts | Versioned Indicator/Check recipe through the state engine. Compute a bounded window; no source schema migration. Persist selected expensive outputs only after S5 evidence. |
| B. New provider or information | Existing provider/Fact registration, explicit acquisition and provenance. Optional fields may fit a registered payload version; new meaning needs a new contract/version. Uncollected history is missing, not reconstructible by conversion. Any external backfill needs its own scope/budget. |
| C. Lossless physical representation | Compatible reader first; optional S6 conversion. Prove semantic hashes and numeric/timing equivalence. No scientific version change solely for identical physical bytes represented differently. |
| D. Semantic correction or recomputation | Append correction history or versioned normalized/derived output with true availability and lineage. Explicit bounded recomputation; old runs remain bound to old inputs/recipe. Distinguish provider historical availability from local acceptance time. |
| E. Canonical schema meaning | Review/update the owning contract and ADR; explicit schema version, compatibility window and operator cutover/backfill only for necessary scope. Runtime never patches schema. This is outside ordinary autonomous research authority. |

## Performance qualification: measure phases, not one headline

Saved evidence is a baseline lead, not an end-to-end performance promise:

| Evidence and locator | Measured fact and limitation |
| --- | --- |
| S `artifacts/storage-implementation/bip-delay-statement-deltas-20260924.json:3-7` | 25 provenance-witness calls: 58.871 s total, 2,354.83 ms mean, 1,085,234 read blocks. Statement aggregate, not whole-request latency or current load. |
| R `artifacts/five-year-research/year-scale-20260929/calendar-coverage-packet.json:200-201` | Annual coverage request: client 46.5 s; server about 125 s before termination, no completed result. Timeout is not a latency baseline for success. |
| R `artifacts/five-year-research/landmark-dependence-20260924/L02-preparation-progress-20260929.json` | Saved BTC preparation operations: source freeze 48.2 s; 5m derivation 38.1 s; 30m derivation 29.4 s; final preparation/freeze 85.5 s. These are operation totals; do not add them as independent phases without proving disjointness. |
| R `artifacts/five-year-research/year-scale-20260929/H04-qualification.json:36-76` | Synthetic 525,799 source rows: engine 383.652 s, total 428.760 s, peak RSS 2,332,000 KiB, result JSON 1,072,498 bytes. Excludes DB reads, freeze, persistence and replay; not market evidence. |
| S `artifacts/storage-implementation/migration-release-20260926/completion-forward-capacity-inventory-20261003.json` | Recorded DB bytes 1,123,252,966,191; header relation 316,197,470,208 including about 150.7 GB listed indexes. Inventory includes retained migration copies; admission incomplete. No fresh capacity claim or cleanup credit. |

Run the following later on disposable/sampled inputs first, using the smallest
fixture that exercises the real plan and integrity path. Never flush production
caches, run full production scans, or use `EXPLAIN ANALYZE` on unbounded statements.
Record exact source/schema, dataset/recipe, device, concurrency and cache condition.
Use plan-only inspection before a bounded timed execution. Expand to the intended
range only after resource admission; a cancelled attempt remains in the ledger.

| Workload | Smallest experiment, phases, and decision resolved |
| --- | --- |
| W1 Recent series/time | One declared series and day, then the real recent request shape. Record selected rows/revisions, plan/pruning, buffers and hydration separately. Determines whether predicate/index work is needed; no assumption date partitions help. |
| W2 Long frozen preparation | Multi-series correction/gap fixture, then one representative month; qualify year and five-year planning/limits incrementally. Separate header selection, manifest/hash construction, payload access and commit. Resolves frontier-query cost, transaction duration and maximum admitted range. |
| W3 Cold history | One verified archive page, then a range crossing pages and hot/cold boundary. Separate object reads/checksum, decoding and filtering; count repeated page loads, read amplification and memory. Resolves page reuse versus format/granularity changes. “Cold format” and “cold OS cache” are distinct conditions. |
| W4 Provenance witness | Small fixture reproducing positive/negative and legacy JSON predicates, then a capped representative sample. Compare equivalent queries/plans, blocks, returned identities and CPU. Resolves predicate/index need and its write/space cost. |
| W5 H04 and identical repeat | Reuse saved synthetic recipe for qualification; later run the exact frozen BTC2022 request once and admit its independent replay separately. Time source preparation/selection, hydration, decode, engine, serialization and persistence. When S5 exists, additionally repeat with reuse enabled and identify which phases were avoided. Cache hit is not replay. |

For each, capture wall/CPU time, peak process RSS and relevant worker/container
memory, rows, logical bytes, DB blocks, archive bytes, temporary files, output bytes,
and write/WAL deltas where applicable. Explain shared-cache/disk-counter limitations;
do not call decoded bytes physical disk reads. Report nonoverlapping phases and
end-to-end time, including queue/admission wait. Record cache hits/misses and the
work avoided. Include idle/baseline and concurrent collector lag, spool growth,
ingestion throughput, DB waits/connections and disk headroom. Never add a database
total to its component relations or count migration-retained copies as useful facts.

Use paired baseline/candidate requests with identical semantic inputs and a declared
repeat count. Report all small-sample timings and variation; use percentiles only
with a sufficient sample. Correctness hashes must match. A performance claim needs
improvement beyond observed variation within the approved resource envelope and
collector tolerance. **Numerical latency, RSS, I/O and lag acceptance limits are
unset until the baseline and host capacity are admitted**; filling that signed-off
envelope is a gate, not permission to choose generous thresholds after a failure.

Growth checks are explicit scenarios, not forecasts: a five-year one-minute series
has roughly five times one year's rows before corrections/seed; memory or runtime
need not scale linearly. Multiple series, correction density and outcome horizons
increase work independently. Retaining K recipe outputs across N windows adds their
actual encoded bytes plus result metadata/pins; it is not free because source facts
are shared. Compare that storage cost with measured repeated engine time before S5.

## Research-resumption acceptance matrix

Statuses below are as of this specification. Historical partial evidence is
preserved, but **no production resumption is certified here**. Gate receipts must
name source/hash, compatible schema/reader versions, test inputs, limits, result,
date and owner. A failed or unavailable applicable gate stays open.

| Gate / requirement and why | Evidence and pass criterion | Current status | Responsible owner / blocked capability |
| --- | --- | --- | --- |
| G0 Exact compatible research release | Serving layout identified; exact candidate starts against its disposable equivalent with strict guards; required H04 versions and single-attempt behavior verified; applicable release checks pass. | Candidate tests exist; independent composition and current deployment unverified. | Release + research / H04 on the serving system. |
| G1 Frozen concurrent selection | S1 barrier tests pass for delayed commits, corrections, aborts and multiple series; repeated membership/hashes unchanged; old invalid bindings fail explicitly. | Code-derived risk; not reproduced or cleared. Serial/lifecycle tests are insufficient. | Data / new durable frozen research and dependent replay. |
| G2 Timing, gaps and one engine | Known-at prefix invariance, seed/reset/no-tail boundaries, gap/readiness, correction history and page equivalence pass; H04 eligibility and late outcome disclosure match its versioned protocol. | Candidate scientific tests exist; require exact composed-source qualification. | Research + data / scientific validity and promotion of evidence. |
| G3 Supported representations | Hot/cold and every format actually selected by the run give equivalent logical inputs, clocks and provenance; corruption/missing-object cases fail loud; required pinned objects/readers remain available. | Existing tier tests; actual selected inputs and composed readers unverified. | Storage / reads using those tiers or formats. Unused future formats do not block hot-only research. |
| G4 Bounded end-to-end resources | W1-W5 relevant fixture/baseline receipts fill the declared envelope; the largest admitted unit fits; collection remains within agreed lag/spool limits; pressure prevents starts or stops work within the declared bound. | Whole-range allocations and historical timeouts observed; fresh host envelope and stoppability absent. | Research worker + data + host operator / unattended heavy preparation, computation and replay. |
| G5 Ownership, cancellation and uncertain completion | Concurrent duplicates share one active job; stale owners cannot publish; cancel/completion has one terminal winner; crash/timeout reconciliation avoids duplicate effects and accounts every attempt; resource stop confirmed. | Fencing/dedup tests and candidate single-attempt test exist; full cancellation/resource proof missing. | Async-job + research / unattended dispatch/retry/cancellation. |
| G6 Protocol, holdout and attempt admission | Revalidate existing Study/Indicator identity, evaluation interval `[2022-01-01T00:00Z, 2023-01-01T00:00Z)`, 200-bar seed starting 2021-12-31T20:40Z, no tail at/after the end, definition 10/evaluator 9, frozen inputs, registered analysis and current budget. Exactly one Check plus a separately admitted replay; no date substitution. | Prepared packet/registered identities exist; input eligibility, current remaining budget and compute window need readback. | Research / first market H04; ETH/follow-ups require their own existing protocol eligibility. |
| G7 Applicable recovery and release compatibility | Exact candidate's applicable normal matrix and promotion/recovery rehearsal pass; preserve compatible prior image/config and usable DB+archive recovery dependencies for selected inputs. Pin/restore tests retain old runs through a representation change when one is included. | Prior receipts cover particular candidates. Cancelled full restore is not passing evidence; actual release/recovery admission remains open. | Release + storage / serving deployment; representation replacement if selected. No requirement to finish unrelated migration or full-history cleanup. |
| G8 First-run interpretation and replay | The admitted first Check completes within its envelope; independent replay reproduces semantic output and input hashes; coverage, strata, concentration, null/reversal and competing explanations are reviewed before further protocol steps. | Not run; synthetic fixture is not a market Check. | Research / continuation beyond the first admitted Check and replay. |
| G9 Optional reuse or conversion | S5 hit/miss/replay-bypass tests or S6 equivalence/publication/crash/restore tests pass for the enabled feature, with measured resource benefit/cost. | Proposed or partial candidate evidence, not generally admitted. | Owning boundary / only reuse or conversion being enabled. |

**Activation order avoids a circular gate:** G0-G7 admit the first bounded Check
and its separately budgeted replay after explicit activation. G8 determines whether
the prepared protocol may continue. G9 is conditional. Engineering may qualify
fixtures without claiming a real market trial. All paused research and storage
automation stays paused during this documentation task.

Preserve the prepared identities: Study `188ce199-0eef-4f6a-ba43-9318902736a5`,
parent link `e279f8cd-12c0-4dc6-96a6-310db86381e2`, and Candle Stats v1 instance
`3ae988f9-1158-44ef-8a6d-7920974cd7a4`. Read back their current meaning and budget
before activation; do not recreate them. The source is R
`artifacts/five-year-research/year-scale-20260929/H04-execution-packet.json`, with
single-attempt behavior qualified separately in `single-attempt-db-receipt.json`
(one passing disposable DB test at R's pinned revision). These are evidence of
existing work, not renewed operational permission.

After explicit activation and the applicable gates, autonomous research may use
`qt` to inspect requirements, freeze already collected eligible inputs, apply
authorized existing normalization recipes, run provider-free Checks, and perform
required independent replays within the registered symbols, dates, holdouts,
hypotheses, attempt and resource budgets. It may read/reuse eligible results and
record Observations/Study links only through existing evidence/authority rules.
Cache hits do not create independent scientific evidence or replenish trial budgets.
Changing code/recipes requires their normal review and requalification, not hidden
self-modification by the research loop.

Autonomy stops at budget exhaustion, ambiguous completion, missing inputs, failed
integrity/eligibility, or collector pressure. It must not acquire new provider data,
expand holdouts, change canonical schemas, deploy, convert storage, delete data,
modify backups/keys or grant execution authority under this admission. Such work
needs its own authorization and applicable owner checks. Readiness is not authority.

## Stress cases and operational consequences

| Situation | Required outcome |
| --- | --- |
| Indicator requests five years | Plan exact available history, seed/gaps and work first. Read supported formats and compute boundedly if admitted. Refuse with a scoped requirement otherwise; no automatic migration or fabricated history. |
| Provider adds/changes a field | Preserve old payload/version and source evidence. Register genuinely new meaning; old runs retain their decoder. Explicitly collect/backfill only if authorized and historically available. |
| Two agents request conversion | Existing storage job ownership deduplicates source/target identity; one publisher wins. Resource admission is still required for that job. |
| Research reads while replacement prepares | Continue on the pinned immutable source or a proven equivalent published representation. Publication cannot invalidate an active reader's object lifetime. |
| Crash halfway through conversion/publication | Unpublished target is not readable truth; inspect durable receipt for uncertain publication. Resume/retry only the bounded job under current ownership; retain source and pins. |
| Old run after a format/recipe changes | Use its pinned binding and supported reader/recipe. Fail explicitly if required evidence is unavailable; do not regenerate under new semantics and label it the same run. |
| Low disk or collection lag | Deny new heavy work and throttle/cancel admitted research/maintenance by declared limits. Keep incomplete outputs unpublished and reserve collection/spool capacity. A lease or a migration deadline cannot override that decision. |

## Delivery, open decisions, and documentation discipline

Immediate implementation is S0-S3, with reproduction and measurement before fixes
whose necessity is uncertain. Near-term S4 and S5 address demonstrated read/repeat
costs. S6, wider precomputation, archive repartitioning, global catalogs and hardware
changes require evidence from the relevant workload/capacity gate. Do not build a
new database, distributed platform, generic artifact service or universal migration
controller for this milestone.

Material decisions still requiring evidence: exact serving schema and release
composition (G0); sequence/writer closure and any new binding version (G1);
largest admissible window and stopping latency (G4); predicate/index versus cold
page reuse (W3/W4); full-identity Check reuse versus a specific stored Indicator
output (W5); actual recovery dependencies and headroom if storage changes (G7/S6).
No unsupported performance target, new persisted catalog, or full-history job is
implicitly approved by leaving these questions open.

Implementers update these slice/gate statuses with concise evidence links and exact
revisions. Preserve operational receipts as history, including superseded release
sequencing; do not rewrite them into commands for this plan. Update contracts only
if meaning changes; update the relevant component document when behavior actually
ships. ADR 0078 remains proposed until its enforcement is qualified. This ADR and
specification are the central design record; no additional workflow manual is needed.

Documentation validation for this change is architecture-index generation,
`make validate-docs`, `make sync-docs`, and `git diff --check`. Runtime implementation
slices require focused tests followed by the applicable normal validation matrix
in [developer workflow](developer-workflow.md); schema/repository changes require
the disposable DB suite. Unavailable prerequisites and skipped checks are never
passes. No heavy performance experiment or production operation belongs to this
documentation task.
