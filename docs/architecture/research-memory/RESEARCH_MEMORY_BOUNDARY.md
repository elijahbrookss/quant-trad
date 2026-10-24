---
component: research-memory-boundary
subsystem: research-memory
layer: boundary
doc_type: architecture
status: active
tags:
  - research
  - memory
  - observations
  - hypotheses
  - studies
  - checks
code_paths:
  - portal/backend/controller/research.py
  - portal/backend/service/research/async_dispatch.py
  - portal/backend/service/research
  - portal/backend/workers/research_worker.py
  - portal/backend/service/async_jobs
  - portal/backend/service/indicators/indicator_service/runtime_validation.py
  - portal/backend/service/reports/contract.py
  - portal/backend/db/models.py
  - portal/backend/db/session.py
  - cli/main.py
  - scripts/db/manual_migration_research_memory_v1.sql
  - scripts/db/manual_migration_async_job_fencing_v1.sql
---
# Research Memory Boundary

## Purpose

Research memory captures why Quant-Trad users care about an idea and what the
system has already learned about it. It stores observations, lightweight
research checks, hypotheses, studies, and links to existing platform artifacts.

Research memory is not runtime truth. It is the reasoning trail around runtime,
data, indicator, experiment, report, and strategy artifacts.

Durable Check behavior is defined by
[Check Evidence Boundary](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md).
Research memory persists and links that evidence; it does not calculate a
parallel result.

Actual research-memory records are private QT-held content under the
[public infrastructure/private research contract](../../contracts/platform/00_system_contract.md#public-infrastructure-and-private-research).
This document describes reusable schemas, interfaces, and procedures; it does
not authorize copying record contents into the public repository. Interpretation
publication and saved-record export retain their existing QT meanings and do
not grant public-release permission.

## Boundary Contract

Research memory may:

- store observations, hypotheses, studies, and research checks,
- link research items to strategies, variants, indicators, instruments, runs,
  reports, experiments, and other research items,
- coordinate explicit preparation through the existing data boundary,
- run currently admitted raw-candle and event-and-fact Checks through their
  registered evaluators and canonical Indicator/Fact inputs,
- queue supported long-running operations through the shared async job store,
- persist check outputs as evidence items,
- recommend whether an observation should be discarded, refined, or promoted to
  a hypothesis.

Historical report/audit/sweep routes may remain as compatibility interfaces;
their presence does not make a retired procedure executable. Current registry
admission owns that decision.

Research memory must not:

- execute trades,
- simulate fills, fees, wallet, margin, slippage, or settlement,
- become a strategy engine,
- read mutable indicator internals,
- treat overlays or debug details as strategy evidence,
- fetch provider data outside existing data-boundary services,
- reconstruct report truth or runtime truth.

## Static Check and Study images

`qt research images attach <item_id> --file chart.png --request-json image.json`
uploads bounded static PNG/JPEG bytes into `portal_research_images` in the existing
PostgreSQL database (`PG_DSN`). `image.json` requires `title`, `explanation`,
`alt_text`, and `binding`. For a durable Check, binding contains its exact
`result_hash` and `evidence_hash`; the server retains definition, request, plan,
input and frozen Dataset/run identities from the validated Check. For a Study,
binding contains an existing `publication_hash`; the server retains that
interpretation revision and its exact evidence reference identities. An image
can explain an earlier revision after a later interpretation is published.

Admission verifies decoded image structure and pixels, limits each image to
4 MiB, 24 megapixels and 12,000 pixels per side, and limits each item to 16 images
and 32 MiB. Identical bytes/text/binding retries return the existing image.
Corrections append a new image; no public update or deletion operation exists.
The original item, Check hashes, Study publications and relationship history are
unchanged. Presentation custody establishes source attribution, not correctness
of plotted values, new scientific results, replay or certification.

`qt research images list <item_id>` returns metadata and exact bindings without
binary content. `qt research images download <item_id> <image_id> --output chart.png`
retrieves server bytes and requires a new destination file. HTTP operations use
`/api/research/items/{item_id}/images` and `/{image_id}/content`; content reads
recheck evidence/access and SHA-256 integrity. Sealed evidence remains restricted.
The v2 research evidence room displays images beside their explanations with
expandable source identities; all browser operations remain read-only.

Image bytes are database custody, covered by the existing database backup and
restore boundary; deleting a source computer's file does not remove them. The
ORM defines clean provisioning; optional explicit provisioning SQL is retained
at `scripts/db/manual_migration_research_images_v1.sql`. Existing deployments
still require separate qualification and operator deployment authorization.
No migration or deployment is performed by upload.

SVG, animated images and HTML are rejected. No isolated interactive HTML serving
contract exists here; adding one requires a separately reviewed isolation and
credential-free serving boundary. This capability adds no renderer framework,
Check procedure, provider acquisition, research dispatch or concurrency setting.

## Research Memory Text Admission

Item creation and explicit Check-to-Observation admission accept an optional
body of at most 8,192 characters, matching the existing persistence column.
Their HTTP request contracts reject an oversized body with a field-specific
422 response before invoking the service. Accepted text is never truncated;
non-ASCII characters count as characters, not encoded bytes. Keep long Study
material in its structured payload or linked evidence and use the body for a
readable summary. This does not change stored records or database schema.

## Checks and background work

A Check has an explicit versioned definition, normalized request and declared
inputs. Preview remains ephemeral. Durable evidence uses frozen inputs and
retains definition, plan, source, quality and result identities. Only the
separately admitted Check-to-Observation operation creates an evidence-backed
finding; saving a Check does not create one automatically.

The [Check evidence boundary](../research-orchestration/CHECK_EVIDENCE_BOUNDARY.md)
owns preparation, analytical execution, gap handling, frozen binding, result
classification and replay availability. Discover supported procedures through
the code-owned catalog. Historical records and dormant compatibility routes do
not imply a current executable method or qualify new evidence.

Long-running operations use the existing
[async job boundary](../research-orchestration/RESEARCH_ASYNC_JOB_BOUNDARY.md).
Jobs retain the owning request, claim/fence, attempts, cancellation and result
contracts; research memory stores and links their admitted scientific output.
A job receipt, successful dispatch or terminal status does not independently
certify the result. Temporary sweeps remain previews and cannot bypass durable
Check admission or create accepted findings automatically.

## Research Metric Presentation

Research-memory presentation surfaces, including compact comparisons and future
leaderboard-style CLI/API views, are generic read models over emitted research
metrics. They are not indicator-specific analysis layers.

A presentation surface may group, rank, and display check outputs only through
explicit metric and dimension contracts. The rank metric must be supplied by
the caller or declared by the producing check contract. Comparable metrics
should carry enough semantics for generic handling: name, value, optional unit,
optional direction, role, dimensions, sample count, caveats, and provenance.

Presentation surfaces must not import indicator-family code, hardcode
family-specific meanings, infer signal quality from labels alone, or choose a
fallback rank metric when intent is missing. Missing rank keys, metric
directions, grouping fields, or required dimensions fail loud before presenting
misleading evidence.

Saved Check comparisons expose each result's emitted `descriptive_outcomes`
and `outcome_resolution` alongside its timeframe. These projections preserve
population definitions, unresolved values, horizon kinds and inference limits
without recalculating events or treating absent values as zero. Legacy
`forward_summary` deltas still apply only to legacy `outcomes.summary` metrics;
the descriptive projection does not create paired-population, common-clock or
causal-effect statistics. Those calculations remain Check-owned work.

## Memory Graph

The storage model is intentionally small:

- `portal_research_items` stores observations, research checks, hypotheses, and
  studies.
- `portal_research_links` stores directed links from a research item to another
  research item or platform artifact.

Every Research Check is a research-memory item. New V2 Checks link directly to
their exact Dataset or run evidence and may exist without a Research
Observation. A completed frozen Check may support a new Observation only
through the explicit admission operation defined by ADR 0065. Historical V1
Checks retain their existing `tests` links and automatically created
Observations as compatibility-readable evidence; those records are not upgraded
or used as the model for new writes.

Useful relations include:

- `tests`
- `supports`
- `derived_from`
- `supported_by`
- `contradicted_by`
- `promoted_to`
- `validated_by`

## Invariants

- Research memory owns reasoning and evidence relationships; runtime owns
  decisions, execution and accounting facts.
- Every durable finding preserves its actual input, quality, method and result
  classification. A manual note or legacy payload cannot masquerade as calculated
  frozen evidence.
- Provider access, Indicator state and analytical calculations stay with their
  existing owners. Memory does not reconstruct outputs from charts or logs.
- Metric presentation uses emitted contracts and explicit rank intent rather
  than indicator-family logic or guessed ranking.
- Admission failure remains explicit. It never falls back to an automatic
  Observation, a preview result or a silently substituted method.
- API publication and export are QT operations over private records, not public
  repository release approval.

## Related Docs

- [Research orchestration boundary](../research-orchestration/RESEARCH_ORCHESTRATION_BOUNDARY.md)
- [Data boundary](../data/DATA_BOUNDARY.md)
- [Indicator runtime boundary](../indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md)
- [Reporting boundary](../reporting/REPORTING_BOUNDARY.md)
- [ADR 0034: Use Research Checks as Analytical Memory Evidence](../decisions/0034-use-research-checks-as-analytical-memory-evidence.md)
- [ADR 0037: Keep Research Presentations Metric-Contract Driven](../decisions/0037-keep-research-presentations-metric-contract-driven.md)
- [ADR 0062: Use Frozen Bindings For Durable Check Evidence](../decisions/0062-use-frozen-bindings-for-durable-check-evidence.md)
- [ADR 0065: Use Explicit Frozen-Check Admission For New Research Observations](../decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md)

## Question-owned interpretation publication

A Research Memory Study may be explicitly adopted as a question using
`POST /api/research/items/{id}/question` with `question` and `scope`. No executable StudyDefinition is part of this boundary. A
hypothesis remains a testable claim; an Observation remains a finding under its
existing admission classification. Neither adoption nor publication grants
execution permission, changes scientific accounting, or certifies causality,
statistical validity, profitability or trading authority.

The existing Study JSONB owns `question_contract` (`research_question.v1`).
Generic item creation cannot write that reserved envelope. Existing Study IDs,
prior payload fields and scientific records remain untouched. Unadopted Studies
are `legacy_uncontracted`; adoption is explicit and idempotent and makes no
retroactive preregistration claim. Runtime performs no adoption or backfill.

`POST /api/research/items/{id}/publications` appends a published interpretation
revision containing conclusion, limitations, scope and exact required references.
A reference names a Research Memory target ID, expected kind and role
(`supports`, `contradicts`, `context`, `decision`, `deviation` or
`replay_dependency`). Check references require exact result and evidence hashes;
the service validates the canonical frozen Check contract and retained Dataset
identity without executing a Check or replay. Other reasoning references require
the content hash returned by the item read and preserve the exact hashed content
snapshot. An adopted Study snapshot includes its question and prior interpretation
history; later source publications cannot change that citation. Nested histories
count toward the same 1 MiB publication bound and are never truncated to fit.
Local paths and caller-provided result envelopes are not evidence locators.
Additional decisions or deviations can be retained in existing Study/Observation
records and referenced by exact content identity; no new artifact platform is
introduced.

Publication retains selected graph relationships and target relationship snapshots
as they existed at publication. Later claim, finding, or link edits cannot alter
that history. New findings or corrections require another revision; previous
publication hashes remain citable. Canonical Check payloads are referenced rather
than copied. Publication does not assert that an actual replay or recovery ran.
Completeness means `reference_complete_interpretation` only.

A PostgreSQL row lock serializes publication for a question. Each request includes
a request ID and expected previous publication hash (null for the first revision).
Matching retries return the original revision; conflicting retries and stale
writers fail. The complete append occurs in one transaction, so interruption
cannot leave a partial completed revision. History is bounded to 1 MiB per
question and 100 references per revision; reaching the bound rejects the write
without discarding history. Larger histories require an explicit future storage
decision, not silent truncation. Sealed Dataset identities in private scientific
protocol manifests are denied at publication; protocol custody remains authoritative.

API publication history, `qt research question history <id>`, and V2 Study
inspection expose the same stored revisions and completion meaning. CLI adoption
and publication use `--request-json` and the shared `ResearchOperations` adapter.
Draft prose remains flexible outside the reserved envelope.

### Compatibility and capacity

Adopt a legacy Study only through an explicit question/scope request. Identical
adoption is idempotent; conflicting adoption requires resolution. Do not
reinterpret Check hashes or refund scientific attempts. An older application
may read the JSON payload without supporting publication operations; code
rollback does not retract stored citations or authorize their deletion.

At the 1 MiB history bound, publication fails atomically with
`question_publication_history_limit`, preserving existing citations. Do not split
a question merely to bypass capacity. A larger storage design requires a
separately reviewed compatibility and migration contract.

Frozen market-data Check references also revalidate retained source material
through the existing frozen-series data owner. Existing report-bound Checks
instead resolve their exact retained run-report binding through the report owner;
no Dataset semantics are forced onto them and RunReportDTO does not own the
interpretation. Published references retain the Check owner's assurance
classification. Small reasoning snapshots explicitly remain
`reasoning_snapshot_not_calculated_evidence`, including manual/legacy findings;
a copied result inside a note does not acquire canonical calculated authority.


## Compact inspection and saved-record bundles

The v2 browser uses bounded SQL metadata projections for Study inventory and
aggregate coverage; it does not calculate scientific outcomes or infer
independent evidence from record counts. Exact interpretations retain their
existing immutable revision/hash and reference snapshots. Compact dependency
checks defer sealed access decisions to current protocol custody.

Read-only export pins the selected publication and reference identities,
includes exact Check payloads and historical reasoning snapshots, and reuses
native image custody. SHA-256 manifests support saved-record verification only.
No separate raw-source or OHLCV files are acquired or included; exact saved
record payloads remain preserved and their source completeness is not certified; raw-input recomputation and actual
replay are not claimed. Existing encoded reports remain original JSON payload
content until an explicit typed extraction mapping is reviewed. No schema,
admission rule, historical definition/result, publication history or image
identity is changed. See [Frontend V2](../frontend/OPERATOR_CONSOLE_V2.md).
