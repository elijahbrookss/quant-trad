# QT Platform Glossary

This glossary standardizes the words QT uses across data, research,
execution, reporting, and operations. Start with the plain-English
definition, then follow the linked contract or architecture decision when
you need the exact behavioral boundary.

The glossary does not create product behavior on its own. Platform contracts
and accepted architecture decisions remain authoritative when a short
definition needs more detail.

## Start Here

| Idea | QT wording |
|---|---|
| Stored market truth | A [Canonical Fact](#canonical-fact) is one immutable provider-neutral observation. A correction is a new [Fact revision](#fact-revision) with the same [Observation key](#observation-key), not a rewrite. |
| Reproducible source evidence | A [Frozen Dataset](#frozen-dataset) identifies exact Fact revisions; a [Frozen Market Data Read Binding](#frozen-market-data-read-binding) identifies the exact slice a consumer used. |
| Measurement | An [Indicator](../../architecture/indicator-runtime/INDICATOR_RUNTIME_BOUNDARY.md) advances causally and publishes typed outputs. |
| Research question | A [Check](#check) is bounded analysis. A [Check preview](#check-preview) is exploratory and cannot silently become durable evidence or a [Research Observation](#research-observation). |
| Decision logic | A [Strategy](#strategy-definition-compiled-strategy-effective-strategy-run-strategy-snapshot) consumes typed Indicator outputs and creates decisions; it does not own fills, wallet state, or execution truth. |
| Execution event or record | The informal phrase **execution event** must resolve to a precise owning type: decision, canonical order transition, fill, wallet-ledger fact, or runtime event. These records are not interchangeable. |
| Inspection and reporting | [RunResearchDataset](#runresearchdataset) is the canonical run-level reporting read model. Reports and BotLens projections explain durable truth; they do not create it. |

## Terms

| Term | Meaning |
|---|---|
| [Canonical Fact](#canonical-fact) | One immutable, typed, provider-neutral, dataset-eligible market observation. |
| [Fact revision](#fact-revision) | Immutable stored revision of one logical observation; correction appends a revision. |
| [Observation key](#observation-key) | Stable logical observation identity inside a series; revisions share it. |
| [Fact type, payload schema version, and revision](#fact-type-payload-schema-version-and-revision) | Three independent axes: meaning, historical interpretation, and correction sequence. |
| [Runtime fact, qualified](#runtime-fact-qualified) | `Runtime fact` is qualified vocabulary for a fact owned by a named runtime boundary, such as a lifecycle event or Wallet Ledger fact. |
| [Research Observation](#research-observation) | A Research Observation is a durable research-memory item. The Check-to-Observation path is a separate explicit operation and admits only completed, frozen, replayable, eligible Check evidence. |
| [Frozen Dataset](#frozen-dataset) | Immutable `market_dataset.v1` manifest over exact Fact revisions, ranges, sources, hashes, gaps, and watermark. |
| [Frozen Market Data Read Binding](#frozen-market-data-read-binding) | Exact consumer binding to a Frozen Dataset and its subjects/ranges/revisions/hashes/gaps. |
| [RunResearchDataset](#runresearchdataset) | Canonical reporting/read-model product derived from durable run truth. |
| [Reporting dataset readiness](#reporting-dataset-readiness) | Materialization status/readiness of RunResearchDataset. |
| [Check](#check) | Bounded analytical operation over declared inputs. |
| [Check preview](#check-preview) | A Check preview is ephemeral analysis. It persists no Check evidence and cannot create, link, or support a Research Observation. |
| [Frozen Check evidence](#frozen-check-evidence) | Frozen Check evidence is a completed Check execution bound to immutable, provider-free, replayable inputs. |
| [Known-at](#known-at) | Causal availability clock used at an evaluation boundary. |
| [Gap Evidence](#gap-evidence) | Explicit source-data range-quality evidence separate from a Fact. |
| [Candle continuity gap](#candle-continuity-gap) | One exact candle-continuity classification. |
| [Order-book sequence gap](#order-book-sequence-gap) | Missing sequence evidence that invalidates book reconstruction. |
| [Runner clock gap / overlay clock gap](#runner-clock-gap-overlay-clock-gap) | Separate runner-liveness and projection-invalidation gaps. |
| [Book Validity Interval](#book-validity-interval) | Interval over which reconstructed book state is valid. |
| [Raw record](#raw-record) | One provider frame record with deterministic `raw_record_id`. |
| [Raw archive object / manifest](#raw-archive-object-manifest) | Durable placement/manifest for one or more preassigned raw records. |
| [Bot and Bot Run](#bot-and-bot-run) | Bot is a durable definition; Bot Run owns lifecycle, lease, events, and report identity. |
| [Run, qualified](#run-qualified) | `Run` has no universal platform object; qualify the owning operation, for example Bot Run, Research Check execution, collection attempt, or report materialization. |
| [Report, qualified](#report-qualified) | Separate reporting concepts and artifacts. |
| [Live runtime/composition](#live-runtime-composition) | A live composition/runtime label, without venue-trading authority. |
| [Evidence, qualified](#evidence-qualified) | `Evidence` has no universal platform meaning. Qualify source/receipt evidence, source-data quality or gap evidence, runtime-lifecycle evidence, Check evidence, and scientific evidence by their owning boundary. |
| [Provider ID / Venue ID / strategy datasource and exchange](#provider-id-venue-id-strategy-datasource-and-exchange) | Provider ID and Venue ID belong to provider selection, credentials, and instrument admission. At the Strategy boundary, `datasource` and `exchange` are compatibility defaults and lookup hints only. |
| [Canonical Instrument / provider product identity](#canonical-instrument-provider-product-identity) | A Canonical Instrument is the linked platform instrument referenced by `instrument_id`. Provider product IDs and display symbols remain provider/venue-facing lookup identities and do not replace that canonical link. |
| [Source Identity / Series Identity](#source-identity-series-identity) | Source Identity records acquisition provenance for a canonical Fact. Series Identity names the canonical logical fact stream whose exact identity is defined by the owning market-data contract. |
| [Collector Definition / collector adapter / collector worker](#collector-definition-collector-adapter-collector-worker) | Definition is durable reviewed configuration; adapter is code that implements one stream contract; worker is the running owner of a definition. |
| [Collector definition admission / collector operation](#collector-definition-admission-collector-operation) | Collector definition admission applies the reviewed configured gate to a durable collector definition. A collector operation requests a lifecycle action against admitted configuration. |
| [Qualified Coverage](#qualified-coverage) | Acquisition coverage, trade-stream coverage intervals, archive-complete mapping evidence, and Frozen Dataset scope are distinct evidence. |
| [Archive mapping / archive coverage / retention pin](#archive-mapping-archive-coverage-retention-pin) | Mapping links raw records to immutable archive objects; archive coverage proves durable placement over a range; a retention pin prevents eligible object expiry. |
| [Reconstructed Book State / book checkpoint / execution-book tape](#reconstructed-book-state-book-checkpoint-execution-book-tape) | Book state is disposable current reconstruction; checkpoint accelerates deterministic replay; a certified execution tape is a frozen runtime input over causal snapshots. |
| [Typed Indicator Output / output catalog / output readiness](#typed-indicator-output-output-catalog-output-readiness) | Catalog declares every public output; each bar returns every declared typed output; `ready=false` means present but not yet usable. |
| [Indicator lifecycle output](#indicator-lifecycle-output) | Indicator lifecycle output is a qualified name for a declared public typed Indicator output that reports lifecycle-like state. |
| [Overlay contract / overlay snapshot / overlay delta](#overlay-contract-overlay-snapshot-overlay-delta) | Contract names renderable payload semantics; snapshot is full current visual state; delta is bounded projection transport with its own clock. |
| [Signal ID / Decision ID / order and fill IDs](#signal-id-decision-id-order-and-fill-ids) | Distinct causal identities link an Indicator signal to a Strategy decision and later execution artifacts without aliasing them. |
| [Strategy definition / Compiled Strategy / effective strategy / run strategy snapshot](#strategy-definition-compiled-strategy-effective-strategy-run-strategy-snapshot) | Authored rules compile to executable semantics; variant resolution produces the effective strategy; run start freezes the exact snapshot. |
| [Strategy Variant / Output Filter](#strategy-variant-output-filter) | A Variant is a named diff whose output filters add deterministic conditions over public outputs already attached to the base Strategy. |
| [SeriesExecutionProfile (compatibility compiler)](#seriesexecutionprofile-compatibility-compiler) | `SeriesExecutionProfile` is the compatibility compiler for current instrument, risk, margin, and legacy fee inputs. It is not `InstrumentExecutionContract` or the immutable run-scoped `ResolvedExecutionContext`. |
| [Resolved Execution Context / context bundle](#resolved-execution-context-context-bundle) | Immutable per-series binding of exact execution contracts; a bundle pins one context per runtime series for a run. |
| [Runtime Execution Plan / Canonical Order Request](#runtime-execution-plan-canonical-order-request) | Plan is the run-time intended action; Canonical Order Request begins durable order quantity and policy custody. |
| [Canonical Order Lifecycle / `FillOrder` / fill](#canonical-order-lifecycle-fillorder-fill) | Lifecycle is append-only durable order truth; `FillOrder` is a compatibility request to an execution adapter; fill is one executed quantity event. |
| [Wallet state / Wallet Ledger fact / wallet commit clock](#wallet-state-wallet-ledger-fact-wallet-commit-clock) | Wallet state is current capital truth; ledger facts are replayable transitions; `wallet_commit_seq` orders shared-wallet mutation. |
| [BotLens projection](#botlens-projection) | Projection is bounded debugger state derived from runtime facts; bootstrap establishes a base; snapshot is the current run- or symbol-scoped read model. |
| [Qualified Cursor](#qualified-cursor) | `run_seq`, `wallet_commit_seq`, `position_commit_seq`, `indicator_commit_seq`, `overlay_commit_seq`, and selected-symbol stream `base_seq` are distinct owner-scoped ordered positions. |
| [Readiness, qualified](#readiness-qualified) | Indicator output readiness, BotLens projection readiness, reporting readiness, comparison readiness, and golden-candidate readiness are distinct owner-scoped states. |
| [Lease / claim / ownership fence](#lease-claim-ownership-fence) | Lease or claim grants time-bounded work ownership; the fence is the token-and-generation check that prevents a stale owner from committing. |
| [Async Job / job-owned effect](#async-job-job-owned-effect) | Durable queue item whose current fenced claim may append its result and associated domain effects atomically. |
| [Semantic fingerprint / operational fingerprint](#semantic-fingerprint-operational-fingerprint) | Semantic fingerprint covers stable trading behavior and material identity; operational fingerprint covers diagnostics, ordering, availability, and runtime drift. |
| [Report input fingerprint / data snapshot hash](#report-input-fingerprint-data-snapshot-hash) | Input fingerprint validates one report materialization against durable run inputs; data snapshot hash identifies exact runtime-consumed data material. |
| [Deployment contract / strategy deployment authority](#deployment-contract-strategy-deployment-authority) | Platform deployment names deployment of QT's platform/runtime composition. Strategy deployment authority would separately permit an artifact to control external execution. |

<a id="canonical-fact"></a>
### Canonical Fact

One immutable, typed, provider-neutral, dataset-eligible market observation.

**Use:** Reserve capitalized Fact for the data-plane concept.

**Defined by:**
- [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)

<a id="fact-revision"></a>
### Fact revision

Immutable stored revision of one logical observation; correction appends a revision.

**Use:** A Fact is not a revision.

**Defined by:**
- [0050-use-one-canonical-append-only-market-data-store.md — Invariants](../../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md#invariants)
- [0063-use-schema-registered-canonical-facts.md — Versioning](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#versioning)

<a id="observation-key"></a>
### Observation key

Stable logical observation identity inside a series; revisions share it.

**Use:** Not a Research Observation ID.

**Defined by:**
- [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)

<a id="fact-type-payload-schema-version-and-revision"></a>
### Fact type, payload schema version, and revision

Three independent axes: meaning, historical interpretation, and correction sequence.

**Use:** Never collapse these into a generic version.

**Defined by:**
- [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
- [0063-use-schema-registered-canonical-facts.md — Versioning](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#versioning)

<a id="runtime-fact-qualified"></a>
### Runtime fact, qualified

`Runtime fact` is qualified vocabulary for a fact owned by a named runtime boundary, such as a lifecycle event or Wallet Ledger fact.

**Use:** A runtime fact is not a Canonical Fact. Always retain the owning qualifier; this term does not create a universal runtime-fact store or authority.

**Defined by:**
- [01_runtime_contract.md — Shared-Wallet Entry Ordering](01_runtime_contract.md#shared-wallet-entry-ordering)
- [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
- [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)

<a id="research-observation"></a>
### Research Observation

A Research Observation is a durable research-memory item. The Check-to-Observation path is a separate explicit operation and admits only completed, frozen, replayable, eligible Check evidence.

**Use:** Manual Observation creation remains a separately owned and admitted path. Ordinary market observations and durable Research Observations are distinct; legacy V1 Check/Observation records remain readable and are not upgraded by this definition.

**Defined by:**
- [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Decision](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#decision)
- [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Supersession Scope](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#supersession-scope)
- [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)

<a id="frozen-dataset"></a>
### Frozen Dataset

Immutable `market_dataset.v1` manifest over exact Fact revisions, ranges, sources, hashes, gaps, and watermark.

**Use:** Use the qualified label for the immutable source-data manifest; bare `Dataset` does not identify an owner or scope.

**Defined by:**
- [0051-require-frozen-datasets-for-canonical-backtests.md — Decision](../../architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md#decision)
- [0063-use-schema-registered-canonical-facts.md — Dataset And Research Semantics](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#dataset-and-research-semantics)

<a id="frozen-market-data-read-binding"></a>
### Frozen Market Data Read Binding

Exact consumer binding to a Frozen Dataset and its subjects/ranges/revisions/hashes/gaps.

**Use:** A binding is not a Dataset or readiness certificate.

**Defined by:**
- [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)

<a id="runresearchdataset"></a>
### RunResearchDataset

Canonical reporting/read-model product derived from durable run truth.

**Use:** Not a Frozen Dataset; does not own source-data truth.

**Defined by:**
- [0010-use-run-research-dataset-as-reporting-contract.md — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)

<a id="reporting-dataset-readiness"></a>
### Reporting dataset readiness

Materialization status/readiness of RunResearchDataset.

**Use:** Does not certify source Dataset completeness or science.

**Defined by:**
- [0010-use-run-research-dataset-as-reporting-contract.md — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)

<a id="check"></a>
### Check

Bounded analytical operation over declared inputs.

**Use:** Not acquisition, Indicator evaluation, Strategy decision, or promotion authority.

**Defined by:**
- [0060-use-capability-native-research-and-collection-contracts.md — Decision](../../architecture/decisions/0060-use-capability-native-research-and-collection-contracts.md#decision)
- [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)

<a id="check-preview"></a>
### Check preview

A Check preview is ephemeral analysis. It persists no Check evidence and cannot create, link, or support a Research Observation.

**Use:** Preview output is neither frozen evidence nor Research-Observation-eligible evidence.

**Defined by:**
- [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Decision](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#decision)
- [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Supersession Scope](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#supersession-scope)

<a id="frozen-check-evidence"></a>
### Frozen Check evidence

Frozen Check evidence is a completed Check execution bound to immutable, provider-free, replayable inputs.

**Use:** Not a scientific verdict.

**Defined by:**
- [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
- [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Decision](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#decision)

<a id="known-at"></a>
### Known-at

Causal availability clock used at an evaluation boundary.

**Use:** Not provider effective, publication, funding, valuation, receipt, or attestation time unless a schema says so.

**Defined by:**
- [00_system_contract.md — Core Invariants](00_system_contract.md#core-invariants)
- [0044-enforce-known-at-prefix-invariance.md — Decision](../../architecture/decisions/0044-enforce-known-at-prefix-invariance.md#decision)

<a id="gap-evidence"></a>
### Gap Evidence

Explicit source-data range-quality evidence separate from a Fact.

**Use:** Bare Gap is insufficient because several gap namespaces coexist.

**Defined by:**
- [0063-use-schema-registered-canonical-facts.md — Provenance And Causal Time](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#provenance-and-causal-time)
- [01_runtime_contract.md — BotLens Candle Continuity Audit Surface](01_runtime_contract.md#botlens-candle-continuity-audit-surface)

<a id="candle-continuity-gap"></a>
### Candle continuity gap

One exact candle-continuity classification.

**Use:** Use expected-session, provider-missing, ingestion-failure, runtime-missing, projection-missing, or unknown.

**Defined by:**
- [01_runtime_contract.md — BotLens Candle Continuity Audit Surface](01_runtime_contract.md#botlens-candle-continuity-audit-surface)
- [0050-use-one-canonical-append-only-market-data-store.md — Decision](../../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md#decision)

<a id="order-book-sequence-gap"></a>
### Order-book sequence gap

Missing sequence evidence that invalidates book reconstruction.

**Use:** Not candle coverage evidence.

**Defined by:**
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)

<a id="runner-clock-gap-overlay-clock-gap"></a>
### Runner clock gap / overlay clock gap

Separate runner-liveness and projection-invalidation gaps.

**Use:** Not source-data gaps.

**Defined by:**
- [0021-use-runner-clock-gap-sentinel.md — Decision](../../architecture/decisions/0021-use-runner-clock-gap-sentinel.md#decision)
- [0038-decouple-visual-overlay-projection-from-runtime-push.md — Decision](../../architecture/decisions/0038-decouple-visual-overlay-projection-from-runtime-push.md#decision)

<a id="book-validity-interval"></a>
### Book Validity Interval

Interval over which reconstructed book state is valid.

**Use:** Avoid generic “valid interval”.

**Defined by:**
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)

<a id="raw-record"></a>
### Raw record

One provider frame record with deterministic `raw_record_id`.

**Use:** Not a raw archive object or Canonical Fact revision.

**Defined by:**
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)

<a id="raw-archive-object-manifest"></a>
### Raw archive object / manifest

Durable placement/manifest for one or more preassigned raw records.

**Use:** Preserve record-versus-object distinction.

**Defined by:**
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)

<a id="bot-and-bot-run"></a>
### Bot and Bot Run

Bot is a durable definition; Bot Run owns lifecycle, lease, events, and report identity.

**Use:** Never assign run lifecycle to the Bot row.

**Defined by:**
- [0030-keep-portal-bots-definition-only.md — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)
- [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)

<a id="run-qualified"></a>
### Run, qualified

`Run` has no universal platform object; qualify the owning operation, for example Bot Run, Research Check execution, collection attempt, or report materialization.

**Use:** A shared identifier or time range does not merge lifecycle, evidence, or authority between those operations.

**Defined by:**
- [0030-keep-portal-bots-definition-only.md — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)
- [0039-use-shared-async-jobs-for-research-dispatch.md — Decision](../../architecture/decisions/0039-use-shared-async-jobs-for-research-dispatch.md#decision)
- [0052-use-typed-fact-collectors-and-explicit-instrument-roles.md — Decision](../../architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md#decision)
- [0031-fingerprint-reports-and-slim-runtime-storage.md — Decision](../../architecture/decisions/0031-fingerprint-reports-and-slim-runtime-storage.md#decision)

<a id="report-qualified"></a>
### Report, qualified

Separate reporting concepts and artifacts.

**Use:** Report status cannot alter Bot Run status.

**Defined by:**
- [0010-use-run-research-dataset-as-reporting-contract.md — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)
- [0031-fingerprint-reports-and-slim-runtime-storage.md — Decision](../../architecture/decisions/0031-fingerprint-reports-and-slim-runtime-storage.md#decision)

<a id="live-runtime-composition"></a>
### Live runtime/composition

A live composition/runtime label, without venue-trading authority.

**Use:** “Live” does not mean external order submission.

**Defined by:**
- [02_execution_playback_contract.md — Runtime Mode And External-Submission Authority](02_execution_playback_contract.md#runtime-mode-and-external-submission-authority)
- [0049-keep-live-order-submission-closed.md — Decision](../../architecture/decisions/0049-keep-live-order-submission-closed.md#decision)

<a id="evidence-qualified"></a>
### Evidence, qualified

`Evidence` has no universal platform meaning. Qualify source/receipt evidence, source-data quality or gap evidence, runtime-lifecycle evidence, Check evidence, and scientific evidence by their owning boundary.

**Use:** Evidence in one boundary grants no readiness, certification, or authority owned by another boundary.

**Defined by:**
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
- [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
- [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
- [0059-use-in-app-scientific-authority-and-offline-certification-ceiling.md — Decision](../../architecture/decisions/0059-use-in-app-scientific-authority-and-offline-certification-ceiling.md#decision)

<a id="provider-id-venue-id-strategy-datasource-and-exchange"></a>
### Provider ID / Venue ID / strategy datasource and exchange

Provider ID and Venue ID belong to provider selection, credentials, and instrument admission. At the Strategy boundary, `datasource` and `exchange` are compatibility defaults and lookup hints only.

**Use:** Compatibility fields cannot override a linked canonical instrument or its source routing; this entry does not define provider/venue identity internals or an exchange-slug translation contract.

**Defined by:**
- [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)

<a id="canonical-instrument-provider-product-identity"></a>
### Canonical Instrument / provider product identity

A Canonical Instrument is the linked platform instrument referenced by `instrument_id`. Provider product IDs and display symbols remain provider/venue-facing lookup identities and do not replace that canonical link.

**Use:** Resolve compatibility symbols or product IDs to the linked canonical instrument before runtime routing; do not treat a display symbol as a globally stable instrument key.

**Defined by:**
- [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
- [0027-use-execution-profiles-as-runtime-instrument-authority.md — Decision](../../architecture/decisions/0027-use-execution-profiles-as-runtime-instrument-authority.md#decision)
- [0052-use-typed-fact-collectors-and-explicit-instrument-roles.md — Decision](../../architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md#decision)

<a id="source-identity-series-identity"></a>
### Source Identity / Series Identity

Source Identity records acquisition provenance for a canonical Fact. Series Identity names the canonical logical fact stream whose exact identity is defined by the owning market-data contract.

**Use:** Provider, source, series, and instrument identities remain distinct. Compatibility datasource or exchange values do not override canonical linked identities.

**Defined by:**
- [0050-use-one-canonical-append-only-market-data-store.md — Decision](../../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md#decision)
- [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
- [0063-use-schema-registered-canonical-facts.md — Provenance And Causal Time](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#provenance-and-causal-time)
- [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)

<a id="collector-definition-collector-adapter-collector-worker"></a>
### Collector Definition / collector adapter / collector worker

Definition is durable reviewed configuration; adapter is code that implements one stream contract; worker is the running owner of a definition.

**Use:** Bare Collector is unsafe when configuration, code, and process ownership differ.

**Defined by:**
- [0064-use-one-code-owned-collector-operations-contract.md — Decision](../../architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md#decision)

<a id="collector-definition-admission-collector-operation"></a>
### Collector definition admission / collector operation

Collector definition admission applies the reviewed configured gate to a durable collector definition. A collector operation requests a lifecycle action against admitted configuration.

**Use:** `Enrollment` is compatibility wording only. Admission does not start a collector, and operation success does not prove subsequent worker readiness.

**Defined by:**
- [0064-use-one-code-owned-collector-operations-contract.md — Decision](../../architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md#decision)

<a id="qualified-coverage"></a>
### Qualified Coverage

Acquisition coverage, trade-stream coverage intervals, archive-complete mapping evidence, and Frozen Dataset scope are distinct evidence.

**Use:** Completeness in one named evidence boundary does not certify another.

**Defined by:**
- [0063-use-schema-registered-canonical-facts.md — Provenance And Causal Time](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#provenance-and-causal-time)
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)
- [0063-use-schema-registered-canonical-facts.md — Dataset And Research Semantics](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#dataset-and-research-semantics)

<a id="archive-mapping-archive-coverage-retention-pin"></a>
### Archive mapping / archive coverage / retention pin

Mapping links raw records to immutable archive objects; archive coverage proves durable placement over a range; a retention pin prevents eligible object expiry.

**Use:** None is a Canonical Fact, Dataset freeze, or book-validity certificate.

**Defined by:**
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)

<a id="reconstructed-book-state-book-checkpoint-execution-book-tape"></a>
### Reconstructed Book State / book checkpoint / execution-book tape

Book state is disposable current reconstruction; checkpoint accelerates deterministic replay; a certified execution tape is a frozen runtime input over causal snapshots.

**Use:** A checkpoint or tape is not provider truth, and hot book state is never Dataset truth.

**Defined by:**
- [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
- [0058-use-replay-certified-execution-book-tapes.md — Decision](../../architecture/decisions/0058-use-replay-certified-execution-book-tapes.md#decision)

<a id="typed-indicator-output-output-catalog-output-readiness"></a>
### Typed Indicator Output / output catalog / output readiness

Catalog declares every public output; each bar returns every declared typed output; `ready=false` means present but not yet usable.

**Use:** Output preference or visibility must not rewrite Indicator truth.

**Defined by:**
- [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Decision](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#decision)
- [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Guardrails](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#guardrails)

<a id="indicator-lifecycle-output"></a>
### Indicator lifecycle output

Indicator lifecycle output is a qualified name for a declared public typed Indicator output that reports lifecycle-like state.

**Use:** The owning output contract defines its exact values. It is not a Bot Run, order, trade, collector, or deployment lifecycle event and does not turn projections into Strategy inputs.

**Defined by:**
- [0004-separate-indicator-truth-from-projections.md — Decision](../../architecture/decisions/0004-separate-indicator-truth-from-projections.md#decision)
- [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Decision](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#decision)

<a id="overlay-contract-overlay-snapshot-overlay-delta"></a>
### Overlay contract / overlay snapshot / overlay delta

Contract names renderable payload semantics; snapshot is full current visual state; delta is bounded projection transport with its own clock.

**Use:** Overlays are projections, never Strategy inputs or canonical execution state.

**Defined by:**
- [0004-separate-indicator-truth-from-projections.md — Decision](../../architecture/decisions/0004-separate-indicator-truth-from-projections.md#decision)
- [0038-decouple-visual-overlay-projection-from-runtime-push.md — Decision](../../architecture/decisions/0038-decouple-visual-overlay-projection-from-runtime-push.md#decision)

<a id="signal-id-decision-id-order-and-fill-ids"></a>
### Signal ID / Decision ID / order and fill IDs

Distinct causal identities link an Indicator signal to a Strategy decision and later execution artifacts without aliasing them.

**Use:** Sharing provenance does not make signal, decision, order, attempt, event, and fill the same object.

**Defined by:**
- [0005-keep-strategy-decisions-separate-from-execution.md — Decision](../../architecture/decisions/0005-keep-strategy-decisions-separate-from-execution.md#decision)
- [0041-use-canonical-execution-plan-and-order-fill-semantics.md — Decision](../../architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md#decision)
- [0057-use-append-only-canonical-order-lifecycle.md — Decision](../../architecture/decisions/0057-use-append-only-canonical-order-lifecycle.md#decision)

<a id="strategy-definition-compiled-strategy-effective-strategy-run-strategy-snapshot"></a>
### Strategy definition / Compiled Strategy / effective strategy / run strategy snapshot

Authored rules compile to executable semantics; variant resolution produces the effective strategy; run start freezes the exact snapshot.

**Use:** “Strategy” must not conceal which stage or frozen identity is meant.

**Defined by:**
- [0018-use-output-filters-as-strategy-variant-contract.md — Decision](../../architecture/decisions/0018-use-output-filters-as-strategy-variant-contract.md#decision)
- [0005-keep-strategy-decisions-separate-from-execution.md — Decision](../../architecture/decisions/0005-keep-strategy-decisions-separate-from-execution.md#decision)

<a id="strategy-variant-output-filter"></a>
### Strategy Variant / Output Filter

A Variant is a named diff whose output filters add deterministic conditions over public outputs already attached to the base Strategy.

**Use:** A Variant does not own ATM selection, Indicator config, or a second evaluator.

**Defined by:**
- [0018-use-output-filters-as-strategy-variant-contract.md — Decision](../../architecture/decisions/0018-use-output-filters-as-strategy-variant-contract.md#decision)

<a id="seriesexecutionprofile-compatibility-compiler"></a>
### SeriesExecutionProfile (compatibility compiler)

`SeriesExecutionProfile` is the compatibility compiler for current instrument, risk, margin, and legacy fee inputs. It is not `InstrumentExecutionContract` or the immutable run-scoped `ResolvedExecutionContext`.

**Use:** Reject Instrument Execution Profile as an umbrella. `InstrumentExecutionContract` remains one distinct constituent of the resolved context, and `ResolvedExecutionContext` is the final immutable run authority.

**Defined by:**
- [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
- [0027-use-execution-profiles-as-runtime-instrument-authority.md — Decision](../../architecture/decisions/0027-use-execution-profiles-as-runtime-instrument-authority.md#decision)
- [0056-pin-venue-neutral-execution-contexts-per-run.md — Decision](../../architecture/decisions/0056-pin-venue-neutral-execution-contexts-per-run.md#decision)

<a id="resolved-execution-context-context-bundle"></a>
### Resolved Execution Context / context bundle

Immutable per-series binding of exact execution contracts; a bundle pins one context per runtime series for a run.

**Use:** Context binds execution assumptions; it does not own Strategy meaning, accounting, or authorization.

**Defined by:**
- [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
- [0056-pin-venue-neutral-execution-contexts-per-run.md — Decision](../../architecture/decisions/0056-pin-venue-neutral-execution-contexts-per-run.md#decision)

<a id="runtime-execution-plan-canonical-order-request"></a>
### Runtime Execution Plan / Canonical Order Request

Plan is the run-time intended action; Canonical Order Request begins durable order quantity and policy custody.

**Use:** Neither is a fill, and the plan is not the durable order lifecycle.

**Defined by:**
- [0041-use-canonical-execution-plan-and-order-fill-semantics.md — Decision](../../architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md#decision)
- [0057-use-append-only-canonical-order-lifecycle.md — Decision](../../architecture/decisions/0057-use-append-only-canonical-order-lifecycle.md#decision)

<a id="canonical-order-lifecycle-fillorder-fill"></a>
### Canonical Order Lifecycle / `FillOrder` / fill

Lifecycle is append-only durable order truth; `FillOrder` is a compatibility request to an execution adapter; fill is one executed quantity event.

**Use:** Never describe `FillOrder` as the durable order or a fill as the whole order.

**Defined by:**
- [0057-use-append-only-canonical-order-lifecycle.md — Decision](../../architecture/decisions/0057-use-append-only-canonical-order-lifecycle.md#decision)
- [0041-use-canonical-execution-plan-and-order-fill-semantics.md — Decision](../../architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md#decision)

<a id="wallet-state-wallet-ledger-fact-wallet-commit-clock"></a>
### Wallet state / Wallet Ledger fact / wallet commit clock

Wallet state is current capital truth; ledger facts are replayable transitions; `wallet_commit_seq` orders shared-wallet mutation.

**Use:** Report/BotLens wallet views and runtime-event append order are not alternate wallet truth.

**Defined by:**
- [01_runtime_contract.md — Shared-Wallet Entry Ordering](01_runtime_contract.md#shared-wallet-entry-ordering)
- [0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md — Decision](../../architecture/decisions/0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md#decision)
- [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)

<a id="botlens-projection"></a>
### BotLens projection

Projection is bounded debugger state derived from runtime facts; bootstrap establishes a base; snapshot is the current run- or symbol-scoped read model.

**Use:** BotLens never creates execution truth; empty projection is not implicit readiness.

**Defined by:**
- [01_runtime_contract.md — BotLens Readiness Semantics](01_runtime_contract.md#botlens-readiness-semantics)
- [0008-treat-botlens-as-projection-debugger.md — Decision](../../architecture/decisions/0008-treat-botlens-as-projection-debugger.md#decision)
- [0055-separate-bounded-botlens-hot-state-from-durable-inspection.md — Decision](../../architecture/decisions/0055-separate-bounded-botlens-hot-state-from-durable-inspection.md#decision)

<a id="qualified-cursor"></a>
### Qualified Cursor

`run_seq`, `wallet_commit_seq`, `position_commit_seq`, `indicator_commit_seq`, `overlay_commit_seq`, and selected-symbol stream `base_seq` are distinct owner-scoped ordered positions.

**Use:** Bare `cursor` or `sequence` is contextual shorthand only; do not compare or substitute positions from different owners.

**Defined by:**
- [0007-use-scoped-causal-clocks-for-runtime-replay.md — Decision](../../architecture/decisions/0007-use-scoped-causal-clocks-for-runtime-replay.md#decision)
- [01_runtime_contract.md — Shared-Wallet Entry Ordering](01_runtime_contract.md#shared-wallet-entry-ordering)

<a id="readiness-qualified"></a>
### Readiness, qualified

Indicator output readiness, BotLens projection readiness, reporting readiness, comparison readiness, and golden-candidate readiness are distinct owner-scoped states.

**Use:** `Ready` must name its owning boundary; one readiness result does not confer certification or authority at another.

**Defined by:**
- [01_runtime_contract.md — BotLens Readiness Semantics](01_runtime_contract.md#botlens-readiness-semantics)
- [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Guardrails](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#guardrails)
- [0010-use-run-research-dataset-as-reporting-contract.md — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)

<a id="lease-claim-ownership-fence"></a>
### Lease / claim / ownership fence

Lease or claim grants time-bounded work ownership; the fence is the token-and-generation check that prevents a stale owner from committing.

**Use:** A row lock, heartbeat, lease timestamp, token, and generation are related but not synonyms.

**Defined by:**
- [0047-fence-async-job-ownership.md — Decision](../../architecture/decisions/0047-fence-async-job-ownership.md#decision)
- [0025-use-per-run-leases.md — Decision](../../architecture/decisions/0025-use-per-run-leases.md#decision)

<a id="async-job-job-owned-effect"></a>
### Async Job / job-owned effect

Durable queue item whose current fenced claim may append its result and associated domain effects atomically.

**Use:** A job is neither the Check it dispatches nor the worker process that claims it.

**Defined by:**
- [0039-use-shared-async-jobs-for-research-dispatch.md — Decision](../../architecture/decisions/0039-use-shared-async-jobs-for-research-dispatch.md#decision)
- [0047-fence-async-job-ownership.md — Decision](../../architecture/decisions/0047-fence-async-job-ownership.md#decision)

<a id="semantic-fingerprint-operational-fingerprint"></a>
### Semantic fingerprint / operational fingerprint

Semantic fingerprint covers stable trading behavior and material identity; operational fingerprint covers diagnostics, ordering, availability, and runtime drift.

**Use:** Operational drift can coexist with semantic equivalence.

**Defined by:**
- [0015-split-semantic-and-operational-golden-fingerprints.md — Decision](../../architecture/decisions/0015-split-semantic-and-operational-golden-fingerprints.md#decision)

<a id="report-input-fingerprint-data-snapshot-hash"></a>
### Report input fingerprint / data snapshot hash

Input fingerprint validates one report materialization against durable run inputs; data snapshot hash identifies exact runtime-consumed data material.

**Use:** Neither is the semantic or operational fingerprint.

**Defined by:**
- [0031-fingerprint-reports-and-slim-runtime-storage.md — Decision](../../architecture/decisions/0031-fingerprint-reports-and-slim-runtime-storage.md#decision)
- [0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md — Decision](../../architecture/decisions/0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md#decision)

<a id="deployment-contract-strategy-deployment-authority"></a>
### Deployment contract / strategy deployment authority

Platform deployment names deployment of QT's platform/runtime composition. Strategy deployment authority would separately permit an artifact to control external execution.

**Use:** Successful deployment, configuration, credentials, or a live label does not grant research-promotion or external-order authority.

**Defined by:**
- [02_execution_playback_contract.md — Runtime Mode And External-Submission Authority](02_execution_playback_contract.md#runtime-mode-and-external-submission-authority)
- [0049-keep-live-order-submission-closed.md — Decision](../../architecture/decisions/0049-keep-live-order-submission-closed.md#decision)

## Aliases And Historical Usage

Old or ambiguous names remain listed so they cannot silently change the
meaning of a current term. Compatibility spelling is acceptable only
within the boundary described below.

<a id="alias-candle-versions-numeric-fact-versions-family-persistence-tables"></a>
### `candle_versions`, `numeric_fact_versions`, `family persistence tables`

**Classification:** historical.

**Use:** Retain only in migration history; current persistence vocabulary is the canonical Fact store.

**Related terms:** [Canonical Fact](#canonical-fact).

**Defined by:**
- [0063-use-schema-registered-canonical-facts.md — Migration And Cutover](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#migration-and-cutover)

<a id="alias-numeric-fact-consolidation-deferred"></a>
### `NUMERIC_FACT_CONSOLIDATION_DEFERRED`

**Classification:** historical.

**Use:** Treat as a superseded historical claim; it does not describe the current hard-cutover state.

**Related terms:** [Canonical Fact](#canonical-fact).

**Defined by:**
- [0063-use-schema-registered-canonical-facts.md — Status](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#status)
- [0063-use-schema-registered-canonical-facts.md — Migration And Cutover](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#migration-and-cutover)

<a id="alias-legacy-market-v1"></a>
### `legacy_market_v1`

**Classification:** historical.

**Use:** Retain as migration/archive lineage only; it is not a current application read vocabulary.

**Related terms:** [Canonical Fact](#canonical-fact).

**Defined by:**
- [0063-use-schema-registered-canonical-facts.md — Migration And Cutover](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#migration-and-cutover)

<a id="alias-legacy-unpinned-legacy-frozen-unverifiable"></a>
### `legacy_unpinned`, `legacy_frozen_unverifiable`

**Classification:** compatibility.

**Use:** Compatibility-readable legacy Check states only; never describe them as current replayable evidence.

**Related terms:** [Check preview](#check-preview), [Frozen Check evidence](#frozen-check-evidence).

**Defined by:**
- [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Supersession Scope](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#supersession-scope)
- [0062-use-frozen-bindings-for-durable-check-evidence.md — Rejected Alternatives](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#rejected-alternatives)

<a id="alias-qt-experiments-run-bot"></a>
### `qt experiments run-bot`

**Classification:** compatibility.

**Use:** Treat `qt experiments run-bot` as a deprecated compatibility spelling. Prefer the current explicit Bot-start and collection workflow; the alias does not merge Check and Bot Run semantics.

**Related terms:** [Check](#check), [Bot and Bot Run](#bot-and-bot-run).

**Defined by:**
- [0060-use-capability-native-research-and-collection-contracts.md — Decision](../../architecture/decisions/0060-use-capability-native-research-and-collection-contracts.md#decision)
- [0030-keep-portal-bots-definition-only.md — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)

<a id="alias-instant-walk-forward-walkforward"></a>
### `instant`, `walk-forward`, `walkforward`

**Classification:** rejected.

**Use:** These are playback vocabulary, not valid execution-mode names.

**Related terms:** [Live runtime/composition](#live-runtime-composition).

**Defined by:**
- [02_execution_playback_contract.md — Execution Mode Policy](02_execution_playback_contract.md#execution-mode-policy)
- [02_execution_playback_contract.md — Playback Contract](02_execution_playback_contract.md#playback-contract)

<a id="alias-legacy-bot-row-lifecycle-fields"></a>
### `legacy Bot-row lifecycle fields`

**Classification:** historical.

**Use:** Retain for lineage only; Bot Run and the runtime event ledger own lifecycle truth.

**Related terms:** [Bot and Bot Run](#bot-and-bot-run).

**Defined by:**
- [0030-keep-portal-bots-definition-only.md — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)
- [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)

<a id="alias-fact-family-table"></a>
### `Fact family table`

**Classification:** historical.

**Use:** Retain only in historical migration documents; exclude from current storage vocabulary.

**Related terms:** [Canonical Fact](#canonical-fact).

**Defined by:**
- [0063-use-schema-registered-canonical-facts.md — Migration And Cutover](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#migration-and-cutover)

<a id="alias-datasource"></a>
### `datasource`

**Classification:** compatibility.

**Use:** `datasource` may remain a Strategy input default or lookup hint. It is neither Provider ID nor canonical Source Identity and cannot override a linked canonical instrument.

**Related terms:** [Provider ID / Venue ID / strategy datasource and exchange](#provider-id-venue-id-strategy-datasource-and-exchange), [Source Identity / Series Identity](#source-identity-series-identity).

**Defined by:**
- [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)

<a id="alias-exchange"></a>
### `exchange`

**Classification:** compatibility.

**Use:** `exchange` may remain a Strategy input default or lookup hint. It is not canonical Venue ID authority and cannot override a linked canonical instrument.

**Related terms:** [Provider ID / Venue ID / strategy datasource and exchange](#provider-id-venue-id-strategy-datasource-and-exchange).

**Defined by:**
- [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)

<a id="alias-symbol-as-instrument-id-product-id-as-instrument-id"></a>
### `symbol as Instrument ID`, `product ID as Instrument ID`

**Classification:** rejected.

**Use:** Resolve a symbol or provider product ID through the linked canonical instrument; neither is accepted as canonical `instrument_id`.

**Related terms:** [Canonical Instrument / provider product identity](#canonical-instrument-provider-product-identity).

**Defined by:**
- [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
- [0027-use-execution-profiles-as-runtime-instrument-authority.md — Decision](../../architecture/decisions/0027-use-execution-profiles-as-runtime-instrument-authority.md#decision)
- [0052-use-typed-fact-collectors-and-explicit-instrument-roles.md — Decision](../../architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md#decision)

<a id="alias-collector"></a>
### `collector`

**Classification:** discouraged.

**Use:** Qualify definition, adapter, worker, session, or operation whenever ownership could differ.

**Related terms:** [Collector Definition / collector adapter / collector worker](#collector-definition-collector-adapter-collector-worker), [Collector definition admission / collector operation](#collector-definition-admission-collector-operation).

**Defined by:**
- [0064-use-one-code-owned-collector-operations-contract.md — Decision](../../architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md#decision)

<a id="alias-market-profile-market-profile-mpf"></a>
### `market-profile`, `market_profile`, `mpf`

**Classification:** compatibility.

**Use:** Retain only as existing render-contract spellings; they do not create separate overlay concepts.

**Related terms:** [Overlay contract / overlay snapshot / overlay delta](#overlay-contract-overlay-snapshot-overlay-delta).

**Defined by:**
- [0038-decouple-visual-overlay-projection-from-runtime-push.md — Decision](../../architecture/decisions/0038-decouple-visual-overlay-projection-from-runtime-push.md#decision)
- [0004-separate-indicator-truth-from-projections.md — Decision](../../architecture/decisions/0004-separate-indicator-truth-from-projections.md#decision)

<a id="alias-indicator-lifecycle"></a>
### `indicator lifecycle`

**Classification:** discouraged.

**Use:** Use Indicator lifecycle output only for a declared typed output whose owning contract defines the exact values; do not use it as a generic lifecycle.

**Related terms:** [Indicator lifecycle output](#indicator-lifecycle-output).

**Defined by:**
- [0004-separate-indicator-truth-from-projections.md — Decision](../../architecture/decisions/0004-separate-indicator-truth-from-projections.md#decision)
- [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Decision](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#decision)

<a id="alias-variant-param-overrides"></a>
### `Variant param_overrides`

**Classification:** rejected.

**Use:** Do not describe param_overrides as the primary Strategy Variant contract; the reviewed contract is Output Filters.

**Related terms:** [Strategy Variant / Output Filter](#strategy-variant-output-filter).

**Defined by:**
- [0018-use-output-filters-as-strategy-variant-contract.md — Decision](../../architecture/decisions/0018-use-output-filters-as-strategy-variant-contract.md#decision)

<a id="alias-fillorder"></a>
### `FillOrder`

**Classification:** compatibility.

**Use:** Retain only as an adapter-facing compatibility request; it is neither the durable order nor a fill.

**Related terms:** [Runtime Execution Plan / Canonical Order Request](#runtime-execution-plan-canonical-order-request), [Canonical Order Lifecycle / `FillOrder` / fill](#canonical-order-lifecycle-fillorder-fill).

**Defined by:**
- [0041-use-canonical-execution-plan-and-order-fill-semantics.md — Decision](../../architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md#decision)
- [0057-use-append-only-canonical-order-lifecycle.md — Decision](../../architecture/decisions/0057-use-append-only-canonical-order-lifecycle.md#decision)

<a id="alias-seq-cursor"></a>
### `seq`, `cursor`

**Classification:** rejected.

**Use:** Never imply one global order; qualify the owning run, stream, overlay, wallet, position, source, or row clock.

**Related terms:** [Qualified Cursor](#qualified-cursor).

**Defined by:**
- [0007-use-scoped-causal-clocks-for-runtime-replay.md — Decision](../../architecture/decisions/0007-use-scoped-causal-clocks-for-runtime-replay.md#decision)
- [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
- [0038-decouple-visual-overlay-projection-from-runtime-push.md — Decision](../../architecture/decisions/0038-decouple-visual-overlay-projection-from-runtime-push.md#decision)

<a id="alias-ready-healthy-complete-comparable-golden-deployable"></a>
### `ready`, `healthy`, `complete`, `comparable`, `golden`, `deployable`

**Classification:** discouraged.

**Use:** Name the exact readiness or certification boundary; one status never certifies the others.

**Related terms:** [Readiness, qualified](#readiness-qualified).

**Defined by:**
- [01_runtime_contract.md — BotLens Readiness Semantics](01_runtime_contract.md#botlens-readiness-semantics)
- [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Guardrails](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#guardrails)
- [0015-split-semantic-and-operational-golden-fingerprints.md — Decision](../../architecture/decisions/0015-split-semantic-and-operational-golden-fingerprints.md#decision)

<a id="alias-fingerprint-hash-golden-hash"></a>
### `fingerprint`, `hash`, `golden hash`

**Classification:** discouraged.

**Use:** Qualify semantic, operational, input, data-snapshot, replay, artifact, or source-tree identity before comparing values.

**Related terms:** [Semantic fingerprint / operational fingerprint](#semantic-fingerprint-operational-fingerprint), [Report input fingerprint / data snapshot hash](#report-input-fingerprint-data-snapshot-hash).

**Defined by:**
- [0015-split-semantic-and-operational-golden-fingerprints.md — Decision](../../architecture/decisions/0015-split-semantic-and-operational-golden-fingerprints.md#decision)
- [0031-fingerprint-reports-and-slim-runtime-storage.md — Decision](../../architecture/decisions/0031-fingerprint-reports-and-slim-runtime-storage.md#decision)

<a id="alias-live-as-deployed-trading-production-as-deployed-trading"></a>
### `live as deployed trading`, `production as deployed trading`

**Classification:** rejected.

**Use:** A live runtime/composition or platform deployment label never implies external-order authority.

**Related terms:** [Live runtime/composition](#live-runtime-composition), [Deployment contract / strategy deployment authority](#deployment-contract-strategy-deployment-authority).

**Defined by:**
- [02_execution_playback_contract.md — Runtime Mode And External-Submission Authority](02_execution_playback_contract.md#runtime-mode-and-external-submission-authority)
- [0049-keep-live-order-submission-closed.md — Decision](../../architecture/decisions/0049-keep-live-order-submission-closed.md#decision)
