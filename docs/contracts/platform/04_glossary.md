# QT Platform Glossary

<!-- Generated from ../../plans/documentation-reconciliation/phase-3-terminology-dispositions.json. Do not edit by hand. -->

> **Normative vocabulary index.** This file adopts reviewed vocabulary,
> distinctions, and alias usage only. It does not create or broaden product
> behavior. Each entry is bounded by its cited platform contract or accepted
> ADR; the owning authority controls if this summary differs.

## Authority Boundary

An adopted term says only that its label, distinction, and usage boundary are
accepted. Adoption does not activate a guarantee, attest enforcement or proof,
or resolve a remediation. The glossary is subordinate to the authorities it
indexes and is not an alternate behavioral contract.

The frozen [Phase 2 proposal](../../plans/documentation-reconciliation/proposed-glossary.md)
and [Phase 3 disposition record](../../plans/documentation-reconciliation/phase-3-terminology-dispositions.md)
remain audit evidence, not product authority. Deferred IDs have no normative
definition here.

## Derived Accounting

| Disposition | Count |
| --- | ---: |
| Adopted terms | 53 |
| Deferred terms | 2 |
| Ratified alias rules | 20 |

## Adopted Term Index

| ID | Adopted label | Kind | Owner |
| --- | --- | --- | --- |
| [`QT-TERM-001`](#qt-term-001) | Canonical Fact | `domain_term` | `data` |
| [`QT-TERM-002`](#qt-term-002) | Fact revision | `domain_term` | `data` |
| [`QT-TERM-003`](#qt-term-003) | Observation key | `domain_term` | `data` |
| [`QT-TERM-004`](#qt-term-004) | Fact type, payload schema version, and revision | `contrast_set` | `data` |
| [`QT-TERM-005`](#qt-term-005) | Runtime fact, qualified | `qualification_rule` | `execution-runtime` |
| [`QT-TERM-006`](#qt-term-006) | Research Observation | `domain_term` | `research-memory` |
| [`QT-TERM-007`](#qt-term-007) | Frozen Dataset | `domain_term` | `data` |
| [`QT-TERM-008`](#qt-term-008) | Frozen Market Data Read Binding | `domain_term` | `data` |
| [`QT-TERM-009`](#qt-term-009) | RunResearchDataset | `domain_term` | `reporting` |
| [`QT-TERM-010`](#qt-term-010) | Reporting dataset readiness | `domain_term` | `reporting` |
| [`QT-TERM-011`](#qt-term-011) | Check | `domain_term` | `research-orchestration` |
| [`QT-TERM-012`](#qt-term-012) | Check preview | `domain_term` | `research-orchestration` |
| [`QT-TERM-013`](#qt-term-013) | Frozen Check evidence | `domain_term` | `research-orchestration` |
| [`QT-TERM-014`](#qt-term-014) | Known-at | `domain_term` | `platform` |
| [`QT-TERM-015`](#qt-term-015) | Gap Evidence | `qualification_rule` | `data` |
| [`QT-TERM-016`](#qt-term-016) | Candle continuity gap | `qualification_rule` | `data` |
| [`QT-TERM-017`](#qt-term-017) | Order-book sequence gap | `qualification_rule` | `data` |
| [`QT-TERM-018`](#qt-term-018) | Runner clock gap / overlay clock gap | `contrast_set` | `execution-runtime` |
| [`QT-TERM-019`](#qt-term-019) | Book Validity Interval | `domain_term` | `data` |
| [`QT-TERM-020`](#qt-term-020) | Raw record | `domain_term` | `data` |
| [`QT-TERM-021`](#qt-term-021) | Raw archive object / manifest | `contrast_set` | `data` |
| [`QT-TERM-022`](#qt-term-022) | Bot and Bot Run | `contrast_set` | `execution-runtime` |
| [`QT-TERM-023`](#qt-term-023) | Run, qualified | `qualification_rule` | `platform` |
| [`QT-TERM-024`](#qt-term-024) | Report, qualified | `contrast_set` | `reporting` |
| [`QT-TERM-025`](#qt-term-025) | Live runtime/composition | `qualification_rule` | `execution-runtime` |
| [`QT-TERM-026`](#qt-term-026) | Evidence, qualified | `qualification_rule` | `platform` |
| [`QT-TERM-027`](#qt-term-027) | Provider ID / Venue ID / strategy datasource and exchange | `contrast_set` | `data` |
| [`QT-TERM-028`](#qt-term-028) | Canonical Instrument / provider product identity | `contrast_set` | `identity` |
| [`QT-TERM-029`](#qt-term-029) | Source Identity / Series Identity | `contrast_set` | `data` |
| [`QT-TERM-030`](#qt-term-030) | Collector Definition / collector adapter / collector worker | `contrast_set` | `data` |
| [`QT-TERM-031`](#qt-term-031) | Collector definition admission / collector operation | `contrast_set` | `data` |
| [`QT-TERM-032`](#qt-term-032) | Qualified Coverage | `qualification_rule` | `data` |
| [`QT-TERM-033`](#qt-term-033) | Archive mapping / archive coverage / retention pin | `contrast_set` | `data` |
| [`QT-TERM-034`](#qt-term-034) | Reconstructed Book State / book checkpoint / execution-book tape | `contrast_set` | `data` |
| [`QT-TERM-036`](#qt-term-036) | Typed Indicator Output / output catalog / output readiness | `contrast_set` | `indicator-runtime` |
| [`QT-TERM-037`](#qt-term-037) | Indicator lifecycle output | `domain_term` | `indicator-runtime` |
| [`QT-TERM-038`](#qt-term-038) | Overlay contract / overlay snapshot / overlay delta | `contrast_set` | `indicator-runtime` |
| [`QT-TERM-039`](#qt-term-039) | Signal ID / Decision ID / order and fill IDs | `contrast_set` | `decision-layer` |
| [`QT-TERM-040`](#qt-term-040) | Strategy definition / Compiled Strategy / effective strategy / run strategy snapshot | `contrast_set` | `decision-layer` |
| [`QT-TERM-041`](#qt-term-041) | Strategy Variant / Output Filter | `contrast_set` | `decision-layer` |
| [`QT-TERM-042`](#qt-term-042) | SeriesExecutionProfile (compatibility compiler) | `domain_term` | `execution-runtime` |
| [`QT-TERM-043`](#qt-term-043) | Resolved Execution Context / context bundle | `contrast_set` | `execution-runtime` |
| [`QT-TERM-044`](#qt-term-044) | Runtime Execution Plan / Canonical Order Request | `contrast_set` | `execution-runtime` |
| [`QT-TERM-045`](#qt-term-045) | Canonical Order Lifecycle / `FillOrder` / fill | `contrast_set` | `execution-runtime` |
| [`QT-TERM-046`](#qt-term-046) | Wallet state / Wallet Ledger fact / wallet commit clock | `contrast_set` | `execution-runtime` |
| [`QT-TERM-047`](#qt-term-047) | BotLens projection | `domain_term` | `botlens-projections` |
| [`QT-TERM-048`](#qt-term-048) | Qualified Cursor | `qualification_rule` | `identity` |
| [`QT-TERM-049`](#qt-term-049) | Readiness, qualified | `qualification_rule` | `platform` |
| [`QT-TERM-050`](#qt-term-050) | Lease / claim / ownership fence | `contrast_set` | `persistence` |
| [`QT-TERM-051`](#qt-term-051) | Async Job / job-owned effect | `contrast_set` | `research-orchestration` |
| [`QT-TERM-052`](#qt-term-052) | Semantic fingerprint / operational fingerprint | `contrast_set` | `reporting` |
| [`QT-TERM-053`](#qt-term-053) | Report input fingerprint / data snapshot hash | `contrast_set` | `reporting` |
| [`QT-TERM-054`](#qt-term-054) | Deployment contract / strategy deployment authority | `contrast_set` | `platform` |

## Adopted Definitions

<a id="qt-term-001"></a>
### `QT-TERM-001` — Canonical Fact

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
- Definition: One immutable, typed, provider-neutral, dataset-eligible market observation
- Usage boundary: Reserve capitalized Fact for the data-plane concept
- Conflict disposition: `QT-CONFLICT-001` is `qualified_nonblocking`; `QT-CONFLICT-002` is `qualified_nonblocking`; `QT-CONFLICT-003` is `qualified_nonblocking`; `QT-CONFLICT-004` is `qualified_nonblocking`; `QT-CONFLICT-005` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-002"></a>
### `QT-TERM-002` — Fact revision

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0050-use-one-canonical-append-only-market-data-store.md — Invariants](../../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md#invariants)
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Versioning](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#versioning)
- Definition: Immutable stored revision of one logical observation; correction appends a revision
- Usage boundary: A Fact is not a revision
- Conflict disposition: `QT-CONFLICT-001` is `qualified_nonblocking`; `QT-CONFLICT-002` is `qualified_nonblocking`; `QT-CONFLICT-003` is `qualified_nonblocking`; `QT-CONFLICT-004` is `qualified_nonblocking`; `QT-CONFLICT-005` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-003"></a>
### `QT-TERM-003` — Observation key

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Consulted boundaries: `research-memory`
- Authority clauses:
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
- Definition: Stable logical observation identity inside a series; revisions share it
- Usage boundary: Not a Research Observation ID
- Conflict disposition: `QT-CONFLICT-010` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-004"></a>
### `QT-TERM-004` — Fact type, payload schema version, and revision

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Versioning](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#versioning)
- Definition: Three independent axes: meaning, historical interpretation, and correction sequence
- Usage boundary: Never collapse these into a generic version
- Conflict disposition: `QT-CONFLICT-001` is `qualified_nonblocking`; `QT-CONFLICT-002` is `qualified_nonblocking`; `QT-CONFLICT-003` is `qualified_nonblocking`; `QT-CONFLICT-004` is `qualified_nonblocking`; `QT-CONFLICT-005` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-005"></a>
### `QT-TERM-005` — Runtime fact, qualified

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `execution-runtime`
- Required reviewers: `data`, `execution-runtime`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — Shared-Wallet Entry Ordering](01_runtime_contract.md#shared-wallet-entry-ordering)
  - `accepted_adr` — [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
- Definition: `Runtime fact` is qualified vocabulary for a fact owned by a named runtime boundary, such as a lifecycle event or Wallet Ledger fact.
- Usage boundary: A runtime fact is not a Canonical Fact. Always retain the owning qualifier; this term does not create a universal runtime-fact store or authority.
- Conflict disposition: `QT-CONFLICT-009` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-006"></a>
### `QT-TERM-006` — Research Observation

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `research-memory`
- Required reviewers: `platform-contract`, `research-memory`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Decision](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#decision)
  - `accepted_adr` — [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Supersession Scope](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#supersession-scope)
  - `accepted_adr` — [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
- Definition: A Research Observation is a durable research-memory item. The Check-to-Observation path is a separate explicit operation and admits only completed, frozen, replayable, eligible Check evidence.
- Usage boundary: Manual Observation creation remains a separately owned and admitted path. Ordinary market observations and durable Research Observations are distinct; legacy V1 Check/Observation records remain readable and are not upgraded by this definition.
- Conflict disposition: `QT-CONFLICT-007` is `resolved_by_authority`; `QT-CONFLICT-010` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-007"></a>
### `QT-TERM-007` — Frozen Dataset

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0051-require-frozen-datasets-for-canonical-backtests.md — Decision](../../architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md#decision)
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Dataset And Research Semantics](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#dataset-and-research-semantics)
- Definition: Immutable `market_dataset.v1` manifest over exact Fact revisions, ranges, sources, hashes, gaps, and watermark
- Usage boundary: Use the qualified label for the immutable source-data manifest; bare `Dataset` does not identify an owner or scope.
- Conflict disposition: `QT-CONFLICT-008` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-008"></a>
### `QT-TERM-008` — Frozen Market Data Read Binding

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`, `research-orchestration`
- Consulted boundaries: `execution-runtime`
- Authority clauses:
  - `accepted_adr` — [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
- Definition: Exact consumer binding to a Frozen Dataset and its subjects/ranges/revisions/hashes/gaps
- Usage boundary: A binding is not a Dataset or readiness certificate
- Conflict disposition: none recorded for this term.

<a id="qt-term-009"></a>
### `QT-TERM-009` — RunResearchDataset

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `reporting`
- Required reviewers: `platform-contract`, `reporting`
- Authority clauses:
  - `accepted_adr` — [0010-use-run-research-dataset-as-reporting-contract.md — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)
- Definition: Canonical reporting/read-model product derived from durable run truth
- Usage boundary: Not a Frozen Dataset; does not own source-data truth
- Conflict disposition: `QT-CONFLICT-008` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-010"></a>
### `QT-TERM-010` — Reporting dataset readiness

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `reporting`
- Required reviewers: `platform-contract`, `reporting`
- Authority clauses:
  - `accepted_adr` — [0010-use-run-research-dataset-as-reporting-contract.md — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)
- Definition: Materialization status/readiness of RunResearchDataset
- Usage boundary: Does not certify source Dataset completeness or science
- Conflict disposition: `QT-CONFLICT-008` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-011"></a>
### `QT-TERM-011` — Check

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `research-orchestration`
- Required reviewers: `platform-contract`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0060-use-capability-native-research-and-collection-contracts.md — Decision](../../architecture/decisions/0060-use-capability-native-research-and-collection-contracts.md#decision)
  - `accepted_adr` — [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
- Definition: Bounded analytical operation over declared inputs
- Usage boundary: Not acquisition, Indicator evaluation, Strategy decision, or promotion authority
- Conflict disposition: `QT-CONFLICT-007` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-012"></a>
### `QT-TERM-012` — Check preview

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `research-orchestration`
- Required reviewers: `platform-contract`, `research-memory`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Decision](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#decision)
  - `accepted_adr` — [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Supersession Scope](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#supersession-scope)
- Definition: A Check preview is ephemeral analysis. It persists no Check evidence and cannot create, link, or support a Research Observation.
- Usage boundary: Preview output is neither frozen evidence nor Research-Observation-eligible evidence.
- Conflict disposition: `QT-CONFLICT-007` is `resolved_by_authority`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-013"></a>
### `QT-TERM-013` — Frozen Check evidence

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `research-orchestration`
- Required reviewers: `platform-contract`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
  - `accepted_adr` — [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Decision](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#decision)
- Definition: Frozen Check evidence is a completed Check execution bound to immutable, provider-free, replayable inputs.
- Usage boundary: Not a scientific verdict
- Conflict disposition: `QT-CONFLICT-007` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-014"></a>
### `QT-TERM-014` — Known-at

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `platform`
- Required reviewers: `platform`, `platform-contract`
- Consulted boundaries: `data`, `engine`
- Authority clauses:
  - `normative_platform_contract` — [00_system_contract.md — Core Invariants](00_system_contract.md#core-invariants)
  - `accepted_adr` — [0044-enforce-known-at-prefix-invariance.md — Decision](../../architecture/decisions/0044-enforce-known-at-prefix-invariance.md#decision)
- Definition: Causal availability clock used at an evaluation boundary
- Usage boundary: Not provider effective, publication, funding, valuation, receipt, or attestation time unless a schema says so
- Conflict disposition: none recorded for this term.

<a id="qt-term-015"></a>
### `QT-TERM-015` — Gap Evidence

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Provenance And Causal Time](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#provenance-and-causal-time)
  - `normative_platform_contract` — [01_runtime_contract.md — BotLens Candle Continuity Audit Surface](01_runtime_contract.md#botlens-candle-continuity-audit-surface)
- Definition: Explicit source-data range-quality evidence separate from a Fact
- Usage boundary: Bare Gap is insufficient because several gap namespaces coexist
- Conflict disposition: `QT-CONFLICT-014` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-016"></a>
### `QT-TERM-016` — Candle continuity gap

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Consulted boundaries: `execution-runtime`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — BotLens Candle Continuity Audit Surface](01_runtime_contract.md#botlens-candle-continuity-audit-surface)
  - `accepted_adr` — [0050-use-one-canonical-append-only-market-data-store.md — Decision](../../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md#decision)
- Definition: One exact candle-continuity classification
- Usage boundary: Use expected-session, provider-missing, ingestion-failure, runtime-missing, projection-missing, or unknown
- Conflict disposition: `QT-CONFLICT-014` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-017"></a>
### `QT-TERM-017` — Order-book sequence gap

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)
- Definition: Missing sequence evidence that invalidates book reconstruction
- Usage boundary: Not candle coverage evidence
- Conflict disposition: `QT-CONFLICT-014` is `qualified_nonblocking`; `QT-CONFLICT-015` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-018"></a>
### `QT-TERM-018` — Runner clock gap / overlay clock gap

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Consulted boundaries: `indicator-runtime`
- Authority clauses:
  - `accepted_adr` — [0021-use-runner-clock-gap-sentinel.md — Decision](../../architecture/decisions/0021-use-runner-clock-gap-sentinel.md#decision)
  - `accepted_adr` — [0038-decouple-visual-overlay-projection-from-runtime-push.md — Decision](../../architecture/decisions/0038-decouple-visual-overlay-projection-from-runtime-push.md#decision)
- Definition: Separate runner-liveness and projection-invalidation gaps
- Usage boundary: Not source-data gaps
- Conflict disposition: `QT-CONFLICT-014` is `qualified_nonblocking`; `QT-CONFLICT-022` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-019"></a>
### `QT-TERM-019` — Book Validity Interval

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)
- Definition: Interval over which reconstructed book state is valid
- Usage boundary: Avoid generic “valid interval”
- Conflict disposition: `QT-CONFLICT-014` is `qualified_nonblocking`; `QT-CONFLICT-015` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-020"></a>
### `QT-TERM-020` — Raw record

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
- Definition: One provider frame record with deterministic `raw_record_id`
- Usage boundary: Not a raw archive object or Canonical Fact revision
- Conflict disposition: `QT-CONFLICT-014` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-021"></a>
### `QT-TERM-021` — Raw archive object / manifest

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)
- Definition: Durable placement/manifest for one or more preassigned raw records
- Usage boundary: Preserve record-versus-object distinction
- Conflict disposition: `QT-CONFLICT-014` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-022"></a>
### `QT-TERM-022` — Bot and Bot Run

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Consulted boundaries: `persistence`
- Authority clauses:
  - `accepted_adr` — [0030-keep-portal-bots-definition-only.md — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)
  - `accepted_adr` — [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
- Definition: Bot is a durable definition; Bot Run owns lifecycle, lease, events, and report identity
- Usage boundary: Never assign run lifecycle to the Bot row
- Conflict disposition: none recorded for this term.

<a id="qt-term-023"></a>
### `QT-TERM-023` — Run, qualified

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `platform`
- Required reviewers: `data`, `execution-runtime`, `platform`, `platform-contract`, `reporting`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0030-keep-portal-bots-definition-only.md — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)
  - `accepted_adr` — [0039-use-shared-async-jobs-for-research-dispatch.md — Decision](../../architecture/decisions/0039-use-shared-async-jobs-for-research-dispatch.md#decision)
  - `accepted_adr` — [0052-use-typed-fact-collectors-and-explicit-instrument-roles.md — Decision](../../architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md#decision)
  - `accepted_adr` — [0031-fingerprint-reports-and-slim-runtime-storage.md — Decision](../../architecture/decisions/0031-fingerprint-reports-and-slim-runtime-storage.md#decision)
- Definition: `Run` has no universal platform object; qualify the owning operation, for example Bot Run, Research Check execution, collection attempt, or report materialization.
- Usage boundary: A shared identifier or time range does not merge lifecycle, evidence, or authority between those operations.
- Conflict disposition: none recorded for this term.

<a id="qt-term-024"></a>
### `QT-TERM-024` — Report, qualified

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `reporting`
- Required reviewers: `platform-contract`, `reporting`
- Authority clauses:
  - `accepted_adr` — [0010-use-run-research-dataset-as-reporting-contract.md — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)
  - `accepted_adr` — [0031-fingerprint-reports-and-slim-runtime-storage.md — Decision](../../architecture/decisions/0031-fingerprint-reports-and-slim-runtime-storage.md#decision)
- Definition: Separate reporting concepts and artifacts
- Usage boundary: Report status cannot alter Bot Run status
- Conflict disposition: `QT-CONFLICT-008` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-025"></a>
### `QT-TERM-025` — Live runtime/composition

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [02_execution_playback_contract.md — Runtime Mode And External-Submission Authority](02_execution_playback_contract.md#runtime-mode-and-external-submission-authority)
  - `accepted_adr` — [0049-keep-live-order-submission-closed.md — Decision](../../architecture/decisions/0049-keep-live-order-submission-closed.md#decision)
- Definition: A live composition/runtime label, without venue-trading authority
- Usage boundary: “Live” does not mean external order submission
- Conflict disposition: `QT-CONFLICT-025` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-026"></a>
### `QT-TERM-026` — Evidence, qualified

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `platform`
- Required reviewers: `data`, `execution-runtime`, `platform`, `platform-contract`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
  - `accepted_adr` — [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
  - `accepted_adr` — [0062-use-frozen-bindings-for-durable-check-evidence.md — Decision](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#decision)
  - `accepted_adr` — [0059-use-in-app-scientific-authority-and-offline-certification-ceiling.md — Decision](../../architecture/decisions/0059-use-in-app-scientific-authority-and-offline-certification-ceiling.md#decision)
- Definition: `Evidence` has no universal platform meaning. Qualify source/receipt evidence, source-data quality or gap evidence, runtime-lifecycle evidence, Check evidence, and scientific evidence by their owning boundary.
- Usage boundary: Evidence in one boundary grants no readiness, certification, or authority owned by another boundary.
- Conflict disposition: none recorded for this term.

<a id="qt-term-027"></a>
### `QT-TERM-027` — Provider ID / Venue ID / strategy datasource and exchange

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `data`
- Required reviewers: `data`, `decision-layer`, `execution-runtime`, `identity`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
- Definition: Provider ID and Venue ID belong to provider selection, credentials, and instrument admission. At the Strategy boundary, `datasource` and `exchange` are compatibility defaults and lookup hints only.
- Usage boundary: Compatibility fields cannot override a linked canonical instrument or its source routing; this entry does not define provider/venue identity internals or an exchange-slug translation contract.
- Conflict disposition: `QT-CONFLICT-011` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-028"></a>
### `QT-TERM-028` — Canonical Instrument / provider product identity

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `identity`
- Required reviewers: `data`, `decision-layer`, `execution-runtime`, `identity`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
  - `accepted_adr` — [0027-use-execution-profiles-as-runtime-instrument-authority.md — Decision](../../architecture/decisions/0027-use-execution-profiles-as-runtime-instrument-authority.md#decision)
  - `accepted_adr` — [0052-use-typed-fact-collectors-and-explicit-instrument-roles.md — Decision](../../architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md#decision)
- Definition: A Canonical Instrument is the linked platform instrument referenced by `instrument_id`. Provider product IDs and display symbols remain provider/venue-facing lookup identities and do not replace that canonical link.
- Usage boundary: Resolve compatibility symbols or product IDs to the linked canonical instrument before runtime routing; do not treat a display symbol as a globally stable instrument key.
- Conflict disposition: `QT-CONFLICT-012` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-029"></a>
### `QT-TERM-029` — Source Identity / Series Identity

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `data`
- Required reviewers: `data`, `decision-layer`, `execution-runtime`, `identity`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0050-use-one-canonical-append-only-market-data-store.md — Decision](../../architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md#decision)
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Decision](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#decision)
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Provenance And Causal Time](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#provenance-and-causal-time)
  - `normative_platform_contract` — [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
- Definition: Source Identity records acquisition provenance for a canonical Fact. Series Identity names the canonical logical fact stream whose exact identity is defined by the owning market-data contract.
- Usage boundary: Provider, source, series, and instrument identities remain distinct. Compatibility datasource or exchange values do not override canonical linked identities.
- Conflict disposition: `QT-CONFLICT-011` is `qualified_nonblocking`; `QT-CONFLICT-012` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-030"></a>
### `QT-TERM-030` — Collector Definition / collector adapter / collector worker

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Consulted boundaries: `platform`
- Authority clauses:
  - `accepted_adr` — [0064-use-one-code-owned-collector-operations-contract.md — Decision](../../architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md#decision)
- Definition: Definition is durable reviewed configuration; adapter is code that implements one stream contract; worker is the running owner of a definition
- Usage boundary: Bare Collector is unsafe when configuration, code, and process ownership differ
- Conflict disposition: `QT-CONFLICT-013` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-031"></a>
### `QT-TERM-031` — Collector definition admission / collector operation

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Consulted boundaries: `platform`
- Authority clauses:
  - `accepted_adr` — [0064-use-one-code-owned-collector-operations-contract.md — Decision](../../architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md#decision)
- Definition: Collector definition admission applies the reviewed configured gate to a durable collector definition. A collector operation requests a lifecycle action against admitted configuration.
- Usage boundary: `Enrollment` is compatibility wording only. Admission does not start a collector, and operation success does not prove subsequent worker readiness.
- Conflict disposition: `QT-CONFLICT-013` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-032"></a>
### `QT-TERM-032` — Qualified Coverage

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Provenance And Causal Time](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#provenance-and-causal-time)
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Dataset And Research Semantics](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#dataset-and-research-semantics)
- Definition: Acquisition coverage, trade-stream coverage intervals, archive-complete mapping evidence, and Frozen Dataset scope are distinct evidence.
- Usage boundary: Completeness in one named evidence boundary does not certify another.
- Conflict disposition: `QT-CONFLICT-014` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-033"></a>
### `QT-TERM-033` — Archive mapping / archive coverage / retention pin

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Invariants](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#invariants)
- Definition: Mapping links raw records to immutable archive objects; archive coverage proves durable placement over a range; a retention pin prevents eligible object expiry
- Usage boundary: None is a Canonical Fact, Dataset freeze, or book-validity certificate
- Conflict disposition: `QT-CONFLICT-014` is `qualified_nonblocking`; `QT-CONFLICT-015` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-034"></a>
### `QT-TERM-034` — Reconstructed Book State / book checkpoint / execution-book tape

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `data`
- Required reviewers: `data`, `execution-runtime`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0053-use-tiered-market-structure-archive-and-replay-boundary.md — Decision](../../architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md#decision)
  - `accepted_adr` — [0058-use-replay-certified-execution-book-tapes.md — Decision](../../architecture/decisions/0058-use-replay-certified-execution-book-tapes.md#decision)
- Definition: Book state is disposable current reconstruction; checkpoint accelerates deterministic replay; a certified execution tape is a frozen runtime input over causal snapshots
- Usage boundary: A checkpoint or tape is not provider truth, and hot book state is never Dataset truth
- Conflict disposition: `QT-CONFLICT-015` is `qualified_nonblocking`; `QT-CONFLICT-020` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-036"></a>
### `QT-TERM-036` — Typed Indicator Output / output catalog / output readiness

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `indicator-runtime`
- Required reviewers: `indicator-runtime`, `platform-contract`
- Consulted boundaries: `decision-layer`
- Authority clauses:
  - `accepted_adr` — [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Decision](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#decision)
  - `accepted_adr` — [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Guardrails](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#guardrails)
- Definition: Catalog declares every public output; each bar returns every declared typed output; `ready=false` means present but not yet usable
- Usage boundary: Output preference or visibility must not rewrite Indicator truth
- Conflict disposition: `QT-CONFLICT-017` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-037"></a>
### `QT-TERM-037` — Indicator lifecycle output

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `indicator-runtime`
- Required reviewers: `indicator-runtime`, `platform-contract`
- Consulted boundaries: `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0004-separate-indicator-truth-from-projections.md — Decision](../../architecture/decisions/0004-separate-indicator-truth-from-projections.md#decision)
  - `accepted_adr` — [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Decision](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#decision)
- Definition: Indicator lifecycle output is a qualified name for a declared public typed Indicator output that reports lifecycle-like state.
- Usage boundary: The owning output contract defines its exact values. It is not a Bot Run, order, trade, collector, or deployment lifecycle event and does not turn projections into Strategy inputs.
- Conflict disposition: `QT-CONFLICT-017` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-038"></a>
### `QT-TERM-038` — Overlay contract / overlay snapshot / overlay delta

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `indicator-runtime`
- Required reviewers: `indicator-runtime`, `platform-contract`
- Consulted boundaries: `botlens-projections`
- Authority clauses:
  - `accepted_adr` — [0004-separate-indicator-truth-from-projections.md — Decision](../../architecture/decisions/0004-separate-indicator-truth-from-projections.md#decision)
  - `accepted_adr` — [0038-decouple-visual-overlay-projection-from-runtime-push.md — Decision](../../architecture/decisions/0038-decouple-visual-overlay-projection-from-runtime-push.md#decision)
- Definition: Contract names renderable payload semantics; snapshot is full current visual state; delta is bounded projection transport with its own clock
- Usage boundary: Overlays are projections, never Strategy inputs or canonical execution state
- Conflict disposition: `QT-CONFLICT-016` is `qualified_nonblocking`; `QT-CONFLICT-017` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-039"></a>
### `QT-TERM-039` — Signal ID / Decision ID / order and fill IDs

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `decision-layer`
- Required reviewers: `decision-layer`, `execution-runtime`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0005-keep-strategy-decisions-separate-from-execution.md — Decision](../../architecture/decisions/0005-keep-strategy-decisions-separate-from-execution.md#decision)
  - `accepted_adr` — [0041-use-canonical-execution-plan-and-order-fill-semantics.md — Decision](../../architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md#decision)
  - `accepted_adr` — [0057-use-append-only-canonical-order-lifecycle.md — Decision](../../architecture/decisions/0057-use-append-only-canonical-order-lifecycle.md#decision)
- Definition: Distinct causal identities link an Indicator signal to a Strategy decision and later execution artifacts without aliasing them
- Usage boundary: Sharing provenance does not make signal, decision, order, attempt, event, and fill the same object
- Conflict disposition: `QT-CONFLICT-018` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-040"></a>
### `QT-TERM-040` — Strategy definition / Compiled Strategy / effective strategy / run strategy snapshot

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `decision-layer`
- Required reviewers: `decision-layer`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0018-use-output-filters-as-strategy-variant-contract.md — Decision](../../architecture/decisions/0018-use-output-filters-as-strategy-variant-contract.md#decision)
  - `accepted_adr` — [0005-keep-strategy-decisions-separate-from-execution.md — Decision](../../architecture/decisions/0005-keep-strategy-decisions-separate-from-execution.md#decision)
- Definition: Authored rules compile to executable semantics; variant resolution produces the effective strategy; run start freezes the exact snapshot
- Usage boundary: “Strategy” must not conceal which stage or frozen identity is meant
- Conflict disposition: `QT-CONFLICT-019` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-041"></a>
### `QT-TERM-041` — Strategy Variant / Output Filter

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `decision-layer`
- Required reviewers: `decision-layer`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0018-use-output-filters-as-strategy-variant-contract.md — Decision](../../architecture/decisions/0018-use-output-filters-as-strategy-variant-contract.md#decision)
- Definition: A Variant is a named diff whose output filters add deterministic conditions over public outputs already attached to the base Strategy
- Usage boundary: A Variant does not own ATM selection, Indicator config, or a second evaluator
- Conflict disposition: `QT-CONFLICT-019` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-042"></a>
### `QT-TERM-042` — SeriesExecutionProfile (compatibility compiler)

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `identity`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
  - `accepted_adr` — [0027-use-execution-profiles-as-runtime-instrument-authority.md — Decision](../../architecture/decisions/0027-use-execution-profiles-as-runtime-instrument-authority.md#decision)
  - `accepted_adr` — [0056-pin-venue-neutral-execution-contexts-per-run.md — Decision](../../architecture/decisions/0056-pin-venue-neutral-execution-contexts-per-run.md#decision)
- Definition: `SeriesExecutionProfile` is the compatibility compiler for current instrument, risk, margin, and legacy fee inputs. It is not `InstrumentExecutionContract` or the immutable run-scoped `ResolvedExecutionContext`.
- Usage boundary: Reject Instrument Execution Profile as an umbrella. `InstrumentExecutionContract` remains one distinct constituent of the resolved context, and `ResolvedExecutionContext` is the final immutable run authority.
- Conflict disposition: `QT-CONFLICT-020` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-043"></a>
### `QT-TERM-043` — Resolved Execution Context / context bundle

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `identity`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
  - `accepted_adr` — [0056-pin-venue-neutral-execution-contexts-per-run.md — Decision](../../architecture/decisions/0056-pin-venue-neutral-execution-contexts-per-run.md#decision)
- Definition: Immutable per-series binding of exact execution contracts; a bundle pins one context per runtime series for a run
- Usage boundary: Context binds execution assumptions; it does not own Strategy meaning, accounting, or authorization
- Conflict disposition: `QT-CONFLICT-020` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-044"></a>
### `QT-TERM-044` — Runtime Execution Plan / Canonical Order Request

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0041-use-canonical-execution-plan-and-order-fill-semantics.md — Decision](../../architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md#decision)
  - `accepted_adr` — [0057-use-append-only-canonical-order-lifecycle.md — Decision](../../architecture/decisions/0057-use-append-only-canonical-order-lifecycle.md#decision)
- Definition: Plan is the run-time intended action; Canonical Order Request begins durable order quantity and policy custody
- Usage boundary: Neither is a fill, and the plan is not the durable order lifecycle
- Conflict disposition: `QT-CONFLICT-020` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-045"></a>
### `QT-TERM-045` — Canonical Order Lifecycle / `FillOrder` / fill

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0057-use-append-only-canonical-order-lifecycle.md — Decision](../../architecture/decisions/0057-use-append-only-canonical-order-lifecycle.md#decision)
  - `accepted_adr` — [0041-use-canonical-execution-plan-and-order-fill-semantics.md — Decision](../../architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md#decision)
- Definition: Lifecycle is append-only durable order truth; `FillOrder` is a compatibility request to an execution adapter; fill is one executed quantity event
- Usage boundary: Never describe `FillOrder` as the durable order or a fill as the whole order
- Conflict disposition: `QT-CONFLICT-018` is `qualified_nonblocking`; `QT-CONFLICT-020` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-046"></a>
### `QT-TERM-046` — Wallet state / Wallet Ledger fact / wallet commit clock

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Consulted boundaries: `persistence`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — Shared-Wallet Entry Ordering](01_runtime_contract.md#shared-wallet-entry-ordering)
  - `accepted_adr` — [0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md — Decision](../../architecture/decisions/0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md#decision)
  - `accepted_adr` — [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
- Definition: Wallet state is current capital truth; ledger facts are replayable transitions; `wallet_commit_seq` orders shared-wallet mutation
- Usage boundary: Report/BotLens wallet views and runtime-event append order are not alternate wallet truth
- Conflict disposition: `QT-CONFLICT-021` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-047"></a>
### `QT-TERM-047` — BotLens projection

- Adoption status: `adopted`
- Entry kind: `domain_term`
- Owner: `botlens-projections`
- Required reviewers: `botlens-projections`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — BotLens Readiness Semantics](01_runtime_contract.md#botlens-readiness-semantics)
  - `accepted_adr` — [0008-treat-botlens-as-projection-debugger.md — Decision](../../architecture/decisions/0008-treat-botlens-as-projection-debugger.md#decision)
  - `accepted_adr` — [0055-separate-bounded-botlens-hot-state-from-durable-inspection.md — Decision](../../architecture/decisions/0055-separate-bounded-botlens-hot-state-from-durable-inspection.md#decision)
- Definition: Projection is bounded debugger state derived from runtime facts; bootstrap establishes a base; snapshot is the current run- or symbol-scoped read model
- Usage boundary: BotLens never creates execution truth; empty projection is not implicit readiness
- Conflict disposition: `QT-CONFLICT-022` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-048"></a>
### `QT-TERM-048` — Qualified Cursor

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `identity`
- Required reviewers: `botlens-projections`, `execution-runtime`, `identity`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0007-use-scoped-causal-clocks-for-runtime-replay.md — Decision](../../architecture/decisions/0007-use-scoped-causal-clocks-for-runtime-replay.md#decision)
  - `normative_platform_contract` — [01_runtime_contract.md — Shared-Wallet Entry Ordering](01_runtime_contract.md#shared-wallet-entry-ordering)
- Definition: `run_seq`, `wallet_commit_seq`, `position_commit_seq`, `indicator_commit_seq`, `overlay_commit_seq`, and selected-symbol stream `base_seq` are distinct owner-scoped ordered positions.
- Usage boundary: Bare `cursor` or `sequence` is contextual shorthand only; do not compare or substitute positions from different owners.
- Conflict disposition: `QT-CONFLICT-022` is `qualified_nonblocking`; `QT-CONFLICT-024` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-049"></a>
### `QT-TERM-049` — Readiness, qualified

- Adoption status: `adopted`
- Entry kind: `qualification_rule`
- Owner: `platform`
- Required reviewers: `botlens-projections`, `indicator-runtime`, `platform`, `platform-contract`, `reporting`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — BotLens Readiness Semantics](01_runtime_contract.md#botlens-readiness-semantics)
  - `accepted_adr` — [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Guardrails](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#guardrails)
  - `accepted_adr` — [0010-use-run-research-dataset-as-reporting-contract.md — Decision](../../architecture/decisions/0010-use-run-research-dataset-as-reporting-contract.md#decision)
- Definition: Indicator output readiness, BotLens projection readiness, reporting readiness, comparison readiness, and golden-candidate readiness are distinct owner-scoped states.
- Usage boundary: `Ready` must name its owning boundary; one readiness result does not confer certification or authority at another.
- Conflict disposition: `QT-CONFLICT-008` is `qualified_nonblocking`; `QT-CONFLICT-014` is `qualified_nonblocking`; `QT-CONFLICT-022` is `qualified_nonblocking`; `QT-CONFLICT-026` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-050"></a>
### `QT-TERM-050` — Lease / claim / ownership fence

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `persistence`
- Required reviewers: `execution-runtime`, `persistence`, `platform-contract`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0047-fence-async-job-ownership.md — Decision](../../architecture/decisions/0047-fence-async-job-ownership.md#decision)
  - `accepted_adr` — [0025-use-per-run-leases.md — Decision](../../architecture/decisions/0025-use-per-run-leases.md#decision)
- Definition: Lease or claim grants time-bounded work ownership; the fence is the token-and-generation check that prevents a stale owner from committing
- Usage boundary: A row lock, heartbeat, lease timestamp, token, and generation are related but not synonyms
- Conflict disposition: `QT-CONFLICT-023` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-051"></a>
### `QT-TERM-051` — Async Job / job-owned effect

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `research-orchestration`
- Required reviewers: `platform-contract`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0039-use-shared-async-jobs-for-research-dispatch.md — Decision](../../architecture/decisions/0039-use-shared-async-jobs-for-research-dispatch.md#decision)
  - `accepted_adr` — [0047-fence-async-job-ownership.md — Decision](../../architecture/decisions/0047-fence-async-job-ownership.md#decision)
- Definition: Durable queue item whose current fenced claim may append its result and associated domain effects atomically
- Usage boundary: A job is neither the Check it dispatches nor the worker process that claims it
- Conflict disposition: `QT-CONFLICT-023` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-052"></a>
### `QT-TERM-052` — Semantic fingerprint / operational fingerprint

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `reporting`
- Required reviewers: `platform-contract`, `reporting`
- Authority clauses:
  - `accepted_adr` — [0015-split-semantic-and-operational-golden-fingerprints.md — Decision](../../architecture/decisions/0015-split-semantic-and-operational-golden-fingerprints.md#decision)
- Definition: Semantic fingerprint covers stable trading behavior and material identity; operational fingerprint covers diagnostics, ordering, availability, and runtime drift
- Usage boundary: Operational drift can coexist with semantic equivalence
- Conflict disposition: `QT-CONFLICT-024` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-053"></a>
### `QT-TERM-053` — Report input fingerprint / data snapshot hash

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `reporting`
- Required reviewers: `platform-contract`, `reporting`
- Authority clauses:
  - `accepted_adr` — [0031-fingerprint-reports-and-slim-runtime-storage.md — Decision](../../architecture/decisions/0031-fingerprint-reports-and-slim-runtime-storage.md#decision)
  - `accepted_adr` — [0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md — Decision](../../architecture/decisions/0046-fingerprint-exact-candle-inputs-and-keep-quality-separate.md#decision)
- Definition: Input fingerprint validates one report materialization against durable run inputs; data snapshot hash identifies exact runtime-consumed data material
- Usage boundary: Neither is the semantic or operational fingerprint
- Conflict disposition: `QT-CONFLICT-024` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

<a id="qt-term-054"></a>
### `QT-TERM-054` — Deployment contract / strategy deployment authority

- Adoption status: `adopted`
- Entry kind: `contrast_set`
- Owner: `platform`
- Required reviewers: `platform`, `platform-contract`
- Consulted boundaries: `security`
- Authority clauses:
  - `normative_platform_contract` — [02_execution_playback_contract.md — Runtime Mode And External-Submission Authority](02_execution_playback_contract.md#runtime-mode-and-external-submission-authority)
  - `accepted_adr` — [0049-keep-live-order-submission-closed.md — Decision](../../architecture/decisions/0049-keep-live-order-submission-closed.md#decision)
- Definition: Platform deployment names deployment of QT's platform/runtime composition. Strategy deployment authority would separately permit an artifact to control external execution.
- Usage boundary: Successful deployment, configuration, credentials, or a live label does not grant research-promotion or external-order authority.
- Conflict disposition: `QT-CONFLICT-025` is `qualified_nonblocking`. Historical evidence and separate documentation repairs remain preserved.

## Ratified Alias Rules

Ratified aliases are reviewed usage rules, not additional canonical terms.
Historical, discouraged, and rejected spellings remain preserved in historical
evidence and are never automatic replacements.

<a id="qt-alias-001"></a>
### `QT-ALIAS-001` — `candle_versions`, `numeric_fact_versions`, `family persistence tables`

- Review status: `ratified`
- Classification: `historical`
- Canonical term references: [`QT-TERM-001`](#qt-term-001)
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Migration And Cutover](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#migration-and-cutover)
- Scope and handling: Retain only in migration history; current persistence vocabulary is the canonical Fact store.
- Automatic replacement: `false`

<a id="qt-alias-002"></a>
### `QT-ALIAS-002` — `NUMERIC_FACT_CONSOLIDATION_DEFERRED`

- Review status: `ratified`
- Classification: `historical`
- Canonical term references: [`QT-TERM-001`](#qt-term-001)
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Status](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#status)
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Migration And Cutover](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#migration-and-cutover)
- Scope and handling: Treat as a superseded historical claim; it does not describe the current hard-cutover state.
- Automatic replacement: `false`

<a id="qt-alias-003"></a>
### `QT-ALIAS-003` — `legacy_market_v1`

- Review status: `ratified`
- Classification: `historical`
- Canonical term references: [`QT-TERM-001`](#qt-term-001)
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Migration And Cutover](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#migration-and-cutover)
- Scope and handling: Retain as migration/archive lineage only; it is not a current application read vocabulary.
- Automatic replacement: `false`

<a id="qt-alias-004"></a>
### `QT-ALIAS-004` — `legacy_unpinned`, `legacy_frozen_unverifiable`

- Review status: `ratified`
- Classification: `compatibility`
- Canonical term references: [`QT-TERM-012`](#qt-term-012), [`QT-TERM-013`](#qt-term-013)
- Owner: `research-memory`
- Required reviewers: `platform-contract`, `research-memory`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0065-use-explicit-frozen-check-admission-for-new-research-observations.md — Supersession Scope](../../architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md#supersession-scope)
  - `accepted_adr` — [0062-use-frozen-bindings-for-durable-check-evidence.md — Rejected Alternatives](../../architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md#rejected-alternatives)
- Scope and handling: Compatibility-readable legacy Check states only; never describe them as current replayable evidence.
- Automatic replacement: `false`

<a id="qt-alias-005"></a>
### `QT-ALIAS-005` — `qt experiments run-bot`

- Review status: `ratified`
- Classification: `compatibility`
- Canonical term references: [`QT-TERM-011`](#qt-term-011), [`QT-TERM-022`](#qt-term-022)
- Owner: `research-orchestration`
- Required reviewers: `execution-runtime`, `platform-contract`, `research-orchestration`
- Authority clauses:
  - `accepted_adr` — [0060-use-capability-native-research-and-collection-contracts.md — Decision](../../architecture/decisions/0060-use-capability-native-research-and-collection-contracts.md#decision)
  - `accepted_adr` — [0030-keep-portal-bots-definition-only.md — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)
- Scope and handling: Treat `qt experiments run-bot` as a deprecated compatibility spelling. Prefer the current explicit Bot-start and collection workflow; the alias does not merge Check and Bot Run semantics.
- Automatic replacement: `false`

<a id="qt-alias-006"></a>
### `QT-ALIAS-006` — `instant`, `walk-forward`, `walkforward`

- Review status: `ratified`
- Classification: `rejected`
- Canonical term references: [`QT-TERM-025`](#qt-term-025)
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [02_execution_playback_contract.md — Execution Mode Policy](02_execution_playback_contract.md#execution-mode-policy)
  - `normative_platform_contract` — [02_execution_playback_contract.md — Playback Contract](02_execution_playback_contract.md#playback-contract)
- Scope and handling: These are playback vocabulary, not valid execution-mode names.
- Automatic replacement: `false`

<a id="qt-alias-007"></a>
### `QT-ALIAS-007` — `legacy Bot-row lifecycle fields`

- Review status: `ratified`
- Classification: `historical`
- Canonical term references: [`QT-TERM-022`](#qt-term-022)
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0030-keep-portal-bots-definition-only.md — Decision](../../architecture/decisions/0030-keep-portal-bots-definition-only.md#decision)
  - `accepted_adr` — [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
- Scope and handling: Retain for lineage only; Bot Run and the runtime event ledger own lifecycle truth.
- Automatic replacement: `false`

<a id="qt-alias-008"></a>
### `QT-ALIAS-008` — `Fact family table`

- Review status: `ratified`
- Classification: `historical`
- Canonical term references: [`QT-TERM-001`](#qt-term-001)
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0063-use-schema-registered-canonical-facts.md — Migration And Cutover](../../architecture/decisions/0063-use-schema-registered-canonical-facts.md#migration-and-cutover)
- Scope and handling: Retain only in historical phase and migration documents; exclude from current storage vocabulary.
- Automatic replacement: `false`

<a id="qt-alias-009"></a>
### `QT-ALIAS-009` — `datasource`

- Review status: `ratified`
- Classification: `compatibility`
- Canonical term references: [`QT-TERM-027`](#qt-term-027), [`QT-TERM-029`](#qt-term-029)
- Owner: `data`
- Required reviewers: `data`, `decision-layer`, `execution-runtime`, `identity`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
- Scope and handling: `datasource` may remain a Strategy input default or lookup hint. It is neither Provider ID nor canonical Source Identity and cannot override a linked canonical instrument.
- Automatic replacement: `false`

<a id="qt-alias-010"></a>
### `QT-ALIAS-010` — `exchange`

- Review status: `ratified`
- Classification: `compatibility`
- Canonical term references: [`QT-TERM-027`](#qt-term-027)
- Owner: `data`
- Required reviewers: `data`, `decision-layer`, `execution-runtime`, `identity`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
- Scope and handling: `exchange` may remain a Strategy input default or lookup hint. It is not canonical Venue ID authority and cannot override a linked canonical instrument.
- Automatic replacement: `false`

<a id="qt-alias-011"></a>
### `QT-ALIAS-011` — `symbol as Instrument ID`, `product ID as Instrument ID`

- Review status: `ratified`
- Classification: `rejected`
- Canonical term references: [`QT-TERM-028`](#qt-term-028)
- Owner: `identity`
- Required reviewers: `data`, `decision-layer`, `execution-runtime`, `identity`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — Instrument Source vs Execution Semantics](01_runtime_contract.md#instrument-source-vs-execution-semantics)
  - `accepted_adr` — [0027-use-execution-profiles-as-runtime-instrument-authority.md — Decision](../../architecture/decisions/0027-use-execution-profiles-as-runtime-instrument-authority.md#decision)
  - `accepted_adr` — [0052-use-typed-fact-collectors-and-explicit-instrument-roles.md — Decision](../../architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md#decision)
- Scope and handling: Resolve a symbol or provider product ID through the linked canonical instrument; neither is accepted as canonical `instrument_id`.
- Automatic replacement: `false`

<a id="qt-alias-012"></a>
### `QT-ALIAS-012` — `collector`

- Review status: `ratified`
- Classification: `discouraged`
- Canonical term references: [`QT-TERM-030`](#qt-term-030), [`QT-TERM-031`](#qt-term-031)
- Owner: `data`
- Required reviewers: `data`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0064-use-one-code-owned-collector-operations-contract.md — Decision](../../architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md#decision)
- Scope and handling: Qualify definition, adapter, worker, session, or operation whenever ownership could differ.
- Automatic replacement: `false`

<a id="qt-alias-013"></a>
### `QT-ALIAS-013` — `market-profile`, `market_profile`, `mpf`

- Review status: `ratified`
- Classification: `compatibility`
- Canonical term references: [`QT-TERM-038`](#qt-term-038)
- Owner: `indicator-runtime`
- Required reviewers: `indicator-runtime`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0038-decouple-visual-overlay-projection-from-runtime-push.md — Decision](../../architecture/decisions/0038-decouple-visual-overlay-projection-from-runtime-push.md#decision)
  - `accepted_adr` — [0004-separate-indicator-truth-from-projections.md — Decision](../../architecture/decisions/0004-separate-indicator-truth-from-projections.md#decision)
- Scope and handling: Retain only as existing render-contract spellings; they do not create separate overlay concepts.
- Automatic replacement: `false`

<a id="qt-alias-014"></a>
### `QT-ALIAS-014` — `indicator lifecycle`

- Review status: `ratified`
- Classification: `discouraged`
- Canonical term references: [`QT-TERM-037`](#qt-term-037)
- Owner: `indicator-runtime`
- Required reviewers: `indicator-runtime`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0004-separate-indicator-truth-from-projections.md — Decision](../../architecture/decisions/0004-separate-indicator-truth-from-projections.md#decision)
  - `accepted_adr` — [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Decision](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#decision)
- Scope and handling: Use Indicator lifecycle output only for a declared typed output whose owning contract defines the exact values; do not use it as a generic lifecycle.
- Automatic replacement: `false`

<a id="qt-alias-015"></a>
### `QT-ALIAS-015` — `Variant param_overrides`

- Review status: `ratified`
- Classification: `rejected`
- Canonical term references: [`QT-TERM-041`](#qt-term-041)
- Owner: `decision-layer`
- Required reviewers: `decision-layer`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0018-use-output-filters-as-strategy-variant-contract.md — Decision](../../architecture/decisions/0018-use-output-filters-as-strategy-variant-contract.md#decision)
- Scope and handling: Do not describe param_overrides as the primary Strategy Variant contract; the reviewed contract is Output Filters.
- Automatic replacement: `false`

<a id="qt-alias-016"></a>
### `QT-ALIAS-016` — `FillOrder`

- Review status: `ratified`
- Classification: `compatibility`
- Canonical term references: [`QT-TERM-044`](#qt-term-044), [`QT-TERM-045`](#qt-term-045)
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0041-use-canonical-execution-plan-and-order-fill-semantics.md — Decision](../../architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md#decision)
  - `accepted_adr` — [0057-use-append-only-canonical-order-lifecycle.md — Decision](../../architecture/decisions/0057-use-append-only-canonical-order-lifecycle.md#decision)
- Scope and handling: Retain only as an adapter-facing compatibility request; it is neither the durable order nor a fill.
- Automatic replacement: `false`

<a id="qt-alias-017"></a>
### `QT-ALIAS-017` — `seq`, `cursor`

- Review status: `ratified`
- Classification: `rejected`
- Canonical term references: [`QT-TERM-048`](#qt-term-048)
- Owner: `identity`
- Required reviewers: `identity`, `platform-contract`
- Authority clauses:
  - `accepted_adr` — [0007-use-scoped-causal-clocks-for-runtime-replay.md — Decision](../../architecture/decisions/0007-use-scoped-causal-clocks-for-runtime-replay.md#decision)
  - `accepted_adr` — [0042-use-runtime-event-ledger-as-lifecycle-truth.md — Decision](../../architecture/decisions/0042-use-runtime-event-ledger-as-lifecycle-truth.md#decision)
  - `accepted_adr` — [0038-decouple-visual-overlay-projection-from-runtime-push.md — Decision](../../architecture/decisions/0038-decouple-visual-overlay-projection-from-runtime-push.md#decision)
- Scope and handling: Never imply one global order; qualify the owning run, stream, overlay, wallet, position, source, or row clock.
- Automatic replacement: `false`

<a id="qt-alias-018"></a>
### `QT-ALIAS-018` — `ready`, `healthy`, `complete`, `comparable`, `golden`, `deployable`

- Review status: `ratified`
- Classification: `discouraged`
- Canonical term references: [`QT-TERM-049`](#qt-term-049)
- Owner: `platform`
- Required reviewers: `platform`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [01_runtime_contract.md — BotLens Readiness Semantics](01_runtime_contract.md#botlens-readiness-semantics)
  - `accepted_adr` — [0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md — Guardrails](../../architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md#guardrails)
  - `accepted_adr` — [0015-split-semantic-and-operational-golden-fingerprints.md — Decision](../../architecture/decisions/0015-split-semantic-and-operational-golden-fingerprints.md#decision)
- Scope and handling: Name the exact readiness or certification boundary; one status never certifies the others.
- Automatic replacement: `false`

<a id="qt-alias-019"></a>
### `QT-ALIAS-019` — `fingerprint`, `hash`, `golden hash`

- Review status: `ratified`
- Classification: `discouraged`
- Canonical term references: [`QT-TERM-052`](#qt-term-052), [`QT-TERM-053`](#qt-term-053)
- Owner: `reporting`
- Required reviewers: `platform-contract`, `reporting`
- Authority clauses:
  - `accepted_adr` — [0015-split-semantic-and-operational-golden-fingerprints.md — Decision](../../architecture/decisions/0015-split-semantic-and-operational-golden-fingerprints.md#decision)
  - `accepted_adr` — [0031-fingerprint-reports-and-slim-runtime-storage.md — Decision](../../architecture/decisions/0031-fingerprint-reports-and-slim-runtime-storage.md#decision)
- Scope and handling: Qualify semantic, operational, input, data-snapshot, replay, artifact, or source-tree identity before comparing values.
- Automatic replacement: `false`

<a id="qt-alias-020"></a>
### `QT-ALIAS-020` — `live as deployed trading`, `production as deployed trading`

- Review status: `ratified`
- Classification: `rejected`
- Canonical term references: [`QT-TERM-025`](#qt-term-025), [`QT-TERM-054`](#qt-term-054)
- Owner: `execution-runtime`
- Required reviewers: `execution-runtime`, `platform-contract`
- Authority clauses:
  - `normative_platform_contract` — [02_execution_playback_contract.md — Runtime Mode And External-Submission Authority](02_execution_playback_contract.md#runtime-mode-and-external-submission-authority)
  - `accepted_adr` — [0049-keep-live-order-submission-closed.md — Decision](../../architecture/decisions/0049-keep-live-order-submission-closed.md#decision)
- Scope and handling: A live runtime/composition or platform deployment label never implies external-order authority.
- Automatic replacement: `false`

## Non-Adopted Term Accounting

The following IDs are deliberately absent from the adopted definitions. Their
proposal wording remains non-normative.

| ID | Disposition | Residual authority gap | Revisit condition |
| --- | --- | --- | --- |
| `QT-TERM-035` | `deferred` | No reviewed authority owns the complete Indicator-definition, persisted-config, and private-runtime-instance contrast. ADR 0018 owns only the config portion. | Adopt only after an indicator-runtime owner reviews a discovered component or source-module contract that defines all three concepts and their boundary. |
| `QT-TERM-055` | `deferred` | DRR-03 approves proof topology, but no current normative or reviewed module contract owns one stable vocabulary across Make targets, hosted CI jobs, shell suites, and pytest profiles. | Adopt only after the engineering/testing authority defines that selector taxonomy and records one semantic owner with required reviewers. |
