# Decision Resolution — How QT Works And What Its Owners Must Decide

## Decision Requested

Review the fifteen resolutions in this packet and approve, amend, reject, or
reroute them. Five are derivations from authority QT has already accepted, nine
require owner judgment, and one is an execution/proof program. The
forty Phase 2B routes are accounted for exactly once.

This packet is deliberately organized for a human—and for a future Codex
working on QT—to understand the system before encountering the accounting.
Machine-complete crosswalks remain available, but they are subordinate to the
plain-language model and the small set of real choices.

Approval of this packet would authorize the stated resolutions as inputs to a
later Phase 3 plan. It would **not**:

- change product behavior or a normative document;
- adopt a glossary term or alias;
- activate a guarantee;
- approve or execute a remediation;
- run a proof or create an attestation;
- delete, archive, move, or consolidate repository material; or
- integrate this branch into `develop`.

Phase 3 remains unauthorized. This pass stops for owner approval.

## Review Frame

- Frozen subject: `origin/develop` at
  `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`.
- Campaign branch: `feat/docs-guarantee-reconciliation`.
- Source decision map: 40 routes, 68 proposed remediations, and 9 proof
  ceilings from accepted Phase 2B.
- Guarantee state: 75 of 75 classified, 0 activated.
- Terminology state: 34 proposed, 2 blocked, 19 deferred, 0 adopted.
- Proof state: 85 definitions, 0 attestations and 0 product-proof results.

## QT In One Page

QT is easiest to understand as two ordered axes laid over one causal pipeline:

```text
Authority: platform contracts -> reviewed scoped module contracts -> ADR rationale
           -> explanatory architecture -> implementation/tests
           -> non-normative assurance index

Truth:     source facts -> typed Indicator outputs -> Strategy decisions
           -> execution facts -> durable ledgers
           -> reports / observability / operator UI
```

This is QT's contract-and-decision model, not a claim that every boundary is
fully enforced today. Source-module contracts remain nonactivating until DRR-02
approves discovery and ownership. `AGENTS.md` remains contributor/agent
governance and nonactivating for product guarantees unless DRR-01's reading is
ratified. The 68 remediation records describe the gaps between this model and
complete enforcement/proof.

The governing distinction is:

> Authority says what ought to happen. Runtime evidence says what did happen.
> Proof says what was checked at one commit. None silently substitutes for
> another.

In contract-and-decision terms:

- QT's accepted data model turns provider observations into canonical,
  causally readable Facts with
  explicit source, series, instrument, revision, provenance, and gap identity.
  A frozen Dataset records what QT knew; it does not certify readiness for every
  consumer.
- Indicators publish typed, known-at outputs. Strategies may read those public
  outputs and produce decision artifacts. Indicator lifecycle evidence,
  projections, overlays, and UI state do not become Strategy inputs merely
  because they are visible.
- A Strategy decision is a proposal, not a fill. Bot execution owns ordering,
  fills, fees, margin, wallet effects, settlement, lifecycle, and execution
  events.
- The runtime/persistence model makes execution facts durable and rebuildable.
  Playback,
  reporting, BotLens, observability, and the operator console explain or project
  those facts; they cannot rewrite them.
- Durable research evidence must be frozen and replayable. Preview results are
  exploratory. A Check result does not authorize promotion or live execution.
- Frozen implementation evidence shows simulated execution and observe-only
  paper ingestion. Accepted authority keeps external exchange-order submission
  closed; this is not a claim of a completed repository-wide absence proof.
- The guarantee registry is an audit index. It is not another contract and all
  guarantees remain unactivated.

A useful reading rule is: **Facts are inputs; typed outputs are research inputs;
decisions are proposals; execution events are run truth; reports and UI are
explanations.**

Exact supporting authority: `docs/contracts/README.md:3-24`;
`docs/contracts/platform/00_system_contract.md:5-24`;
`docs/contracts/platform/01_runtime_contract.md:5-15,19-63,75-112,165-195`;
`docs/contracts/platform/02_execution_playback_contract.md:13-26,55-90`; and
`docs/contracts/platform/03_engineering_contract.md:10-36`. Accepted
data/research decisions supporting the Fact, Dataset, and Check model are
`docs/architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md:43-77`,
`docs/architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md:60-79,104-128`,
and
`docs/architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md:47-68,81-95`.

## What The Forty Routes Actually Mean

The route count overstated the number of choices because it mixed three kinds
of work:

| Class | Count | Meaning |
| --- | ---: | --- |
| `derivable` | 5 | Frozen authority already supports one answer; owners must ratify the reading before any later edit |
| `owner-judgment` | 9 | Existing authority leaves a real choice, disposition, or ownership assignment that evidence cannot settle |
| `execution/proof` | 1 | Product meaning is sufficiently clear; the remaining work is denominator construction, environment admission, or proof |
| **Total** | **15** | Forty routes reduced without losing a route |

Nine resolutions contain discretionary owner choices. Six are cross-system
authority or trading/research/execution choices:

1. where canonical ownership and source-module-contract discovery live;
2. which layered source owns the relational-schema enforcement read order;
3. whether frozen evidence, not preview, is the sole admission path to a
   Research Observation;
4. whether `SeriesExecutionProfile` compiles inputs while
   `ResolvedExecutionContext` becomes run-scoped authority;
5. whether QT adopts the existing X0–X5 execution-quality ladder as its common
   vocabulary; and
6. how each glossary proposal and alias is ratified after the substantive
   choices above.

The other three are narrower documentation, frontend-support, and operations
choices: treatment of missing links and an orphan asset, admission of the
currently unwired frontend tests and starter README replacement, and the
supported logging/Grafana operating topology.

Everything else is either a proposed reading of authority already present or
future execution/proof work.

## Nine Owner-Judgment Resolutions At A Glance

| Resolution | Recommended answer | Real alternative | Consequence if deferred |
| --- | --- | --- | --- |
| DRR-02 | Architecture metadata owns semantic components; repository governance maps roles to people/teams; reviewed module contracts are linked and scoped | Central ownership manifest or CODEOWNERS-led routing | Owner slugs and source-module contracts remain provisional/nonactivating |
| DRR-04 | Adopt the observed layered schema stack and its read order | Fund a single migration-manifest authority model | Schema enforcement references remain fragmented and DB remediations cannot close cleanly |
| DRR-07 | Only frozen, replayable Check evidence may support a Research Observation | Permit preview Observations or add separate Observation classes | `QT-REM-004` stays gated and two terms stay blocked |
| DRR-10 | `SeriesExecutionProfile` compiles compatibility inputs; `ResolvedExecutionContext` is immutable run authority | Keep the profile final, or accept unsafe dual authority | Economics/context guarantees remain unresolved |
| DRR-11 | Adopt X0–X5 as the single quality ladder, with attained class limited by weakest evidence | Keep it explanatory or replace it | Execution-quality labels remain non-comparable |
| DRR-12 | Apply factual index/model/CI alignment; retain missing-history notes and the orphan asset as unverified unless owners provide better destinations/lineage | Remove references, provide canonical destinations, or recover lineage | Known documentation residue remains stale/unclear |
| DRR-13 | Replace starter README and admit both tracked JSX suites through a pinned Vitest/jsdom topology | Formally retain the suites as historical unsupported tests | Frontend support and cross-stack proof remain incomplete |
| DRR-14 | Ratify Alloy for native server and no in-process hot path; decide local Promtail status; adopt a reviewed backup/restore workflow or remove unsupported commands | Retire local Promtail, add reviewed wrappers, or adopt another documented tool | Logging topology and Grafana instructions remain conflicting/stale |
| DRR-15 | Ratify terms and aliases individually after substantive decisions | Blanket adoption or rejection | All terms retain their proposed/blocked/deferred state |

The other six resolutions still require owner ratification or program approval:
five proposed derivations must be confirmed, and DRR-03 must be approved as a
future execution/proof protocol.

## Exact-Once Route Consolidation

| Resolution | Class | Original Phase 2B routes |
| --- | --- | --- |
| `DRR-01` Authority and agent-governance scope | `derivable` | `QT-RM-AUTH-001`, `QT-RM-AUTH-003`, `QT-RM-SEC-001` |
| `DRR-02` Canonical ownership and module-contract discovery | `owner-judgment` | `QT-RM-AUTH-002`, `QT-RM-AUTH-005`, `QT-RM-AUTH-006` |
| `DRR-03` Closed-denominator and proof-topology program | `execution/proof` | `QT-RM-AUTH-004`, `QT-RM-AUTH-007`, `QT-RM-DATA-007`, `QT-RM-IND-001`, `QT-RM-IND-002`, `QT-RM-STRAT-001`, `QT-RM-EXPERIMENT-001`, `QT-RM-EXEC-001`, `QT-RM-DOC-007`, `QT-RM-CI-002`, `QT-RM-FRONTEND-003` |
| `DRR-04` Relational-schema authority stack | `owner-judgment` | `QT-RM-DATA-002` |
| `DRR-05` Dataset/Check claim-authority ceiling | `derivable` | `QT-RM-DATA-003` |
| `DRR-06` Canonical market identity and compatibility aliases | `derivable` | `QT-RM-DATA-005` |
| `DRR-07` Check-preview/Observation supersession | `owner-judgment` | `QT-RM-RESEARCH-001` |
| `DRR-08` Market-structure lifecycle and GC026 trace correction | `derivable` | `QT-RM-DATA-001`, `QT-RM-DATA-006` |
| `DRR-09` Current execution-mode and signal-bar meaning | `derivable` | `QT-RM-EXEC-002`, `QT-RM-EXEC-004`, `QT-RM-EXEC-005` |
| `DRR-10` Execution profile versus resolved-context authority | `owner-judgment` | `QT-RM-EXEC-003` |
| `DRR-11` Execution-quality ladder and ceiling owner | `owner-judgment` | `QT-RM-EXEC-006` |
| `DRR-12` Documentation/index/lifecycle/CI reconciliation | `owner-judgment` | `QT-RM-DOC-001`, `QT-RM-DOC-002`, `QT-RM-DOC-003`, `QT-RM-DOC-004`, `QT-RM-DOC-008`, `QT-RM-CI-001` |
| `DRR-13` Supported frontend boundary and test topology | `owner-judgment` | `QT-RM-DOC-005`, `QT-RM-FRONTEND-001`, `QT-RM-FRONTEND-002` |
| `DRR-14` Loki/Grafana operating model | `owner-judgment` | `QT-RM-DOC-006`, `QT-RM-OBS-001` |
| `DRR-15` Glossary ratification policy | `owner-judgment` | `QT-RM-TERM-001` |

Count check: 40 source routes, 40 mapped occurrences, 40 unique route IDs.

## Resolution Cards

### DRR-01 — Authority And Agent-Governance Scope

**Recommended resolution.** Ratify the existing hierarchy: platform contracts
own product behavior; `AGENTS.md` owns contributor and agent workflow and may
summarize product rules, but cannot independently override a platform contract
or activate a guarantee. Architecture roadmaps remain explanatory. Any autonomy
rule intended as product authority belongs in the existing contract hierarchy.
Read ADR 0048 in its stated acceptance scope: offline agent research through
`RESEARCH_CERTIFIED`. Its “every mutation” language applies to those agent
research actions, while non-research mutation surfaces remain outside the ADR.

**Alternatives and consequences.** Owners may promote selected roadmap or
`AGENTS.md` clauses into a platform contract, or broaden ADR 0048 in a new
decision. Either is a normative change for a later phase. Treating these sources
as silent parallel authorities would leave precedence ambiguous and permit
unreviewed guarantee activation.

**Affected.** Candidates `QT-GC-014`, `062`, `063`, `075`; guarantees
`QT-GUAR-AGENT-WORKFLOW-BOUNDARIES`,
`QT-GUAR-ENUMERATED-AGENT-MUTATION-GATES`,
`QT-GUAR-SHARED-APPLICATION-CONTRACT`, and
`QT-GUAR-SOLE-POSTGRES-PERSISTENCE-AUTHORITY`; remediations `QT-REM-008`,
`304`, `305`, `315`.

**Exact authority and supporting evidence.** Authority/decision:
`docs/contracts/README.md:3-7` and
`docs/architecture/decisions/0048-gate-agent-mutation-and-research-promotion.md:27-29,41-50,66-69`.
Supporting governance/explanatory evidence: `docs/README.md:15-17,31`;
`docs/architecture/README.md:79-81,104-107`;
`AGENTS.md:3-9,35-44,191-205,216-239`; and
`docs/architecture/research-orchestration/AUTONOMOUS_RESEARCH_AND_PROMOTION_ROADMAP.md:40-44,951-956`.

**Required review.** Agent governance, application interface, architecture
documentation, normative/platform contract, repository governance, and
research-governance owners.

### DRR-02 — Canonical Ownership And Module-Contract Discovery

**Recommended resolution.** Extend the existing architecture frontmatter/index
mechanism with canonical semantic-owner and required-reviewer metadata. Keep
role-to-person/team resolution in repository governance, not this campaign
directory. A source-module contract qualifies only when it declares component
scope and owner and is linked from its owning component document; it remains
subordinate to platform contracts. Keep the typed `OverlaySpec` registry and
legacy plotting-handler registry distinct and assign them respectively to
indicator-runtime/projection ownership and legacy frontend/BotLens rendering
ownership. `CODEOWNERS` may enforce review routing but must not become
product-semantic authority.

**Alternatives and consequences.** A central ownership manifest is workable but
risks creating another authority layer. `CODEOWNERS` alone cannot adequately
express semantic or component ownership. Until one model is adopted, owner
slugs remain provisional, source-module contracts remain nonactivating, and the
two registries must not be consolidated.

**Affected.** Candidates `QT-GC-002`, `010`, `015`, `027`–`032`, `034`,
`074`, `075`; 12 guarantees listed in
[Exact Wide-Scope Guarantee Impact](#exact-wide-scope-guarantee-impact);
remediations `QT-REM-100`, `101`, `113`–`118`, `120`, `314`, `315`.

**Exact authority and supporting evidence.** Authority/decision:
`docs/contracts/README.md:21-24`. Supporting governance, audit, and
implementation evidence: `AGENTS.md:216-239`;
`docs/plans/documentation-reconciliation/authority-matrix.md:77-102`;
`src/indicators/market_profile/docs/timing_contract.md:3-18`;
`docs/architecture/indicator-runtime/CANDLE_STATS_SIGNAL_CONTRACT.md:1-40`;
`src/overlays/registry.py:12-42,59-81,115-135`; and
`src/core/overlay_registry.py:2-21`.

**Required review.** Architecture documentation, component, repository
governance, platform contract, indicator-runtime, BotLens projection, and
frontend owners.

### DRR-03 — Closed-Denominator And Proof-Topology Program

**Recommended resolution.** Treat this as execution/proof work, not product
semantics. Adopt one protocol:

1. exhaustive claims use a versioned inventory of admitted producers,
   consumers, routes, registries, extension hooks, exclusions, and source-tree
   identity;
2. dynamic registries attest built-ins plus their extension-admission rule;
3. negative claims such as “no external submission path” use repository-wide
   static denominators;
4. every denominator maps to exact CI jobs, environment profiles, selectors,
   and collected cases;
5. absent declared roots fail validation instead of silently skipping; and
6. Node, database, and manual environments report honest typed states—never an
   inferred PASS or FAIL.

Domain owners retain signoff for their own manifests. This is not an omnibus
reviewer substitution.

**Alternatives and consequences.** Representative-only tests or static-only
scans leave the associated claims partial. Permanently freezing open registries
would contradict intentional extension. Narrowing every claim is safe but would
discard intended platform commitments.

**Affected.** 25 candidates: `QT-GC-002`, `015`, `019`–`021`, `027`–`035`,
`050`–`057`, `073`–`075`; 25 guarantees listed in
[Exact Wide-Scope Guarantee Impact](#exact-wide-scope-guarantee-impact);
remediations `QT-REM-100`, `101`, `113`–`121`, `214`–`220`, `314`,
`315`, `403`–`405`.

**Exact authority and supporting evidence.**
`docs/contracts/platform/01_runtime_contract.md:5-15,75-112,163-229`;
`docs/contracts/platform/03_engineering_contract.md:18-30`;
`docs/architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md:46-75,92-102`;
`docs/architecture/decisions/0005-keep-strategy-decisions-separate-from-execution.md:39-53`;
`docs/architecture/decisions/0018-use-output-filters-as-strategy-variant-contract.md:68-90,109-123`;
`docs/architecture/decisions/0019-use-file-backed-sequential-experiment-plans.md:43-81`;
`docs/architecture/decisions/0052-use-typed-fact-collectors-and-explicit-instrument-roles.md:60-79,104-128`;
`docs/architecture/decisions/0060-use-capability-native-research-and-collection-contracts.md:41-68`;
`docs/architecture/decisions/0049-keep-live-order-submission-closed.md:39-64,81-93`;
`docs/architecture/decisions/0001-use-boundary-first-architecture-docs.md:34-54`;
`docs/architecture/ARCHITECTURE_DOCS_MODEL.md:107-151`.

**Required review.** The original routes retain their exact domain reviewers.
Testing/proof owners approve the common protocol; component and subsystem
owners approve each denominator.

### DRR-04 — Relational-Schema Authority Stack

**Recommended owner choice.** Adopt the current layered read order: the platform
engineering contract owns schema behavior; ORM metadata and code-owned schema
registries define the clean current model;
`Database._bootstrap_schema_contract` is the startup enforcement boundary;
manual SQL owns explicit historical cutovers and operations; generated seed SQL
is derivative; Docker bootstrap owns extensions and environment initialization.
This is a stack, not a false “ORM or migrations” choice. The implementation
describes the frozen enforcement stack; it remains evidence until these owners
ratify the ownership/read order.

**Alternatives and consequences.** A single migration manifest could be adopted
later, but would require a migration program. Calling ORM declarations or SQL
files individually authoritative would misdescribe current enforcement.

**Affected.** Candidates `QT-GC-006`, `007`; guarantees
`QT-GUAR-BACKTEST-FROZEN-BINDING` and
`QT-GUAR-DATASET-REALITY-CONSUMER-ADMISSION`. Those candidates have no
remediation record. The remediation appendix separately notes that
`QT-REM-002` and `006` also depend on this schema model.

**Exact authority and supporting evidence.** Authority:
`docs/contracts/platform/03_engineering_contract.md:32-36`. Supporting
nonactivating governance, implementation, and audit evidence:
`AGENTS.md:152-156`; `portal/backend/db/session.py:275-305,324-369`;
`docs/plans/documentation-reconciliation/phase-1-findings.md:213-225`.

**Required review.** Data-authority, database, and persistence owners.

### DRR-05 — Dataset/Check Claim-Authority Ceiling

**Recommended resolution.** Keep `QT-GC-006`, `007`, and `010` as candidates.
ADRs 0051 and 0062 clearly describe intended behavior, but an ADR alone does not
create an activatable platform guarantee. A later owner may place a narrowly
worded promise in an existing platform or reviewed module contract after its
denominator exists.

**Alternatives and consequences.** Platform-level adoption or reviewed
data/research module-contract adoption are both available later; both are
normative changes. Promoting the current ADR wording now would bypass the
authority and activation model.

**Affected.** Candidates `QT-GC-006`, `007`, `010` and guarantees
`QT-GUAR-BACKTEST-FROZEN-BINDING`,
`QT-GUAR-DATASET-REALITY-CONSUMER-ADMISSION`, and
`QT-GUAR-CHECK-AUTHORITY-CEILING`. None has a remediation record.

**Exact authority and supporting evidence.** `docs/contracts/README.md:3-24`;
`docs/plans/documentation-reconciliation/authority-matrix.md:23-38`;
`docs/architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md:43-77`;
`docs/architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md:47-68,81-95`;
`docs/architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md:61-87,145-162`.

**Required review.** Data authority, research authority, platform contract, and
testing owners.

### DRR-06 — Canonical Market Identity And Compatibility Aliases

**Recommended resolution.** Canonical linked instrument and source identities
control runtime routing. Strategy `datasource` and `exchange` values remain
input defaults and lookup hints only. Strategy writes reject `provider_id` and
`venue_id`. An alias may assist lookup but cannot override the linked canonical
instrument.

**Alternatives and consequences.** Additional compatibility aliases may be
retained only at explicit adapter boundaries. Allowing aliases to become runtime
authority would contradict the platform contract and make provenance ambiguous.

**Affected.** `QT-GC-016`,
`QT-GUAR-CANONICAL-MARKET-IDENTITY-ROUTING`, and `QT-REM-400`.

**Exact authority and supporting evidence.**
`docs/contracts/platform/01_runtime_contract.md:19-63`.

**Required review.** Data, decision-layer, and runtime owners.

### DRR-07 — Check-Preview/Observation Supersession

**Recommended resolution.** Explicitly supersede ADR 0034’s automatic
Observation-creation clauses for new records with ADR 0062:

- preview is ephemeral and never Research-Observation eligible;
- only completed, frozen, replayable Check evidence may support a durable
  Research Observation;
- legacy Check/Observation records remain readable and are not upgraded; and
- ordinary “market observation” and the durable Research Observation remain
  distinct concepts.

**Alternatives and consequences.** Owners could allow preview-created
exploratory Observations or introduce two Observation classes. The first weakens
evidence admission; the second expands the domain model. Until approval,
`QT-REM-004` stays decision-gated and `QT-TERM-006`/`012` stay blocked.

**Affected.** Candidates `QT-GC-008`, `009`, `010`; guarantees
`QT-GUAR-CHECK-AUTHORITY-CEILING`,
`QT-GUAR-CHECK-OBSERVATION-ADMISSION`, and
`QT-GUAR-CHECK-PREVIEW-EVIDENCE-SEPARATION`; remediation `QT-REM-004`.

**Exact authority and supporting evidence.** Decision evidence:
`docs/architecture/decisions/0034-use-research-checks-as-analytical-memory-evidence.md:46-66`;
`docs/architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md:45-71,86-95`;
supporting explanatory/implementation evidence:
`docs/architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md:61-68,164-181,211-224`;
`portal/backend/service/research/service.py:1310-1342`;
`tests/test_portal/test_research_evidence_service.py:258-315,356-373`.

**Required review.** Normative-contract, research-memory, and
research-orchestration owners.

### DRR-08 — Market-Structure Lifecycle And GC026 Trace Correction

**Recommended resolution.** Retain ADR 0053’s four-layer model and ADR 0064’s
code-owned collector boundary. Later, align explanatory lifecycle wording so
implemented/current, historical, and genuinely future work are distinct.
Record a forward erratum for `QT-GC-026`: ADR 0053 `:128-132` is the intended
authority and the data-plane matrix at `:1044` is the intended required-proof
and acceptance-definition locator, not a proof result. Preserve the original
Phase 1 artifact unchanged.

**Alternatives and consequences.** Retaining contradictory “later campaign”
wording obscures current behavior; silently editing frozen Phase 1 destroys
audit lineage. Neither is recommended.

**Affected.** Candidates `QT-GC-022`–`026`; guarantees
`QT-GUAR-DURABLE-VERIFIED-RAW-ARCHIVE`,
`QT-GUAR-PIN-SAFE-MARKET-DATA-LIFECYCLE`,
`QT-GUAR-INTERVAL-VALID-ORDER-BOOK-TRUTH`,
`QT-GUAR-CODE-OWNED-AUDITED-COLLECTOR-CONTROL`, and
`QT-GUAR-PROVEN-ZERO-TRADE-COVERAGE`; remediations `QT-REM-406`–`410`.

**Exact authority and supporting evidence.**
`docs/architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md:72-150,235-248`;
`docs/architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md:46-63,78-86,119-140`;
`docs/architecture/data/MARKET_STRUCTURE_DATA_PLANE.md:37-48,1035-1053,1322-1372`.

**Required review.** Data authority, data, market-structure, architecture
documentation, and documentation-assurance owners.

### DRR-09 — Current Execution-Mode And Signal-Bar Meaning

**Recommended resolution.** Ratify the current reading:

- paper simulated and observe-only behavior is implemented; live is a reserved
  composition seam and external submission remains closed;
- immediate market entry may fill at signal close;
- an accepted resting limit-maker submission cannot fill from the already-known
  signal-bar range and begins range-fill eligibility on later bars;
- a new position cannot use earlier signal-bar high/low for stop/target exit;
  and
- FAST and FULL share causal/known-at discipline but use different resolution
  and may produce different outcomes.

**Alternatives and consequences.** Claims of identical FAST/FULL outcomes,
forbidding immediate market entry, or describing live submission as implemented
contradict accepted authority.

**Affected.** Candidates `QT-GC-036`, `040`, `042`; guarantees
`QT-GUAR-MODE-AWARE-RUNTIME-COMPOSITION`,
`QT-GUAR-POST-ONLY-SIGNAL-BAR-CAUSALITY`, and
`QT-GUAR-EXECUTION-MODE-PLAYBACK-SEPARATION`; remediations `QT-REM-200`,
`204`, `206`.

**Exact authority and supporting evidence.**
`docs/architecture/decisions/0012-use-runtime-composition-root-for-mode-aware-wiring.md:32-49`;
`docs/architecture/execution-runtime/RUNTIME_COMPOSITION_ROOT.md:31-37,77-81`;
`docs/architecture/execution-runtime/PAPER_ENGINE_V1_DESIGN.md:32-53`;
`docs/architecture/decisions/0049-keep-live-order-submission-closed.md:33-64`;
`docs/contracts/platform/02_execution_playback_contract.md:13-26,55-90`;
`docs/architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md:81-88`;
`docs/architecture/decisions/0006-keep-execution-semantics-independent-from-playback.md:36-53`.

**Required review.** Architecture, execution-runtime, playback, and platform
contract owners.

### DRR-10 — Execution Profile Versus Resolved-Context Authority

**Recommended resolution.** Choose the newer split explicitly: canonical
instrument owns source identity; `SeriesExecutionProfile` is a compatibility
compiler/input authority; `ResolvedExecutionContext` is immutable run-scoped
authority for instrument, venue, fees, and execution model; later code consumes
the resolved context and compatibility adapters remain explicit.

**Alternatives and consequences.** Owners may retain the profile as final
authority and demote the context. Declaring dual authority is not recommended
because disagreement would have no deterministic winner. The runtime contract
still calls `SeriesExecutionProfile` “the runtime authority,” so the newer ADR
model cannot silently replace it.

**Affected.** Candidates `QT-GC-037`, `038`; guarantees
`QT-GUAR-STRATEGY-INDEPENDENT-EXECUTION-ECONOMICS` and
`QT-GUAR-PINNED-EXECUTION-CONTEXTS`; remediations `QT-REM-201`, `202`.

**Exact authority and supporting evidence.** Authority/decision:
`docs/contracts/platform/01_runtime_contract.md:43-48`;
`docs/architecture/decisions/0027-use-execution-profiles-as-runtime-instrument-authority.md:45-72`;
`docs/architecture/decisions/0056-pin-venue-neutral-execution-contexts-per-run.md:39-68`;
supporting explanatory evidence:
`docs/architecture/execution-runtime/PHASE_2A_VENUE_NEUTRAL_EXECUTION_CONTEXT.md:35-39,129-166`.

**Required review.** Execution-runtime and instruments owners, plus the exact
reviewers retained by the two remediation records.

### DRR-11 — Execution-Quality Ladder And Ceiling Owner

**Recommended resolution.** Ratify the existing X0–X5 ladder as QT’s single
execution-model quality vocabulary: the model artifact sets only a ceiling;
reports assign the attained class from the weakest required evidence;
missing/contradictory context forces X0; X3/X4 require causal certified
book/spread/depth evidence; X5 requires actually exercised bounded
passive-queue/latency evidence; and no class implies venue-realized fill
probability or live realism.

**Alternatives and consequences.** Owners may retain X0–X5 as explanatory only
or replace it with another reviewed ladder. Leaving fee, slippage, rounding,
liquidity, queue, and reporting labels independent invites semantic drift and
prevents a defensible minimum-quality claim.

**Affected.** `QT-GC-043`,
`QT-GUAR-RUNTIME-EXECUTION-OWNERSHIP-QUALITY-CEILING`, and `QT-REM-207`.

**Exact authority and supporting evidence.** Decision evidence:
`docs/architecture/decisions/0040-use-runtime-exit-plans-and-liquidity-roles.md:57-93`;
`docs/architecture/decisions/0056-pin-venue-neutral-execution-contexts-per-run.md:39-65`;
supporting explanatory evidence:
`docs/architecture/execution-runtime/PHASE_1_ECONOMIC_EXECUTION_CONTRACT.md:100-149`;
`docs/architecture/execution-runtime/PHASE_2A_VENUE_NEUTRAL_EXECUTION_CONTEXT.md:129-166`;
`docs/architecture/execution-runtime/PHASE_3A_REPLAY_CERTIFIED_BOOK_EXECUTION.md:99-119,172-183`;
`docs/architecture/execution-runtime/PHASE_3B_PASSIVE_QUEUE_BOUNDS_AND_LATENCY.md:32-40,130-175`.

**Required review.** Accounting, execution-model, and execution-runtime owners.

### DRR-12 — Documentation/Index/Lifecycle/CI Reconciliation

**Recommended owner choice.** Approve this bounded reconciliation bundle. Four
parts are factual alignments; the missing-link and orphan-asset treatments are
owner dispositions:

- mark ADR 0048 Accepted in the manual index and include accepted ADR 0064;
- replace the two absent BTC V1/V2 links with an explicit
  historical-unavailable note and link the retained V3 postmortem;
- include active CLI, operator-console, and research-memory boundaries in
  architecture navigation;
- mark completed campaign, validation, and migration records historical while
  retaining them in place;
- keep the source-less platform-flow SVG as `unverified retained` unless
  lineage is recovered; Mermaid renders remain optional derivatives; and
- describe CI as four jobs—`pr-suite`, `frontend`,
  `deployment-contract`, and `clean-database-bootstrap`—with clean bootstrap
  and DB-marked verification as steps in the fourth job.

**Alternatives and consequences.** Owners may provide real replacement
destinations, remove the dead-link references without a historical note, or
recover/recreate asset lineage instead of retaining the SVG as unverified.
Inventing link targets, deleting history without review, or calling CI steps
separate jobs would make documentation less truthful.

**Affected.** `QT-GC-072`, `QT-GUAR-PR-VERIFICATION-TOPOLOGY`, and
`QT-REM-313`. The other five routes are documentation findings without
guarantee/remediation records.

**Exact authority and supporting evidence.** Decision/status evidence:
`docs/architecture/decisions/0048-gate-agent-mutation-and-research-promotion.md:25-29`;
accepted primary metadata in
`docs/architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md:1-12`;
supporting documentation and implementation evidence:
`docs/architecture/decisions/README.md:68-91`;
`docs/index.md:51-52`;
`docs/research-campaigns/BTC_PERP_MARKET_STRUCTURE_CAMPAIGN_V3_DOSSIER.md:17-40`;
`docs/architecture/ARCHITECTURE_DOCS_MODEL.md:107-139`; active frontmatter in
`docs/architecture/cli/CLI_SETUP_BOUNDARY.md:1-19`,
`docs/architecture/frontend/OPERATOR_CONSOLE_V2.md:1-30`, and
`docs/architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md:1-27`;
`.github/workflows/test.yaml:9-165`;
`docs/engineering/testing/ci-test-topology.md:13-39,116-122`.

**Required review.** The exact index, architecture, CI/testing, component,
historical-record, campaign, and generated-asset owners retained by the six
routes.

### DRR-13 — Supported Frontend Boundary And Test Topology

**Recommended owner choice.** Adopt the supported frontend boundary: platform/runtime/service owners own
truth while the frontend owns presentation and read-model composition; V2
primary rooms remain GET/read-only; collector lifecycle belongs to
`CollectorOperationsService`. Enumerate actual V2 roots and fail or correct
absent declared roots rather than silently skipping them. Replace the generic
Vite README with a QT-specific entry page. Wire the two tracked JSX suites into
a pinned Vitest/jsdom profile and make `frontend-check` execute both Node and
Vitest suites.

**Alternatives and consequences.** Owners may formally retain the JSX files as
historical unsupported tests. Silent non-discovery is not an acceptable test
topology because it makes tracked assertions look supported when they are not.

**Affected.** Candidates `QT-GC-050`–`057`; eight guarantees listed in
[Exact Wide-Scope Guarantee Impact](#exact-wide-scope-guarantee-impact);
remediations `QT-REM-214`–`220`. `QT-GC-056` remains a candidate and
therefore has no remediation.

**Exact authority and supporting evidence.** Decision evidence:
`docs/architecture/decisions/0035-use-complete-output-catalogs-and-split-strategy-read-contracts.md:68-102`;
`docs/architecture/decisions/0064-use-one-code-owned-collector-operations-contract.md:119-135`;
supporting governance, explanatory, and implementation evidence:
`AGENTS.md:15-21,191-205`;
`docs/architecture/frontend/OPERATOR_CONSOLE_V2.md:45-78,95-108,187-205`;
`portal/frontend/tests/v2ReadOnlySurface.test.js:10-51`;
`portal/frontend/package.json:6-12,29-42`;
`portal/frontend/src/components/__tests__/DeleteIndicatorModal.test.jsx:1-12`;
`portal/frontend/src/components/__tests__/IndicatorCard.test.jsx:1-12`;
`Makefile:480-486`; `portal/frontend/README.md:1-12`.

**Required review.** Frontend, operator-console, collector-operations,
architecture-documentation, and testing owners.

### DRR-14 — Loki/Grafana Operating Model

**Recommended owner choice.** Ratify two parts separately. The amendment
derives Alloy as the native-Linux server shipper and preserves the
one-out-of-process-shipper/no-in-process-hot-path architecture. Owners must
decide whether local Promtail remains a supported development topology or is
merely retained historical tooling. Operations/security owners must also
choose a supported Grafana workflow: either adopt the tracked backup script and
an actual stack reload/restart restoration procedure, add reviewed wrappers, or
remove the nonexistent `grafana-backup`/`grafana-restore` instructions.

**Alternatives and consequences.** Owners may retire local Promtail, add
reviewed Make wrappers, or adopt another shipper through a new decision.
Describing Promtail as the server shipper, assuming the backup script is already
an approved operational contract, or documenting commands that do not exist
would overstate the frozen evidence.

**Affected.** `QT-GC-066`,
`QT-GUAR-SINGLE-LOKI-INGRESS-PER-TOPOLOGY`, and `QT-REM-308`.

**Exact authority and supporting evidence.** Decision evidence:
`docs/architecture/decisions/0033-use-promtail-as-runtime-loki-ingress.md:33-37,61-85`;
supporting explanatory, implementation, and operational evidence:
`docs/engineering/observability.md:29-38`;
`docker/docker-compose.yml:221-249`;
`docker/docker-compose.server.yml:307-343`;
`docker/grafana/provisioning/dashboards/README.md:11-80`;
`scripts/backup-grafana-dashboards.sh:1-49`; the frozen Makefile contains no
named backup/restore targets.

**Required review.** Observability, deployment, operations, and Makefile owners.

### DRR-15 — Glossary Ratification Policy

**Recommended resolution.** Do not blanket-adopt. After the substantive
decisions above, approve each term and alias through its domain authority owner:
definitions that merely restate clear authority may be adopted as qualified
terms; alias actions remain explicit; 19 deferred terms remain deferred until
source/ownership issues close; `QT-TERM-006` and `QT-TERM-012` remain blocked
until DRR-07 is approved; historical spellings remain in historical records.

**Alternatives and consequences.** Blanket adoption would smuggle unresolved
semantics into authority. Blanket rejection would discard useful qualified
distinctions. A single procedure is appropriate, but 55 term dispositions
cannot safely become one semantic vote.

**Affected.** Candidates `QT-GC-002`, `006`, `007`, `010`, `015`,
`027`–`035`; 14 guarantees listed in
[Exact Wide-Scope Guarantee Impact](#exact-wide-scope-guarantee-impact);
remediations `QT-REM-100`, `101`, `113`–`121`.

**Exact authority and supporting evidence.**
`docs/plans/documentation-reconciliation/proposed-glossary.md:170-193,305-326,1231-1237`;
`docs/plans/documentation-reconciliation/terminology-inventory.md:17-24,103-108`;
and the exact authority locators attached to each glossary entry.

**Required review.** Domain-terminology owner, platform-contract reviewer, and
the authority owner for each individual term.

## Decisions That Must Stay Separate

- DRR-07, DRR-10, and DRR-11 are unrelated semantic choices. Check admission,
  execution authority, and execution-quality meaning must not be bundled into
  one architecture approval.
- DRR-15 establishes one ratification procedure, but does not turn 55 term
  dispositions into a blanket semantic decision.
- DRR-03 provides one proof protocol only at the cross-cutting level. Each
  domain inventory still requires the exact reviewers named by its original
  route.

## Exact Wide-Scope Guarantee Impact

The four wide cards use exact lists here so the decision cards remain
readable.

### DRR-02 — 12 Guarantees

- `QT-GUAR-AGENT-WORKFLOW-BOUNDARIES`
- `QT-GUAR-CHECK-AUTHORITY-CEILING`
- `QT-GUAR-CONTRACT-DRIVEN-GENERIC-SURFACES`
- `QT-GUAR-DERIVED-OUTPUT-TIMELINE`
- `QT-GUAR-EFFECTIVE-STRATEGY-RESOLUTION-PARITY`
- `QT-GUAR-EXTERNAL-ORDER-SUBMISSION-CLOSED`
- `QT-GUAR-INDICATOR-LIFECYCLE-EVIDENCE-SEPARATION`
- `QT-GUAR-INDICATOR-OUTPUT-CATALOG-AND-STRATEGY-READS`
- `QT-GUAR-INDICATOR-OUTPUT-PRESENCE-AND-READINESS`
- `QT-GUAR-INDICATOR-PUBLICATION-AUTHORITY`
- `QT-GUAR-PROJECTION-DOES-NOT-CHANGE-INDICATOR-TRUTH`
- `QT-GUAR-STRATEGY-DECISION-ARTIFACT-SEPARATION`

### DRR-03 — 25 Guarantees

- `QT-GUAR-AGENT-WORKFLOW-BOUNDARIES`
- `QT-GUAR-ARCHITECTURE-DOC-INDEX-INTEGRITY`
- `QT-GUAR-BOT-RUN-CONTAINER-IDENTITY-SEPARATION`
- `QT-GUAR-BOTLENS-CURSOR-LINEAGE`
- `QT-GUAR-BOTLENS-HOT-STATE-NOT-HISTORY`
- `QT-GUAR-BOTLENS-TYPED-READINESS`
- `QT-GUAR-CONTRACT-DRIVEN-GENERIC-SURFACES`
- `QT-GUAR-DERIVED-OUTPUT-TIMELINE`
- `QT-GUAR-DETERMINISTIC-SEQUENTIAL-EXPERIMENT-PLAN`
- `QT-GUAR-EFFECTIVE-STRATEGY-RESOLUTION-PARITY`
- `QT-GUAR-EXTERNAL-ORDER-SUBMISSION-CLOSED`
- `QT-GUAR-FENCED-IDEMPOTENT-SCHEDULED-COLLECTION`
- `QT-GUAR-INDICATOR-LIFECYCLE-EVIDENCE-SEPARATION`
- `QT-GUAR-INDICATOR-OUTPUT-CATALOG-AND-STRATEGY-READS`
- `QT-GUAR-INDICATOR-OUTPUT-PRESENCE-AND-READINESS`
- `QT-GUAR-INDICATOR-PUBLICATION-AUTHORITY`
- `QT-GUAR-OPERATOR-CONSOLE-NONAUTHORITATIVE-SURFACE`
- `QT-GUAR-OVERLAY-COMPLETENESS-ISOLATION`
- `QT-GUAR-PROJECTION-DOES-NOT-CHANGE-INDICATOR-TRUTH`
- `QT-GUAR-PROJECTOR-ONLY-SELECTED-SYMBOL-READS`
- `QT-GUAR-PROVIDER-CAPABILITY-AUTHORIZATION`
- `QT-GUAR-STRATEGY-DECISION-ARTIFACT-SEPARATION`
- `QT-GUAR-STRATEGY-VARIANT-OUTPUT-FILTER-BOUNDARY`
- `QT-GUAR-TRADE-MARKER-CAUSAL-CANDLE-PROJECTION`
- `QT-GUAR-TYPED-CONSUMER-FACT-REQUIREMENTS`

### DRR-13 — 8 Guarantees

- `QT-GUAR-BOT-RUN-CONTAINER-IDENTITY-SEPARATION`
- `QT-GUAR-BOTLENS-CURSOR-LINEAGE`
- `QT-GUAR-BOTLENS-HOT-STATE-NOT-HISTORY`
- `QT-GUAR-BOTLENS-TYPED-READINESS`
- `QT-GUAR-OPERATOR-CONSOLE-NONAUTHORITATIVE-SURFACE`
- `QT-GUAR-OVERLAY-COMPLETENESS-ISOLATION`
- `QT-GUAR-PROJECTOR-ONLY-SELECTED-SYMBOL-READS`
- `QT-GUAR-TRADE-MARKER-CAUSAL-CANDLE-PROJECTION`

### DRR-15 — 14 Guarantees

- `QT-GUAR-BACKTEST-FROZEN-BINDING`
- `QT-GUAR-CHECK-AUTHORITY-CEILING`
- `QT-GUAR-DATASET-REALITY-CONSUMER-ADMISSION`
- `QT-GUAR-DERIVED-OUTPUT-TIMELINE`
- `QT-GUAR-DETERMINISTIC-SEQUENTIAL-EXPERIMENT-PLAN`
- `QT-GUAR-EFFECTIVE-STRATEGY-RESOLUTION-PARITY`
- `QT-GUAR-EXTERNAL-ORDER-SUBMISSION-CLOSED`
- `QT-GUAR-INDICATOR-LIFECYCLE-EVIDENCE-SEPARATION`
- `QT-GUAR-INDICATOR-OUTPUT-CATALOG-AND-STRATEGY-READS`
- `QT-GUAR-INDICATOR-OUTPUT-PRESENCE-AND-READINESS`
- `QT-GUAR-INDICATOR-PUBLICATION-AUTHORITY`
- `QT-GUAR-PROJECTION-DOES-NOT-CHANGE-INDICATOR-TRUTH`
- `QT-GUAR-STRATEGY-DECISION-ARTIFACT-SEPARATION`
- `QT-GUAR-STRATEGY-VARIANT-OUTPUT-FILTER-BOUNDARY`

## Companion Disposition Packets

- [All 68 proposed remediation dispositions](decision-resolution-remediation-dispositions.md)
  preserve each record and separate later resolution, execution, proof, and
  activation-priority work.
- [All nine proof-environment decisions](decision-resolution-proof-decisions.md)
  reduce to five owner-facing choices while retaining every ceiling and its
  exact reviewers.

## Owner Approval Checklist

An approval should state:

1. which of DRR-01 through DRR-15 are approved, amended, rejected, or rerouted;
2. the canonical owner source and module-contract discovery choice for DRR-02;
3. the selected Research Observation admission rule for DRR-07;
4. the authority choice for DRR-10 and quality-vocabulary choice for DRR-11;
5. whether the five proof choices and 68 proposed dispositions are accepted as
   planning inputs; and
6. whether a separately bounded Phase 3 authorization will follow.

Until that approval arrives, every guarantee remains unactivated, every
glossary entry remains unadopted or blocked/deferred as recorded, every
remediation remains proposed and unexecuted, every proof definition remains a
definition rather than a result, and Phase 3 remains unauthorized.
