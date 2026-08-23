# Phase 1 Findings and Conflicts

This report records findings against frozen baseline
`d46e40bf55caeea12f4ccbde640c71f271eaf9c4`. No finding in this file changes
product semantics or documentation authority. Proposed dispositions are inputs to
Gate 1 and later gated phases.

## Finding Register

| ID | Severity | Status | Summary | Review requirement |
| --- | --- | --- | --- | --- |
| `DOC-AUTH-001` | high | conflicting | An explanatory architecture roadmap declares a normative autonomy matrix | Normative-meaning review |
| `DOC-INDEX-001` | medium | conflicting | Manual ADR index says ADR 0048 is Proposed; all primary/generated metadata says Accepted | Ordinary reconciliation |
| `DOC-INDEX-002` | medium | stale | Manual ADR index omits accepted ADR 0064 | Ordinary reconciliation |
| `DOC-LINK-001` | medium | stale | `docs/index.md` has two links to nonexistent BTC campaign dossiers | Ordinary reconciliation |
| `DOC-MODEL-001` | medium | stale | Architecture entry/model omits or denies active CLI, frontend, and research-memory boundaries | Architecture-owner review |
| `DOC-META-001` | high | blind spot | Repository docs declare no owner/authority/lifecycle/audit/replacement metadata and no CODEOWNERS exists | Ownership-model review |
| `DOC-PATH-001` | medium | stale | One of 437 architecture `code_paths` targets does not exist | Frontend-owner review |
| `DOC-MARKET-STRUCTURE-001` | high | conflicting | One active data-plane document describes the market-structure plane as both implemented and later/proposed | Data-owner review |
| `DOC-RUNTIME-COMPOSITION-001` | high | conflicting | Runtime composition prose still treats paper/live as placeholders while separate paper behavior is implemented in a draft-labeled design | Runtime-owner review |
| `DOC-LOG-INGRESS-001` | medium | conflicting | ADR 0033's decision body says Promtail-only while its amendment and server topology use Alloy | Observability/deployment-owner review |
| `DOC-MUTATION-SCOPE-001` | high | conflicting | ADR 0048's universal mutation-gate wording conflicts with its own exclusions and nonuniform enforcement | Normative-scope review |
| `DOC-CI-TOPOLOGY-001` | high | stale | CI topology prose denies the separate database runner and understates the current PR jobs | Testing-owner review |
| `DOC-LIFECYCLE-001` | medium | conflicting | Completed campaign/validation evidence is labeled active or lacks lifecycle metadata | Ordinary reconciliation |
| `DOC-STALE-001` | medium | stale | Frontend README is untouched React/Vite starter material | Deletion/replacement review |
| `DOC-OPS-001` | high | stale | Grafana README instructs operators to use two nonexistent Make targets | Operations-owner review |
| `DOC-CONTRACT-001` | high | unclear | Indicator contract placement/discovery is inconsistent across Market Profile and Candle Stats | Authority-model review |
| `DOC-VALIDATION-001` | medium | partial | Existing docs validation does not validate owners, field values, paths, links, duplicates, or clean regeneration | Phase 3 validation design |
| `DOC-LINEAGE-001` | low | unverified | One docs SVG has no documented source; five Mermaid sources have no rendered SVG | Generated-lineage review |
| `ARCH-COVERAGE-001` | medium | blind spot | Broad/shared component mappings obscure implementation ownership and direct test traceability | Component-owner review |
| `SCHEMA-AUTH-001` | high | fragmented | Schema authority is split across ORM metadata, startup assertions, manual SQL, generated seed SQL, and Docker bootstrap | Persistence-owner review |
| `REGISTRY-OWNERSHIP-001` | medium | unclear | Typed and legacy overlay registries are both active but architecture-unowned and semantically distinct | Indicator/frontend owner review |
| `REGISTRY-DYNAMIC-001` | informational | explicit dynamic scope | Seven registries permit runtime/import-time additions and cannot be frozen as closed static sets | Accept dynamic-attestation model |
| `CI-TRACE-001` | medium | partial | Seventy backend test files have no profile-specific marker; 30 of 43 active components have no direct test path in their index row | Testing/component-owner review |
| `TEST-GAP-001` | high | stale | Two tracked JSX/Vitest suites containing 44 tests are not wired into `npm test` | Frontend-owner review |
| `TEST-SCOPE-001` | high | stale | The V2 read-only surface test silently skips missing scan directories, including a documented path that does not exist | Frontend-owner review |
| `TEST-ENV-001` | informational | unavailable | Frontend Node proof could not complete in the isolated worktree because dependencies are not installed | Attestation only; rerun after environment setup |

The 26 terminology conflicts `QT-CONFLICT-001` through `QT-CONFLICT-026` are
defined in `terminology-inventory.md` and form part of this register.

## Evidence

### `DOC-AUTH-001` — Parallel normative authority

The global hierarchy makes platform contracts authoritative:

- `AGENTS.md:8-9,35-44`;
- `docs/README.md:15-17,31`;
- `docs/contracts/README.md:3-7`;
- `docs/architecture/README.md:79-81,106`.

`docs/architecture/research-orchestration/AUTONOMOUS_RESEARCH_AND_PROMOTION_ROADMAP.md:40-42,953-956`
instead calls itself a ratified governing roadmap and its autonomy matrix
normative. Later work must either promote the required rule into the existing
normative hierarchy or make the roadmap explanatory. Phase 1 assigns no
precedence.

### `DOC-INDEX-001` and `DOC-INDEX-002` — ADR index drift

- ADR 0048 frontmatter at line 6 and body at lines 25-29 say Accepted.
- The generated component index says Accepted at line 101.
- `docs/architecture/decisions/README.md:76` says Proposed.
- That README ends its record list with ADR 0063 at line 91.
- Accepted ADR 0064 exists and is present in the generated component index.

### `DOC-LINK-001` — Broken primary navigation

`docs/index.md:51-52` links two nonexistent BTC campaign V1/V2 documents. A
repository-wide local Markdown-target scan found no other missing local targets.

### `DOC-MODEL-001` — Stale architecture model

`docs/architecture/ARCHITECTURE_DOCS_MODEL.md:135` says frontend operator
surfaces lack a dedicated boundary, while
`docs/architecture/frontend/OPERATOR_CONSOLE_V2.md:2-6` is active. The boundary
map in `docs/architecture/README.md:55-75` also omits these active components:

- `docs/architecture/cli/CLI_SETUP_BOUNDARY.md:2-6`;
- `docs/architecture/frontend/OPERATOR_CONSOLE_V2.md:2-6`;
- `docs/architecture/research-memory/RESEARCH_MEMORY_BOUNDARY.md:2-6`.

### `DOC-META-001` and `ARCH-COVERAGE-001` — Ownership blind spots

Across all 179 frozen Markdown files there are zero top-level `owner`, `owners`,
`authority`, `lifecycle`, `audit_status`, `superseded_by`, or `replaced_by`
fields, and no tracked `CODEOWNERS`. Only two documents declare `last_verified`.

The architecture corpus declares 1,216 mappings to 437 unique `code_paths`; 261
paths are shared by multiple component documents and `cli/main.py` is referenced
by 37. Of 264 tracked files below `tests/`, 193 have no direct architecture-index
mapping. The coverage ledger therefore records inferred `owner_basis` on every
row rather than presenting inferred ownership as repository authority.

### `DOC-PATH-001` — Dead architecture path

`docs/architecture/frontend/OPERATOR_CONSOLE_V2.md:22` names
`portal/frontend/src/features/market-structure`, which is absent at the frozen
commit. It is the only nonexistent literal target among the 1,216 declarations.

### `DOC-MARKET-STRUCTURE-001` — Implemented and proposed in one active boundary

`docs/architecture/data/MARKET_STRUCTURE_DATA_PLANE.md:37-48` describes the
market-structure plane as implemented, while lines 1322-1372 retain later-
campaign and proposed-path language for capabilities that now have code and
proof. The current implementation evidence supports the candidate invariants;
the conflict is the document's lifecycle and phase wording.

### `DOC-RUNTIME-COMPOSITION-001` — Paper/live lifecycle drift

`docs/architecture/execution-runtime/RUNTIME_COMPOSITION_ROOT.md:33-35,79-80`
and `docs/architecture/ARCHITECTURE_DOCS_MODEL.md:136` describe paper/live as
backtest-like placeholders. In contrast,
`docs/architecture/execution-runtime/PAPER_ENGINE_V1_DESIGN.md:38-43,92-112,161-201,400-429`
and current adapters describe and implement separate paper behavior, while that
design document remains `status: draft`. A subsystem review must choose accurate
current lifecycle language without promoting explanatory prose into a contract.

### `DOC-LOG-INGRESS-001` — Promtail/Alloy decision-body conflict

ADR 0033's amendment at lines 33-37 selects Alloy for server deployment, while
its decision body at lines 61-80 still says the topology is Promtail-only.
Current compose files consistently use one out-of-process shipper per
environment: Promtail in development and Alloy on the server.

### `DOC-MUTATION-SCOPE-001` — Mutation-gate scope overstatement

ADR 0048 acknowledges nonuniform guards/audit at lines 33-37 and excludes
non-research workflows at lines 66-69, but lines 41-59 say every mutation uses
the gate. Until the normative scope is reviewed, the corresponding guarantee
candidate remains limited to enumerated CLI, MCP, and offline-research paths.

### `DOC-CI-TOPOLOGY-001` — CI topology drift

`docs/engineering/testing/ci-test-topology.md:15-39,118-122` says the PR suite is
the whole gate and that no database runner exists. `.github/workflows/test.yaml`
instead defines separate frontend, deployment-contract, clean-bootstrap, and
PostgreSQL-marked jobs in addition to the non-DB suite.

### `DOC-LIFECYCLE-001` — Completed evidence labeled current

Content establishes these as historical even where frontmatter says active or is
missing:

- `docs/plans/backtest-dataset-boundary.md:6,71-80`;
- `docs/plans/platform-baseline-cleanup.md:12-30`;
- `docs/engineering/collector-operations-discovery.md:3,11-18`;
- `docs/engineering/collector-operations-validation.md:3,11-16`;
- `docs/engineering/frontend-v2-operator-validation.md`;
- the three Canonical Fact migration records and three retained market-research
  dossiers listed in `authority-matrix.md`.

The ledger preserves raw status and records lifecycle separately.

### `DOC-STALE-001` and `DOC-OPS-001` — Stale source-local READMEs

- `portal/frontend/README.md:1-12` is generic starter text despite the active V2
  frontend. It is a candidate replacement/deletion, requiring explicit review.
- Grafana dashboard instructions name `make grafana-backup` at line 16 and
  `make grafana-restore` at line 70; neither target exists in the Makefile.

### `DOC-CONTRACT-001` — Inconsistent component-contract discovery

`docs/contracts/README.md:23-24` delegates Indicator-specific semantics to
module docs. Market Profile follows that placement in
`src/indicators/market_profile/docs/timing_contract.md`, but no other Markdown
file links to it. Candle Stats instead places a signal “contract” under
explanatory architecture. This needs an authority/discovery decision, not an
automatic semantic move.

### `DOC-VALIDATION-001` — Narrow validation

`scripts/docs/build_architecture_index.py:55-60` silently skips files without
frontmatter. `tests/contract/test_architecture_docs_index.py:16-39` checks
required key substrings but not their values, ownership, path existence,
duplicate mappings, internal links, or exact index equivalence. `make
validate-docs` regenerates the checked-in index before testing and does not itself
assert a clean worktree. Positive evidence: the frozen index independently and
exactly matches all 114 eligible component files.

### `DOC-LINEAGE-001` — Generated/source relationships

There are 22 Mermaid sources and 17 architecture SVG counterparts. Five sources
have no render, which is allowed when tooling is unavailable; no architecture
SVG lacks its same-name Mermaid source. `docs/assets/quant-trad-platform-flow.svg`
is unreferenced and has no documented generator/source, so it remains an
unverified orphan rather than being presumed generated.

### `TEST-GAP-001`, `TEST-SCOPE-001`, and `TEST-ENV-001` — Frontend proof topology

The frozen tree contains 44 frontend test files:

- 42 `.test.js` files with 239 statically declared Node tests;
- two `.test.jsx` files with 44 statically declared Vitest tests.

The package command is `node --test`. It discovers the JS files but does not
discover `.jsx`; `vitest` and Testing Library are not configured dependencies.
The two JSX suites are therefore a repository proof-topology gap.

`portal/frontend/tests/v2ReadOnlySurface.test.js:35-42` skips a scan directory
when it is absent. That behavior lets the suite pass without examining the
documented but missing `portal/frontend/src/features/market-structure` tree, so
the missing boundary is recorded rather than treated as proof of a clean V2
surface.

A Phase 1 `npm test` attempt in the isolated worktree reported 179 passed cases
and 10 file-level import failures before 50 additional declared cases could
register. Every failure was caused by absent installed packages such as React or
Vite because the isolated worktree has no `node_modules`. That attempt is
attested `UNAVAILABLE IN THIS ENVIRONMENT`, not `FAIL`. It made no product-file
changes.

### `SCHEMA-AUTH-001` — Fragmented schema ownership

The 74 SQLAlchemy tables carry 111 check constraints, 58 unique constraints, 86
foreign keys, 74 primary keys, and 109 indexes. Runtime schema authority is not a
single migration manifest: `portal/backend/db/session.py:Database` creates
selected schemas/tables/indexes and functions/triggers, asserts migration-owned
columns and cutovers, compares the stored Fact registry with code, and rejects
retired tables. Thirty-three SQL files separately contain manual migrations,
operations, a generated seed, a forensic query, and Docker extension bootstrap.

This is not automatically wrong, but the guarantee registry must reference the
correct enforcement owner rather than treating ORM declarations or migrations
alone as authoritative.

### `REGISTRY-OWNERSHIP-001` and `REGISTRY-DYNAMIC-001` — Registry semantics

`src/overlays/registry.py` owns 16 typed runtime overlay contracts, while
`src/core/overlay_registry.py` owns four legacy plotting handlers. Both are
active and neither has direct architecture ownership. They must not be collapsed
merely because both are called registries.

The exact frozen built-in catalogs are recorded in
`implementation-surface-inventory.json`. Seven catalogs remain intentionally
open to runtime or import-time extension: normalized Fact payload schemas,
provider instances, BotLens projector contexts, collector adapters, provider
metadata, typed overlays, and legacy overlay handlers. A later proof must attest
both built-in contents and allowed dynamic extension behavior.

### `CI-TRACE-001` — Proof-profile trace gaps

Pytest collects 2,102 cases from 249 Python suites; the frontend tree declares
283 cases across 44 files. CI exposes four jobs, 15 named suites, and eight
profile markers. Seventy backend test files have no profile-specific marker,
although generic PR/backend selection still includes them. Thirty of 43 active
architecture components have no direct `tests/` path in their own index row.
These are traceability gaps, not claims that the code is untested.

## Explicit Review Queue

Gate 1 must decide the authority/lifecycle model and accept the recorded blind
spots before Phase 2. Later subsystem gates are mandatory before:

- changing the roadmap's claimed normative meaning;
- reconciling market-structure and paper/live lifecycle claims;
- narrowing or expanding the agent mutation-gate contract;
- reconciling the Promtail/Alloy deployment decision;
- changing older Check/Observation semantics;
- choosing component-contract placement;
- replacing or deleting the frontend README;
- changing CI topology or frontend proof scope;
- archiving or deleting any historical campaign/validation record;
- changing a platform contract in response to implementation drift.
