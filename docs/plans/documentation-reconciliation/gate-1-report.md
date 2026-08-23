# Gate 1 — Discovery Review

## Decision Requested

Approve or reject the Phase 1 corpus, existing-authority model, provisional
ownership rules, candidate inventories, and conflict register as the basis for
Phase 2. Approval does not activate any guarantee, adopt proposed terminology,
resolve a conflict, delete a document, or change normative meaning.

## Frozen Subject and Isolation

- audited ref: `origin/develop`;
- audited commit: `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`;
- campaign branch: `codex/docs-guarantee-reconciliation`;
- product code/doc behavior edits during Phase 1: none;
- Phase 1 writes: this audit directory and one deterministic inventory script;
- `develop`: untouched.

## Accounted Denominator

The machine ledger contains **2,407 units**. Every row has an audit status and a
provisional owning boundary with a recorded `owner_basis`; dynamic or ambiguous
surfaces are represented rather than omitted.

| Coverage class | Count | Notes |
| --- | ---: | --- |
| Frozen tracked files | 1,397 | Source Git-tree denominator |
| Documentation/source-local artifacts | 224 | 179 Markdown, 22 Mermaid, 17 source-linked SVGs, 1 source-less SVG, 5 research-evidence JSON |
| Architecture components | 114 | 1,216 mappings to 437 unique paths |
| Implementation paths | 437 | One dead literal target; 261 shared by multiple components |
| CLI | 203 nodes | 178 executable leaves, 148 unique handlers, zero unresolved |
| API | 218 routes | 117 GET, 83 POST, 9 DELETE, 6 PUT, 2 WebSocket, 1 PATCH |
| MCP | 98 members | 59 tools, 10 resources, 29 templates, zero unresolved |
| Database tables | 74 | 111 checks, 58 unique constraints, 86 foreign keys, 109 indexes |
| Schema artifacts/sources | 39 | 33 SQL files, one Python data migration, five bootstrap/model/generator sources |
| Test suites | 293 | 249 Python, 42 Node-profile, 2 unwired Vitest |
| Test support | 15 | Fixtures, helpers, package markers, and `conftest.py` |
| Static test declarations | 2,212 | 1,929 Python plus 283 frontend |
| Collected Python tests | 2,102 | Includes parameter expansion; collection completed read-only |
| Validation surfaces | 12 | Make, pytest, CI/docs scripts, Python and frontend configuration |
| Contract-language passages | 648 | Reproducible full-docstring/comment-block selection over 471 production Python files |

The flat unit counts sum to 2,407 because documentation, component, interface,
schema, proof, and contract-language rows are semantic coverage units, not a
claim that the repository has 2,407 tracked files.

## Status Accounting

| Audit status | Units | Interpretation |
| --- | ---: | --- |
| `verified` | 2 | Narrow facts independently established (current component index and contract-hierarchy descriptor) |
| `conflicting` | 17 | Rows with a recorded active conflict |
| `stale` | 13 | Rows with demonstrated stale links, paths, prose, lifecycle, or proof wiring |
| `unverified` | 2,375 | Explicitly in scope and owned, awaiting Phase 2/3 semantic/proof classification |

`unverified` is intentional Phase 1 status, not silent omission. Zero exact
CLI/API/MCP registrations are unresolved, and zero ledger rows lack a provisional
owner. Fourteen selected contract passages have no architecture-index owner and
therefore use visible fallback owner rules; the architecture-ownership gap stays
open in `ARCH-COVERAGE-001`.

## Existing Authority Model

Phase 1 preserves the repository's hierarchy:

1. Platform contracts own normative platform behavior.
2. Module contracts may own semantics only within their declared component
   scope and must conform to platform contracts.
3. Accepted ADRs retain decision history and rationale but do not replace
   contracts.
4. Architecture documents explain current boundaries and flows.
5. Operator guidance owns supported workflows while the running interface owns
   exact accepted arguments.
6. Code, database constraints, and tests provide enforcement/conformance/proof
   evidence but do not silently rewrite a clear contract.
7. Plans, incidents, validation reports, dossiers, generated assets, and research
   JSON retain working, historical, or derivative roles.

The detailed matrix and every inference rule are in `authority-matrix.md` and
`coverage-ledger.json`.

## Inventories Ready for Review

- `coverage-ledger.json` — full frozen denominator, independent axes, owners,
  exact members, missing paths, and extraction status;
- `authority-matrix.md` — authority/lifecycle/ownership model and the grouped
  classification of all 62 non-architecture Markdown files;
- `terminology-inventory.md` — 55 term candidates, 20 historical/rejected
  aliases, and 26 semantic conflicts/collisions;
- `guarantee-candidates.md` — 75 candidate claims (15 cross-cutting and 60
  component-led), with independent source-authority, source-lifecycle,
  static-conformance, and unrun-proof axes; every candidate is unactivated;
- `implementation-surface-inventory.json` — exact interfaces, schema shape,
  code-owned registries, dynamic registries, proof corpus, and architecture-index
  coverage;
- `phase-1-findings.md` — evidence-backed defects, blind spots, and the explicit
  later-review queue.

## Highest-Risk Findings

1. An explanatory architecture roadmap declares a normative autonomy matrix,
   creating a parallel-authority conflict.
2. Active market-structure, runtime-composition, log-ingress, and mutation-gate
   documents contain internally conflicting lifecycle or scope statements.
3. Active data/persistence/provider documentation mixes the retired family-table
   architecture with the accepted canonical Fact store, while older
   Check/Observation documents conflict with frozen-evidence eligibility.
4. Repository documentation has no declared owner/authority/lifecycle/replacement
   metadata; broad/shared `code_paths` are not ownership.
5. Schema enforcement is split across ORM declarations, startup assertions and
   triggers, manual SQL, generated seed SQL, and Docker bootstrap.
6. Grafana recovery commands do not exist, the frontend README is starter
   residue, CI topology prose is stale, two tracked Vitest suites containing 44
   tests are not wired, and one V2 proof silently skips an absent scan tree.
7. Current validation does not check internal links, path existence, owners,
   exact generated consistency, interface traceability, or claim-to-proof links.

No item above has been silently repaired.

## Proof Attestations So Far

- coverage generator compile/regeneration/check: `PASS` at the campaign tree;
- CLI/API/MCP static extraction: `PASS`, independently cross-checked against
  runtime-safe parser/registry introspection;
- frozen architecture index equivalence: `PASS` by independent audit;
- Python collection: `PASS`, 2,102 tests collected without executing them;
- frontend execution in dependency-less campaign tree: `UNAVAILABLE IN THIS
  ENVIRONMENT` (179 cases passed before 10 missing-package file-load failures);
- two JSX/Vitest suites: `NOT RUN` and structurally unwired from `npm test`.

These are discovery attestations, not guarantee PASS results.

## Gate 1 Choices

Approval should confirm only that:

- the 2,407-unit denominator and auxiliary registry inventory are sufficient to
  proceed;
- the authority model preserves the existing hierarchy;
- provisional ownership rules and explicit blind spots are acceptable inputs;
- the term, claim, and conflict inventories are an adequate Phase 2 starting
  point;
- the listed normative, deletion/archive, and subsystem reviews remain hard
  gates.

If approved, Phase 2A will create the proposed glossary location, registry schema,
generated/validated human view, proof/attestation model, and a 10-15 guarantee
calibration batch. It will then stop at Gate 2 before whole-system candidate
classification.
