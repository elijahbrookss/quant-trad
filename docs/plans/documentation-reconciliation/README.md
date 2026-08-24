# QT Documentation Reconciliation Campaign

This directory is the non-normative execution record for the whole-system
documentation, terminology, architecture-conformance, and guarantee-traceability
campaign. It records what was inspected and what remains unresolved; it does not
replace `AGENTS.md`, the platform contracts, accepted ADRs, or component owners.

## Frozen Baseline

- Baseline branch: `origin/develop`
- Baseline commit: `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`
- Campaign branch: `feat/docs-guarantee-reconciliation`
- Current phase: Phase 2B whole-system classification complete
- Current gate: subsystem and normative-decision review before Phase 3

`develop` remains unchanged for the duration of the campaign. The baseline is a
fixed audit subject rather than a moving integration target.

## Authority During The Audit

The repository's existing source classes remain in force as the subject of this
audit. `AGENTS.md` governs agent and contributor work within its repository
scope; its precedence relative to platform behavior contracts is not stated
precisely enough for this campaign to invent one. The platform-document and
evidence order is:

1. `docs/contracts/` state normative platform behavior.
2. Accepted ADRs explain durable decisions and their tradeoffs.
3. Architecture and concept documents explain current boundaries and flows.
4. Implementation, database constraints, tests, and runtime surfaces provide
   conformance and enforcement evidence.
5. Plans, validation records, incidents, and campaign dossiers retain working or
   historical evidence without silently becoming current contracts.

The audit may identify a normative document as apparently stale or contradicted,
but implementation does not replace that authority automatically. Any change to
normative meaning requires an explicit reviewed decision. For assurance-index
purposes, `AGENTS.md` remains non-activating until its precedence and ownership
model is reviewed, and source-module contracts remain non-activating until their
discovery and ownership model is reviewed.

## Phase 1 Coverage Policy

The coverage denominator is enumerated from the frozen Git tree. It includes:

- every tracked Markdown document, including the six outside `docs/`;
- every architecture component represented by frontmatter and the generated
  component index;
- every referenced architecture `code_paths` owner;
- 74 database models/tables, clean-bootstrap ownership, and every tracked SQL or
  Python migration/schema artifact;
- all 203 CLI command nodes (178 executable leaves), 218 API routes, and 59 MCP
  tools plus 10 resources and 29 templates;
- CI, Make, pytest, and documentation-validation surfaces;
- 249 Python suites, 44 frontend suites, test-support files, and each applicable
  execution profile, including tests that are tracked but not wired;
- Mermaid sources, generated render counterparts, unverified source-less assets,
  and research evidence assets;
- 648 reproducibly selected contract-language passages across 471 Python files
  under `src/`, `portal/backend/`, `cli/`, and `scripts/`.

The contract-language selection is deliberately mechanical and auditable. It
selects full module, class, function, and async-function docstrings (including
private and nested symbols) when a word-bounded contract token occurs. It groups
consecutive standalone comments only when their indentation and line continuity
match, retains each inline comment as a singleton passage, and emits one ledger
row per matching docstring or comment block. The token expression is versioned
in `scripts/docs/build_reconciliation_inventory.py`; selection is triage, not a
claim that every selected passage is a valid guarantee.

Rendered assets are inventoried for lineage but are not treated as independent
semantic authorities when a tracked source exists. A source-less SVG is instead
an explicitly unverified lineage unit. Vendor, cache, environment, and ignored
runtime artifacts are outside the Git-based denominator.

Each covered unit receives separate values for:

- **authority** — normative, decision, explanatory, operational, historical,
  generated, evidence, or implementation;
- **lifecycle** — active, proposed, draft, accepted, superseded, archived,
  historical, missing, or unclear;
- **audit status** — verified, stale, conflicting, duplicate, unverified, or
  intentionally retained;
- **owning boundary** — the component or subsystem responsible for meaning.

These axes are deliberately independent. For example, an active normative claim
can be contradicted by current implementation, and a historical ADR can be both
verified and intentionally retained.

## Phase Gates

1. **Gate 1 — discovery:** review the complete corpus, authority matrix,
   terminology and claim inventory, conflicts, and blind spots.
2. **Gate 2 — calibration:** review the glossary, registry schema, proof model,
   and initial 10–15 guarantees before whole-system expansion.
3. **Subsystem gates — consolidation:** review each subsystem before deletion,
   archival, or a change to normative meaning.
4. **Final gate:** review residual risks, proof attestations, validation, and the
   clean campaign branch before any integration into `develop`.

Gate 1 was approved by the user on 2026-08-23, and Gate 2 was approved on
2026-08-24. During Phase 2A and Phase 2B, existing product semantics and
normative documentation remain read-only. Writes are limited to the proposed
glossary, assurance-index model, deterministic validation and human view,
classification and remediation records, and review packets. Product-semantic
discrepancies remain findings rather than silent repairs.

## Phase 1 Deliverables

- machine-readable coverage ledger and coverage summary;
- document authority and ownership matrix;
- terminology and deprecated-alias inventory;
- explicit and implicit guarantee-candidate inventory;
- implementation, constraint, test, and surface evidence map;
- exact code-owned and dynamic registry inventory;
- contradiction, ambiguity, duplication, and blind-spot findings;
- Gate 1 report with a precise accounted/unaccounted denominator.

Phase 1 is not complete merely because searches stop finding new material. It is
complete when every enumerated unit has an owner and explicit status, and every
remaining uncertainty is itself represented in the ledger.

## Phase 2A Deliverables

- `proposed-glossary.md` — 21 calibration-scoped term entries; 19 remain
  proposed and `QT-TERM-006`/`QT-TERM-012` remain blocked;
- `phase-2a-calibration.md` — the representative 12-record batch and its exact
  frozen-baseline authority, enforcement, proof, conflict, and environment
  findings;
- `../../assurance/guarantees/registry.json` — machine-readable provisional
  classifications with zero active guarantees;
- `../../assurance/guarantees/proof-catalog.json` — stable proof requirements
  without execution results;
- `../../assurance/guarantees/GUARANTEES.md` — generated human view;
- `../../assurance/guarantees/schemas/` and `attestations/README.md` — executable
  model documentation and commit-bound attestation semantics;
- `gate-2-report.md` — decision packet and explicit stop before Phase 2B.

Phase 2A completion does not adopt the glossary, activate a guarantee, classify
the remaining 63 candidates, settle a conflict, repair product semantics, or
authorize deletion/archive. Gate 2 approval authorized application of the
calibrated model; it did not authorize any of those other actions.

## Phase 2B Deliverables

- `phase-2b-work/batch-a.json` through `batch-d.json` — frozen-baseline
  classification working records for the remaining 63 candidates;
- `proposed-glossary.md` — all 55 Phase 1 term IDs accounted for as 34 proposed,
  2 blocked, 19 deferred, and 0 adopted;
- `../../assurance/guarantees/registry.json` — all 75 candidates classified,
  with 65 `partially_enforced`, 3 `contradicted`, 6 `candidate`, 1
  `implementation_property`, and 0 active guarantees;
- `../../assurance/guarantees/remediations/` — 68 concrete remediation records,
  one for every `partially_enforced` or `contradicted` guarantee;
- `../../assurance/guarantees/proof-catalog.json` — 85 proof definitions without
  execution results;
- `../../assurance/guarantees/GUARANTEES.md` — regenerated human view;
- `phase-2b-review-map.json` and `phase-2b-report.md` — the complete required
  review routing and human decision packet before Phase 3.

Phase 2B completion does not activate a guarantee, adopt terminology, resolve a
normative conflict, repair product semantics, change a contract, execute proof,
or authorize deletion, archival, consolidation, or integration into `develop`.
