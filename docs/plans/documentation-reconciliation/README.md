# QT Documentation Reconciliation Campaign

This directory is the non-normative execution record for the whole-system
documentation, terminology, architecture-conformance, and guarantee-traceability
campaign. It records what was inspected and what remains unresolved; it does not
replace `AGENTS.md`, the platform contracts, accepted ADRs, or component owners.

## Frozen Baseline

- Baseline branch: `origin/develop`
- Baseline commit: `d46e40bf55caeea12f4ccbde640c71f271eaf9c4`
- Campaign branch: `feat/docs-guarantee-reconciliation`
- Current phase: Phase 1 discovery
- Current gate: Gate 1 pending

`develop` remains unchanged for the duration of the campaign. The baseline is a
fixed audit subject rather than a moving integration target.

## Authority During The Audit

The repository's existing precedence remains in force:

1. `AGENTS.md` and `docs/contracts/` state normative platform behavior.
2. Accepted ADRs explain durable decisions and their tradeoffs.
3. Architecture and concept documents explain current boundaries and flows.
4. Implementation, database constraints, tests, and runtime surfaces provide
   conformance and enforcement evidence.
5. Plans, validation records, incidents, and campaign dossiers retain working or
   historical evidence without silently becoming current contracts.

The audit may identify a normative document as apparently stale or contradicted,
but implementation does not replace that authority automatically. Any change to
normative meaning requires an explicit reviewed decision.

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

During Phase 1, existing product code and documentation are read-only. Writes are
limited to audit artifacts in this campaign directory and narrowly scoped audit
tooling needed to produce them. Product-semantic discrepancies become findings,
not silent repairs.

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
