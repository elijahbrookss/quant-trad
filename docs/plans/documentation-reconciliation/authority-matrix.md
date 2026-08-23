# Phase 1 Authority and Ownership Matrix

This matrix classifies the frozen audit subject at
`d46e40bf55caeea12f4ccbde640c71f271eaf9c4`. It is an audit artifact, not a
new authority layer. Repository documents keep their existing authority until a
reviewed change is made in a later phase.

## Classification Axes

The coverage ledger records four independent axes:

| Axis | Meaning | Allowed Phase 1 values |
| --- | --- | --- |
| Authority | What kind of claim the artifact is permitted to own | normative governance or contract, module contract, decision record, explanatory architecture, operational guidance, working plan, historical evidence, implementation/proof evidence, generated derivative |
| Lifecycle | Whether the artifact is current and in what decision state | active, draft, proposed, accepted, superseded, historical, missing, unclear |
| Audit status | What the reconciliation has established about conformance | verified, stale, conflicting, duplicate, unverified, intentionally retained |
| Owning boundary | The subsystem responsible for resolving the artifact's claims | the architecture `subsystem` where declared; otherwise an explicit path/content rule recorded in `owner_basis` |

`status_raw` preserves existing frontmatter without treating its overloaded value
as the new lifecycle automatically. In particular, ADR `accepted` status and
active-document lifecycle are different concepts.

## Existing Authority Model

| Repository class | Phase 1 authority classification | Default lifecycle | Conflict rule |
| --- | --- | --- | --- |
| `AGENTS.md` | Agent and contributor governance | active | Apply to repository work; its precedence relative to platform contracts is not stated precisely enough to invent one here |
| `docs/contracts/platform/*.md` | Normative platform behavior | active | Current platform authority; implementation drift is a conformance finding, not an automatic contract rewrite |
| Source-module contract documents | Normative only inside their declared component scope | active | Must conform to platform contracts and be discoverable from the owning component |
| `docs/operators/README.md` and canonical runbooks | Operational-canonical workflow | active | The running release CLI/API owns exact accepted arguments; prose must not invent commands |
| Accepted and proposed ADRs | Decision rationale and history | accepted or proposed | Explain why; do not override platform contracts; proposed is not implemented authority |
| Architecture boundary/component documents | Explanatory current design | active, draft, or historical | Link to normative owners; a normative clause here is an authority conflict |
| Engineering standards | Contributor policy or working standard | active | Subordinate to the engineering contract; must not create platform behavior contracts |
| Concepts, guides, and entry-point READMEs | Explanatory or operational summary | active | Summarize and link; do not silently redefine owned terms or guarantees |
| Plans, validation reports, discovery audits, incidents, and campaign dossiers | Working or historical evidence | active while executing, otherwise historical | Never become current behavior authority merely by describing a run or campaign |
| Component index and rendered SVGs | Generated derivative | active while current | Source metadata or Mermaid wins; generated content is never independent authority |
| Research evidence JSON | Immutable evidence artifact | historical | Describes a recorded operation, not current product truth |
| Code, schemas, constraints, CLI/API/MCP registrations, and tests | Implementation, enforcement, and proof evidence | active, historical, or explicitly missing | Demonstrate conformance; do not silently supersede a clear normative contract |

## Documentation Corpus Matrix

The documentation/source-local denominator is exactly **224 artifacts**:

| Slice | Count | Ledger kind |
| --- | ---: | --- |
| Tracked Markdown | 179 | `document` |
| Mermaid sources | 22 | `diagram-source` |
| Source-linked SVG derivatives under `docs/` | 17 | `generated-asset` |
| Source-less SVG asset under `docs/` | 1 | `unverified-doc-asset` |
| Research evidence JSON | 5 | `research-evidence` |

The 179 Markdown files comprise 117 architecture files and 62 files outside
architecture. Of the 117 architecture files, 114 are indexed component
documents; the architecture README, ADR README, and generated component index
are the three non-component files.

| Non-architecture Markdown group | Count | Authority | Lifecycle default | Primary owner |
| --- | ---: | --- | --- | --- |
| Root/project governance and portals | 2 | governance or explanatory | active | platform / architecture-docs |
| `docs/` entry points and getting started | 4 | explanatory or operational | active | architecture-docs / operations |
| Contract hierarchy | 5 | authority descriptor plus four normative contracts | active | platform contracts |
| Concepts | 5 | explanatory | active | named subsystem |
| Guides | 10 | operational/how-to | active | data, indicator-runtime, or decision-layer |
| Operator handbook | 1 | operational-canonical routing | active | operations |
| Engineering navigation, standards, and runbooks | 11 | explanatory, contributor policy, or operational | active | engineering/testing/docs/operations |
| Completed engineering migration/validation evidence | 6 | historical evidence | historical | data, collectors, or frontend |
| Incident index and records | 8 | active index plus historical evidence | active index / historical records | operations / execution-runtime |
| Completed plan ledgers | 2 | historical campaign evidence | historical | platform or execution-runtime |
| Research dossiers and corrective notice | 4 | three historical evidence dossiers plus one active explanatory notice | historical / active | research-orchestration |
| Grafana dashboard README | 1 | operational-intended | active | observability |
| Frontend starter README | 1 | explanatory residue | superseded | frontend |
| Market Profile module README and timing contract | 2 | module navigation plus module contract | active | indicator-runtime |

Every individual artifact is represented in `coverage-ledger.json`; the grouped
matrix above is a readable view, not a replacement denominator.

## Ownership Basis and Blind Spot

All 2,407 current ledger units have a provisional owning boundary, and every row
records how that boundary was assigned. That is accounting completeness, not
proof that repository ownership is healthy:

- no tracked `CODEOWNERS` exists;
- none of the 179 Markdown files declares `owner`, `owners`, `authority`,
  `lifecycle`, `audit_status`, `superseded_by`, or `replaced_by` frontmatter;
- architecture `code_paths` contains 1,216 declarations over 437 unique paths;
- 261 of those paths are shared by two or more component documents;
- `cli/main.py` alone is referenced by 37 component documents.

Consequently, `subsystem` frontmatter is the strongest existing ownership signal
for architecture components. All other ownership values are explicit audit
inferences and remain reviewable at Gate 1.

For implementation evidence, a literal architecture `code_paths` match takes
precedence over filename or directory heuristics; multiple literal owners are
recorded as `shared`. Prefix matches remain useful for passage coverage but do
not override a literal mapping. Mixed interface controllers are classified at
route/tool/resource granularity where their semantics cross persistence,
security, projection, observability, and runtime boundaries.

Some role-specific units intentionally have a narrower owner than the physical
file's aggregate implementation-path row. For example, a shared model module can
contain tables for several subsystems while its `schema-source` unit describes
the persistence-owned relational-schema-definition mechanism. Those units carry
the literal architecture component/boundary context so the distinction is
explicit rather than a silent same-path disagreement.

## Lifecycle Overrides Established by Content

The following records describe completed or pre-cutover work even where
frontmatter says `active` or is absent, so the ledger preserves `status_raw` and
classifies lifecycle separately as `historical`:

- `docs/engineering/canonical-fact-migration-{backup,discovery,validation}.md`;
- `docs/engineering/collector-operations-{discovery,validation}.md`;
- `docs/engineering/frontend-v2-operator-validation.md`;
- `docs/plans/backtest-dataset-boundary.md`;
- `docs/plans/platform-baseline-cleanup.md`;
- the BTC V3 and both Chainlink breakout campaign dossiers.

`docs/research-campaigns/CHAINLINK_RESEARCH_BOUNDARY_LIMITATIONS.md` remains an
active explanatory corrective notice. Its “Canonical Rerun Contract” heading
does not promote it into the normative contract hierarchy.

## Gate 1 Authority Decisions

These are recorded conflicts, not repairs:

1. `AUTONOMOUS_RESEARCH_AND_PROMOTION_ROADMAP.md` calls its autonomy matrix
   normative even though architecture documents are explanatory. Later work must
   either move the required rule into the existing normative hierarchy or remove
   the parallel-authority wording.
2. The exact precedence relationship between `AGENTS.md` contributor governance
   and platform behavior contracts should be stated rather than inferred.
3. Component-contract discovery is inconsistent: Market Profile owns a
   source-local timing contract with no inbound Markdown reference, while Candle
   Stats places a signal “contract” in explanatory architecture.
4. Repository ownership is inferred, not declared. Phase 2/3 must decide whether
   ownership belongs in existing frontmatter/validation without turning this
   campaign directory into a permanent authority source.
