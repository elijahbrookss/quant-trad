# Documentation Writing Standard

Quant-Trad docs should explain system intent clearly enough that a future reader
can tell the difference between a normal limitation and a bug. The reader is
assumed to be competent, busy, and human. Do not make them reverse-engineer the
meaning from a checklist.

Write like you are helping future-you remember why this part of the system
exists and what it is trying to preserve.

## Core Standard

A good doc teaches the normal shape first. It starts with what the thing is
for, follows the path data or control actually takes, and names the rule the
system is trying to protect. Constraints should feel like consequences of that
intent, not like detached legal clauses.

Prefer this kind of prose:

```text
BotLens is allowed to be late, unavailable, or partial because it is a
projection. It is not allowed to invent execution truth. When projection state is
missing, the honest output is unavailable state, not an empty chart that looks
valid.
```

Avoid writing that only looks organized:

```text
Purpose: projection availability.
Scope: selected-symbol state.
Non-goal: execution semantics.
Strict contract: do not fabricate valid chart state.
```

The second version has sections, but the first version carries the idea.

Headings should name the story they introduce. A heading like `State And Truth`
or `Inputs / Outputs` is usually a warning sign: it can collect unrelated facts
without explaining why the reader should care. Prefer headings such as `How
Wallet State Stays Replayable`, `Source Facts Handed Downstream`, or `Evidence
Runtime Must Leave Behind`. Those names force the section to carry meaning
instead of becoming a bucket.

## What Every Doc Should Preserve

- State shipped behavior or explicitly contracted behavior.
- Make the source of truth obvious.
- Explain the normal flow before the exception path.
- Name the identifiers, clocks, or cursors that make behavior correct.
- Say what the system refuses to fake.
- Label real gaps as caveats instead of hiding them in optimistic prose.
- Link to contracts or source paths when exact behavior matters.

This does not require a fixed section template. Use headings only when they help
the reader move through the explanation. A short doc with three clear paragraphs
is better than a long doc with ten impressive headings.

## Architecture Docs

Architecture docs should be boundary-first, but they should not read like
policy binders. The reader should come away knowing what the boundary is trying
to protect and why the rest of the implementation follows from that.

Frontmatter makes architecture docs discoverable and gives semantic changes an
explicit review route. It does not turn explanatory architecture prose into a
platform contract, prove that a reviewer approved a change, or activate a
guarantee.

### Component Metadata Version 2

New component docs and owner-reviewed metadata migrations use this shape:

```yaml
---
metadata_version: 2
component: indicator-runtime-boundary
subsystem: indicator-runtime
layer: boundary
doc_type: architecture
status: active
semantic_owner: indicator-runtime
required_reviewers:
  - architecture-documentation-owner
  - indicator-runtime-owner
tags:
  - indicators
  - runtime
code_paths:
  - src/indicators
module_contracts:
  - src/indicators/market_profile/docs/timing_contract.md
---
```

The fields mean:

- `metadata_version` is `2` for this complete schema. Do not add version 2
  fields piecemeal to a legacy document.
- `component` is a repository-unique, stable kebab-case component slug.
- `subsystem` and `layer` are kebab-case classification slugs.
- `doc_type` is `architecture`, `adr`, or `validation`.
- `status` is `active`, `accepted`, `draft`, `historical`, or `superseded`.
- `semantic_owner` is the canonical boundary or role accountable for the
  component's meaning. It is not inferred from the folder, `subsystem`, source
  paths, implementation behavior, audit records, or `CODEOWNERS`.
- `required_reviewers` is the sorted, unique, nonempty set of canonical role
  slugs required to review semantic changes. The list is a routing requirement,
  not evidence that a person holds the role or completed the review.
- `tags` is a nonempty list of unique stable discovery slugs.
- `code_paths` is a nonempty list of unique, normalized, existing
  repository-relative paths used
  for navigation and coverage. Shared paths are valid and do not make this
  field a file-ownership registry.
- `module_contracts` is a sorted, unique list of source-module contract paths
  owned by this component. Use `module_contracts: []` when the component has
  none.

Role-to-person or role-to-team resolution belongs in repository governance.
`CODEOWNERS` may help enforce review routing, but it is neither semantic
ownership nor product authority. The generated architecture index is the human
view of this metadata; it is derived and must not be edited as another source of
truth.

### Source-Module Contract Discovery

A source-module contract is structurally discoverable only when its owning
version 2 component doc lists it in `module_contracts` and the contract declares
matching scope and ownership:

```yaml
---
module_contract_version: 1
contract_kind: source-module
owning_component: indicator-runtime-boundary
component_scope: market-profile
semantic_owner: indicator-runtime
status: active
---
```

Only an active `doc_type: architecture` component doc may own a current module
contract. The contract path must stay inside the repository and be equal to or
beneath a path declared by the owning component in `code_paths`.
`owning_component` and `semantic_owner` must exactly match that component's
metadata, and one contract has one owning component. Draft, historical, and
superseded contracts may stay linked for lineage, but they are not current
contract authority.

File placement, a contract-like filename, frontmatter without an owning link,
or implemented behavior cannot qualify a source-module contract. A discovered
and reviewed source-module contract remains subordinate to platform contracts.
Discovery and remediation success still do not activate a guarantee.

### Metadata Transition

Existing unversioned architecture docs keep their legacy seven-field metadata
until their semantic owner and required reviewers approve a complete migration.
A migration adds all version 2 fields atomically; it never guesses ownership
from the current subsystem or from historical discovery and review routing. New
component docs use version 2. The transition mechanism may grandfather only the
exact pre-version-2 document paths, and each reviewed rollout removes its
migrated paths rather than allowing legacy metadata to expand.

After that, let the doc choose its own shape. Common useful moves are:

- Explain the intent in plain language.
- Walk through the normal path.
- Name what owns truth and what is only a projection.
- Explain the one or two constraints that make bugs recognizable.
- End with links to related docs when the reader needs more detail.

Contracts can stay stricter. They are allowed to be terse and normative because
their job is to settle exact behavior. Explanatory docs should make those rules
understandable without copying the contract into another file.

## Diagram Budget

Mermaid diagrams are useful when they show meaning at a glance: a truth flow, a
state transition, a causality order, or a boundary that is hard to hold in your
head from prose alone.

Do not add diagrams because a doc feels empty. Do not make every component own a
diagram. A diagram earns its place when it reduces reader load.

When a diagram is worth adding:

- Keep the Mermaid source beside the doc it explains, usually in a local
  `diagrams/` directory.
- Keep the graph small enough to understand in under 30 seconds.
- Use real system names such as `run_id`, `known_at`, `run_seq`, or
  `overlay_commit_seq` when those names carry meaning.
- Render SVGs through the repo script so the quick-reference `.svg` sits beside
  the `.mmd` source in the same local diagram folder.

When a diagram is not worth adding, write the sentence instead.

## Quick Review Before Finishing

- Can a reader tell what the system wants to do?
- Can a reader tell what would be a bug instead of user error?
- Did the doc explain the normal path before the weird path?
- Are exact rules linked to contracts or source instead of repeated loosely?
- Did every diagram, table, and bullet list earn its keep?
