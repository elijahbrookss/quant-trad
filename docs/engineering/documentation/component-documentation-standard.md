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

Frontmatter is still required for architecture docs so the generated component
index can find them:

- `component`
- `subsystem`
- `layer`
- `doc_type`
- `status`
- `tags`
- `code_paths`

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
