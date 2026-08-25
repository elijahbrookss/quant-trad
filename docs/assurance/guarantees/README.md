# Guarantee Assurance Index

Status: **Phase 2B classification complete; no guarantee is active.**

This directory is an assurance index over the repository's existing authority
hierarchy. It does not create requirements, replace a platform or component
contract, elevate an ADR, settle a conflict, or make implementation behavior
normative. A source referenced as `primary` remains the owner of the claim. A
`contradicted` record may instead retain multiple `conflicting` sources and no
primary so calibration does not silently choose a winner. If this index and an
authoritative source disagree, the source wins.

Phase 2A calibrated the model against twelve of the seventy-five Phase 1
candidates at frozen baseline
`d46e40bf55caeea12f4ccbde640c71f271eaf9c4`. After Gate 2 approval, Phase 2B
applied that model to all seventy-five candidates. Classification completeness
means only that the frozen candidate denominator is represented. It does not
mean that a claim is normative, adequately enforced, proven, or activated.

## Artifacts

- `registry.json` records provisional classifications, exact authority and
  enforcement references, maturity assessments, and unresolved findings.
- `proof-catalog.json` records stable proof requirements and their coverage.
  It contains no execution result.
- `GUARANTEES.md` is the deterministic human view generated from those two
  files. Do not edit it by hand.
- `schemas/*.v1.schema.json` document the versioned interchange models.
- `EXECUTION_LIFECYCLE.md` separates pre-execution Docker admission from the
  cleanup-verified environment admission embedded in an attestation and defines
  the immutable draft, execution, cleanup, interruption, and finalization
  boundary.
- `attestations/README.md` defines result semantics and the rules for creating
  immutable, commit-bound attestations. Phase 2A and Phase 2B create no
  synthetic PASS attestation.
- `scripts/docs/guarantees.py` validates the durable inputs and renders or
  checks the human view.

The adopted vocabulary index is
[`docs/contracts/platform/04_glossary.md`](../../contracts/platform/04_glossary.md).
The frozen [Phase 2 proposal](../../plans/documentation-reconciliation/proposed-glossary.md)
remains historical campaign evidence and is not normative. Terminology
adoption does not activate a guarantee, prove enforcement, execute proof, or
resolve a remediation.

## Independent axes

The registry keeps these questions separate:

1. What lifecycle and authority does the indexed claim have?
2. What provisional registry disposition did the calibrated model assign?
3. Has a distinct activation review activated the claim?
4. Does the frozen implementation statically conform?
5. How mature is enforcement?
6. How mature is proof, and is it automated, manual, or mixed?
7. Is remediation not required, pending, concretely recorded, or resolved?

An aligned implementation or passing proof does not activate a guarantee.
Gate 2 model approval cannot be cited as a guarantee-activation decision. The
checked-in Phase 2B snapshot keeps every guarantee unactivated and has no
activation decision or attestation references. Finding IDs and Phase 1
crosswalk aliases do not count as remediation records. Concrete remediation
records bind the exact owning role and a sorted, nonempty `required_reviewers`
role set. A generic review-required flag without those reviewer roles is
insufficient for whole-system classification.

## Commands

```text
python scripts/docs/guarantees.py validate
python scripts/docs/guarantees.py render
python scripts/docs/guarantees.py check
```

`validate` checks strict JSON, shapes, cross-references, repository paths,
proof mappings, activation prerequisites, and Gate 2 constraints. `render`
updates the human view deterministically. `check` validates and byte-compares
the checked-in view without changing files.

Phase 3 verification is implemented by `scripts/assurance/verify_guarantees.py`
and remains separately authorized from registry activation. Automated Docker
execution requires the versioned execution-admission and lifecycle records in
`EXECUTION_LIFECYCLE.md`. A missing local image, isolation prerequisite, or
cleanup proof is reported honestly rather than replaced by a weaker
environment. Manual recovery remains bound to its separately reviewed
procedure.

## Review boundary

The next review boundary is the subsystem and normative-decision review before
Phase 3. Reviewers are being asked to resolve the specific ownership,
terminology, authority, proof-environment, and contract questions listed in the
Phase 2B report. They are not being asked to activate any record, treat an ADR,
source-module contract, `AGENTS.md`, implementation behavior, or this registry
as automatic activation authority, adopt proposed or blocked terminology,
repair product semantics, or approve deletion or archival. Those actions
require their own authority and review.
