# Guarantee Assurance Index

Status: **Gate 2 proposal; no guarantee is active.**

This directory is an assurance index over the repository's existing authority
hierarchy. It does not create requirements, replace a platform or component
contract, elevate an ADR, settle a conflict, or make implementation behavior
normative. A source referenced as `primary` remains the owner of the claim. A
`contradicted` record may instead retain multiple `conflicting` sources and no
primary so calibration does not silently choose a winner. If this index and an
authoritative source disagree, the source wins.

Phase 2A calibrates the model against twelve of the seventy-five Phase 1
candidates at frozen baseline
`d46e40bf55caeea12f4ccbde640c71f271eaf9c4`. The sample is deliberately
representative, not exhaustive. Whole-system classification starts only after
Gate 2 approval.

## Artifacts

- `registry.json` records provisional classifications, exact authority and
  enforcement references, maturity assessments, and unresolved findings.
- `proof-catalog.json` records stable proof requirements and their coverage.
  It contains no execution result.
- `GUARANTEES.md` is the deterministic human view generated from those two
  files. Do not edit it by hand.
- `schemas/*.v1.schema.json` document the three versioned interchange models.
- `attestations/README.md` defines result semantics and the rules for creating
  immutable, commit-bound attestations. Phase 2A creates no synthetic PASS
  attestation.
- `scripts/docs/guarantees.py` validates the durable inputs and renders or
  checks the human view.

The proposed glossary remains in the reconciliation campaign directory through
Gate 2 and until a separate terminology-adoption review. Its location and
status prevent vocabulary review from silently changing the normative read
order.

## Independent axes

The registry keeps these questions separate:

1. What lifecycle and authority does the indexed claim have?
2. What provisional registry disposition did calibration assign?
3. Has a distinct activation review activated the claim?
4. Does the frozen implementation statically conform?
5. How mature is enforcement?
6. How mature is proof, and is it automated, manual, or mixed?
7. Is remediation not required, pending, concretely recorded, or resolved?

An aligned implementation or passing proof does not activate a guarantee.
While `scope.gate` is `gate_2_pending`, validation requires an active-guarantee
count of zero. Gate 2 model approval cannot be cited as a guarantee-activation
decision. Finding IDs and Phase 1 crosswalk aliases do not count as remediation
records.

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

Phase 3 may add `verify-guarantees`; Phase 2A does not execute arbitrary proof
recipes or treat test presence as a fresh result.

## Review boundary

Gate 2 reviewers are being asked whether this model and calibration are fit for
whole-system use. They are not being asked to activate any record, adopt the
proposed, blocked, or deferred terminology or aliases, resolve
`QT-CONFLICT-007`, repair product semantics, or approve deletion or archival.
Those actions require their own authority and review.
