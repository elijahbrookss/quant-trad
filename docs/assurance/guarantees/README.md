# Guarantee Assurance Inventory

Status: **75 indexed records preserved; all 75 remain unactivated.**

This directory preserves an exact assurance inventory over the repository's
authority hierarchy. It does not create requirements, replace a platform or
component contract, elevate an ADR, settle a conflict, or make implementation
behavior normative. If this inventory and an authoritative source disagree,
the authoritative source wins.

The inventory was calibrated against a frozen subset and then completed for all
75 records at audit baseline
`d46e40bf55caeea12f4ccbde640c71f271eaf9c4`. Completeness means only that the
frozen denominator is represented. It does not mean that every record should be
maintained as a first-class guarantee or that any record is adequately enforced,
proven, or activated.

Current ongoing treatment is defined by:

- [ADR 0066: Scale Assurance To Consequence And Trust Boundaries](../../architecture/decisions/0066-scale-assurance-to-consequence-and-trust-boundaries.md)
- [QT Core Promises](../../core-promises.md)
- [Assurance Maintenance](../../engineering/assurance-maintenance.md)

Those documents group the existing identifiers for maintenance purposes. They
do not modify this inventory's classifications or states.

## Preserved Artifacts

- `registry.json` contains the 75 classifications, authority and enforcement
  references, maturity assessments, and unresolved findings.
- `proof-catalog.json` contains 85 proof definitions and their coverage. A proof
  definition is a description of required evidence, not a result.
- `GUARANTEES.md` is the deterministic human view generated from those two
  files. Do not edit it by hand.
- `remediations/` contains 68 concrete proposed records with their original
  owners, reviewers, gaps, acceptance criteria, and proof plans.
- `schemas/*.v1.schema.json` preserves the versioned interchange models.
- `EXECUTION_LIFECYCLE.md`, `attestations/`, `docker/assurance/`, and
  `scripts/assurance/` preserve the exact-build, execution, cleanup,
  attestation, and publication capability.
- `scripts/docs/guarantees.py` validates the durable inputs and renders or
  checks the generated human view.

Historical review packets remain under
`docs/plans/documentation-reconciliation/` with their original paths and
wording. They are evidence of how the inventory was derived, not current
workflow instructions.

The adopted vocabulary index is
[`docs/contracts/platform/04_glossary.md`](../../contracts/platform/04_glossary.md).
The frozen [historical terminology proposal](../../plans/documentation-reconciliation/proposed-glossary.md)
is not normative. Terminology adoption does not activate a guarantee, prove
enforcement, execute a proof, or resolve a remediation.

## Independent Recorded Axes

The preserved registry keeps these questions separate:

1. What lifecycle and authority does the indexed claim have?
2. What registry disposition did the frozen classification assign?
3. Has a distinct activation review activated the claim?
4. Does the frozen implementation statically conform?
5. How mature is enforcement?
6. How mature is proof, and is it automated, manual, or mixed?
7. Is remediation not required, pending, concretely recorded, or resolved?

An aligned implementation, accepted ADR, successful remediation, or passing
test does not activate a guarantee. The checked-in registry has no activation
decision or attestation references. Finding IDs and historical crosswalk aliases
do not count as remediation records.

## Current Checking Model

Normal unit, integration, static, and frontend tests are the default way to
protect QT. Real persistence semantics use disposable isolated databases.
Credential tests use synthetic values. Destructive recovery uses a separately
approved isolated rehearsal. External order submission and production or live
systems are never enabled merely to obtain evidence.

The stronger runner remains available only when a stated evidence audience and
trust boundary require exact source, build, environment, cleanup, attestation,
or publication provenance. Its availability does not make it a default
development or merge requirement.

## Inventory Commands

```text
python scripts/docs/guarantees.py validate
python scripts/docs/guarantees.py render
python scripts/docs/guarantees.py check
```

`validate` checks strict JSON, shapes, cross-references, repository paths, proof
mappings, and activation prerequisites. `render` updates the human view
deterministically. `check` validates and byte-compares the checked-in view
without changing files.

The retained exact-build executor is documented in `EXECUTION_LIFECYCLE.md`.
It must fail honestly when an image, isolation prerequisite, cleanup result, or
manual recovery result is unavailable. Cleanup recovery is not a proof result,
and publication never commits or activates a guarantee.

## Change Boundary

Do not mutate these historical records merely to match the simpler maintenance
model. Changes to product meaning belong in the owning contract or ADR. Changes
to the six core promises or their constituent mapping require architecture-owner
review. Activation, remediation closure, attestation publication, terminology
adoption, and removal of retained machinery remain separate explicit decisions.
