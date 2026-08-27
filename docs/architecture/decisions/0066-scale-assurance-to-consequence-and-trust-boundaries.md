---
component: adr-consequence-scaled-assurance
subsystem: engineering-governance
layer: decision
doc_type: adr
status: accepted
metadata_version: 2
semantic_owner: architecture-owner
required_reviewers:
  - architecture-owner
  - platform-contract-reviewer
  - testing-owner
module_contracts: []
tags:
  - adr
  - assurance
  - testing
  - evidence
  - governance
code_paths:
  - docs/core-promises.md
  - docs/engineering/assurance-maintenance.md
  - docs/assurance/guarantees/registry.json
  - docs/assurance/guarantees/proof-catalog.json
  - scripts/docs/guarantees.py
  - scripts/assurance/verify_guarantees.py
---

# ADR 0066: Scale Assurance To Consequence And Trust Boundaries

## Status

Accepted on 2026-08-26.

## Context

QT's exhaustive architecture review identified 75 properties the repository
tries to keep true, described 85 proof definitions, and recorded 68 concrete
remediation proposals. That work was valuable: it exposed authority boundaries,
made conflicts reviewable, clarified terminology, and preserved exact links
between claims, implementation, tests, and owner decisions.

The assurance-architecture review used clean source
`31c34f2f93579d71875e3588c59d8dbfdde8c3c2`. The three environment inspection
candidates prepared for that source were never approvals, and no cataloged proof
was executed through them.

The resulting execution system can also bind proof results to an exact source
tree, runner build, Docker daemon, immutable image, cleanup record, and staged
publication. Those controls are appropriate when evidence must cross a real
trust boundary. Applying them uniformly to documentation integrity, ordinary UI
state, routine engineering constraints, and capital- or research-critical
properties would make the evidence system a large product of its own without a
matching reduction in risk.

Safe testing and elaborate evidence provenance are separate concerns. A test
may still need no network, no live credentials, no external-order capability, a
disposable database, or verified cleanup even when it does not need a signed or
exact-build proof record.

## Decision

QT uses minimum sufficient assurance: the consequence of a property being
wrong and the audience that must trust the evidence determine its treatment.
The repository does not use one universal assurance ladder.

The 75 indexed properties have one of three ongoing treatments:

1. **Core-promise constituent.** Twenty-two indexed properties support six
   plain-language QT promises. They receive durable ownership and risk-focused
   tests. Stronger evidence is added only when the promise crosses a release,
   capital, security, recovery, customer, legal, or other explicit trust
   boundary.
2. **Owned engineering invariant.** Fifty-one properties remain important
   engineering rules. Their default protection is the ordinary test suite,
   static validation, review, and subsystem ownership.
3. **Historical or deferred context.** Two deployment properties remain useful
   review evidence but do not create recurring proof obligations until QT has
   the deployment boundary they describe.

The exact mapping is maintained in
[`docs/engineering/assurance-maintenance.md`](../../engineering/assurance-maintenance.md).
The six human-facing promises are summarized in
[`docs/core-promises.md`](../../core-promises.md).

Normal tests are the default evidence. Real-database behavior uses disposable,
isolated integration environments. Destructive recovery uses a separately
reviewed rehearsal. Release evidence is collected at an actual release
boundary. Exact runner builds, daemon and wheelhouse identity, source admission,
immutable attestations, and staged publication are reserved for a stated trust
boundary that requires those properties.

Proof results are internal engineering evidence by default. They are not
published or treated as durable attestations merely because the machinery can
produce them.

The existing inventory remains frozen historical evidence:

- all 75 registry records retain their identifiers and current classifications;
- all 85 proof definitions remain preserved;
- all 68 remediation records remain preserved with their existing states; and
- every indexed guarantee remains unactivated.

This decision changes ongoing treatment only. It does not rewrite the registry,
close a remediation, assert that a proof passed, or activate a guarantee. It
also creates no new registry, schema, runner, assurance level, or parallel
authority hierarchy.

## Consequences

Owners can understand QT through six durable promises without treating every
implementation constraint as an externally meaningful guarantee. Routine
changes continue to receive routine tests, and high-consequence boundaries keep
the isolation needed to test them safely.

The checked-in proof and attestation machinery remains available as retained
audit capability. It is not a prerequisite for normal development or for every
future repository change. Any later use must name the evidence audience, exact
trust boundary, owner, cadence, retention need, and failure response before
requiring the stronger machinery.

The prior completion plan to admit environments and formally publish every
automated proof definition is replaced by this consequence-scaled maintenance
model. Ordinary validation remains required; exhaustive formal publication does
not.

Passing a test or completing a remediation still does not activate a guarantee.
Activation remains a separate owner decision against the authority applicable
at that time.

## Rejected Alternatives

- **Prove and attest all 75 properties uniformly.** This repeats ordinary test
  work and makes exact-build provenance the default without a corresponding
  trust boundary.
- **Delete the exhaustive inventory and runner.** This would discard useful
  architecture discovery, traceability, and a capability that may be justified
  later.
- **Choose a target number of guarantees first.** Counts are an output of risk
  and ownership decisions, not an architectural objective.
- **Treat safe isolation as inseparable from attestation.** Network, credential,
  database, recovery, and cleanup controls remain useful even when evidence is
  not formally published.
- **Let passing tests activate indexed guarantees.** Test success is evidence,
  not authority.

## Maintenance Boundary

Changes to the six promises or the 22 constituent mapping require architecture
owner review. Subsystem owners may update ordinary tests for the 51 engineering
invariants without introducing stronger evidence machinery. Moving either
historical deployment property into recurring assurance requires an approved,
supported deployment and release boundary.

The generated registry view remains generated. Historical source paths and
packets remain literal evidence and are not renamed to fit this decision.

## References

- [QT Core Promises](../../core-promises.md)
- [Assurance Maintenance](../../engineering/assurance-maintenance.md)
- [Guarantee Assurance Index](../../assurance/guarantees/README.md)
- [Platform System Contract](../../contracts/platform/00_system_contract.md)
- [Platform Engineering Contract](../../contracts/platform/03_engineering_contract.md)
