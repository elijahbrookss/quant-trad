---
component: adr-consequence-scaled-validation
subsystem: engineering
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - testing
  - evidence
code_paths:
  - docs/core-promises.md
  - docs/engineering/testing/testing-strategy.md
  - scripts/ci/run_test_suite.sh
  - docker/docker-compose.test.yml
---

# ADR 0066: Scale Validation To Consequence And Trust Boundaries

## Status

Accepted on 2026-08-26.

## Context

QT has many properties that should remain true. They do not all carry the same
risk when broken, and they do not all need the same kind of evidence.

Ordinary engineering rules are usually protected best by clear contracts,
focused tests, integration tests where needed, and the normal regression suite.
Exact build identity, immutable result publication, or other heavy evidence
tracking adds cost and is useful only when someone outside the normal
development boundary must independently trust a result.

Safe testing is a separate concern from elaborate evidence tracking. A test may
still require no network, no live credentials, no external-order capability, a
disposable database, or verified cleanup even when it does not need a formal
record of the exact machine and build that ran it.

## Decision

QT uses the minimum verification sufficient for the consequence of failure and
the audience that must trust the result.

The six [core promises](../../core-promises.md) describe the system's
high-consequence boundaries in plain language. Their normal protection is the
platform contracts, accepted architecture decisions, implementation guards,
and risk-focused tests linked from that document.

Other important engineering rules remain in their subsystem contracts,
architecture documents, and tests. They do not need a parallel registry merely
to be maintained correctly.

Normal tests are the default evidence. PostgreSQL behavior uses disposable,
isolated integration environments. Destructive recovery uses a separately
authorized source-to-restore rehearsal. Release evidence is collected at an
actual release boundary.

Stronger evidence is added only when the affected boundary justifies it, such
as:

- external order submission or real capital;
- security-sensitive credential access;
- a supported recovery commitment;
- an external, customer, legal, or regulatory commitment; or
- an independently certified release.

Before adding exact-build provenance or formal publication, the change must
state who needs the evidence, what they need to trust, how long the evidence
must remain useful, and what happens when verification fails.

## Consequences

Developers can understand QT through its current contracts and six promises
without learning a second verification system. Routine changes continue to use
routine tests, while dangerous boundaries keep the isolation needed to test
them safely.

This decision intentionally leaves room for stronger verification later. It
does not make exact-build records, formal validation records, or publication
workflows part of normal development before a real trust boundary requires them.

A passing test supports the documented claim within the test's scope. It does
not override a platform contract or establish a broader claim than the test
actually exercises.

## Rejected Alternatives

- **Give every desirable property the strongest verification.** This repeats
  ordinary test work and creates permanent machinery without a matching
  reduction in risk.
- **Treat safe isolation and formal evidence as one feature.** Network,
  credential, database, recovery, and cleanup controls remain valuable on their
  own.
- **Use a target inventory size.** The right mechanism follows from the
  consequence of failure, not from reaching a preferred count.
- **Treat passing tests as product authority.** Tests check behavior; contracts
  and accepted decisions define intended meaning.

## Maintenance Boundary

Changes to a core promise require review of the relevant platform contract and
accepted decisions. Ordinary tests may evolve with their subsystem without
introducing stronger evidence machinery. Any move into external, security,
recovery, release-certification, or capital trust must explicitly revisit this
decision.

## References

- [QT Core Promises](../../core-promises.md)
- [Platform System Contract](../../contracts/platform/00_system_contract.md)
- [Platform Engineering Contract](../../contracts/platform/03_engineering_contract.md)
- [Testing Strategy](../../engineering/testing/testing-strategy.md)
