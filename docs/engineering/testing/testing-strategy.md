# Testing Strategy

## Purpose

Tests protect meaningful behavior, risk boundaries, and regressions. Coverage
and test counts are not goals or evidence of production readiness.

The [platform contracts](../../contracts/README.md) and accepted decisions define
intended behavior. Tests check the implementation against those contracts; a
passing test supports only the behavior and environment it actually exercises.
[ADR 0066](../../architecture/decisions/0066-scale-validation-to-consequence-and-trust-boundaries.md)
explains how verification scales with consequence and the audience that must
trust the result.

## Default Posture

Start with the affected contract and the consequence of failure. Add or update
tests when a change affects an important outcome, crosses a real boundary, or
fixes a regression. Do not add tests merely because a function is new or a file
has no coverage.

Use focused checks during development, then satisfy the applicable handoff
checks in [AGENTS.md](../../../AGENTS.md#normal-validation-matrix) and the
[developer workflow](../developer-workflow.md#validation). Declining a low-value
new test does not waive existing validation. Architectural churn is a reason to
test a stable public contract, not to defer protection for risky behavior.

## What Should Be Tested Now

The [six core promises](../../core-promises.md) connect high-consequence behavior
to its owning contracts and decisions. Use them to identify the relevant risks:

| Boundary | Valuable protection |
| --- | --- |
| Causal market truth | Known-at timing, prefix invariance, immutable Fact revisions, explicit gaps, and provider-free canonical reads |
| Frozen research authority | Exact frozen inputs and versions, deterministic replay, preserved historical meaning, and explicit Check-to-Observation admission |
| Decision and execution authority | One `initialize -> apply_bar -> snapshot` timeline; Strategy decisions separate from fills; reporting, Playback, CLI, MCP, and UI respecting their owning contracts |
| Capital, order, and accounting integrity | Order transitions, partial fills, fees, slippage, sizing, margin, reservations, settlement, wallet reconciliation, and fail-closed external-order submission |
| Credential confinement | Synthetic secrets remaining behind the credential boundary and absent from public payloads and logs |
| Durable persistence and recovery | Clean schema and historical cutovers, constraints, transaction rollback, fencing, archive/reference integrity, and bounded source-to-restore recovery |

Import/bootstrap, configuration, API responsiveness, frontend adapters, and
service boundaries also need regression protection when their failure changes
supported behavior. This is not an exhaustive test inventory.

Integration testing is already part of QT's validation. Use it where real
interactions determine correctness; isolated unit tests cannot prove PostgreSQL
constraints, process ownership, container wiring, or recovery. Some broader
properties have representative coverage only. Check the actual test selection
and assertions before claiming a flow is protected.

## Supported Commands And Environment Boundaries

Choose checks by what changed and what the test actually needs. Multiple rows
may apply; broad or cross-system changes use the full normal validation matrix.

| Change or boundary | Focused feedback and applicable handoff checks |
| --- | --- |
| Documentation, glossary, architecture metadata, or indexes | `make validate-docs`; this refreshes the architecture index and checks documentation contracts |
| Backend, domain, CLI, service, runtime, reporting, or configuration | Focused test files/node IDs first, then `make backend-check` for the ordinary non-database Python corpus |
| Persistence, schema, repository, recovery guard, or database-backed behavior | `./scripts/ci/run_test_suite.sh db <test-path-or-node-id>` for focused work; the same command without arguments for the broad disposable DB row; check whether specialized fixtures are also needed |
| Frontend, frontend adapters, or API-view contracts | `make frontend-check` for Node-native and JSX tests plus the production Vite build |
| Deployment scripts or Compose/configuration | Shell syntax and disposable Compose rendering from the normal validation matrix; use the [CI topology](ci-test-topology.md#3-deployment-contract) for additional image/runtime proof scope |
| Destructive storage, archive, or recovery behavior | Relevant owned disposable topology and failure/recovery cases; follow the [DB/recovery coverage map](../developer-workflow.md#exact-source-ci-equivalence-for-broad-dbrecovery-handoff) rather than assuming the broad DB profile exercises every optional case |
| Every handoff | `git diff --check`, review known failures, and preserve unrelated changes; follow the repository's commit and clean-tree requirements |

Focused non-database checks use the repository Python environment and explicit
test paths, for example `QT_OMIT_DB_TESTS=1 python -m pytest -q <test-path>`.
This omits DB-marked files; it is not evidence for their behavior. Use
`make help` for existing helpers such as `test-reporting`, `test-reporting-api`,
and `test-botlens`. Use the checked-in Python and frontend lockfiles.

`make check` combines repository hygiene, documentation, and ordinary Python
validation. `make check-all` adds frontend tests and the production build.
Neither includes the disposable database or specialized recovery proofs. A
broad `pytest` invocation does not replace the DB command: DB-marked tests skip
without explicit isolation flags. Clean bootstrap is a separate empty-database
check; it does not prove an existing database can be migrated.

The DB runner builds checkout source subject to `.dockerignore`, rather than
mounting the developer checkout. It generates a synthetic database identity and
Compose project for each invocation, uses an internal network without a host DB
port, and attempts teardown of that project's containers, network, volumes, and
locally built test image on success and failure. Inspect cleanup failures.
The broad DB profile is not a promise of a fresh database for every test.

The Python harness disables implicit repository `.env` and `secrets.env`
discovery. Provide test inputs explicitly. Required dependencies must be
installed; missing supported dependencies must not silently become skips.
Normal validation uses synthetic data and isolated resources, never shared or
production databases, real credentials, live providers, or external orders.

## Unit Test Style Rules

### Test Behavior, Not Implementation

Assert outcomes, invariants, and contracts. For example, prove that a partial
fill updates quantity, fees, reservation, and remaining order state coherently;
do not merely assert the sequence of private helper calls. An internal detail
is worth asserting only when it is itself the contract.

Use small fixtures, real in-memory objects, and controlled fakes. Avoid large
snapshot payloads, excessive mocks, trivial wrapper tests, and new fixture
frameworks that add maintenance without protecting meaningful behavior. A reader
should see the rule, the failing case, and why the assertion matters.

### Keep External Boundaries Closed

Stub provider, credential, network, container, and order-execution boundaries
unless the test explicitly owns an isolated integration environment. Unit tests
must not reach storage, telemetry, or network services through accidental paths.
The `integration` marker describes a test; it does not provision infrastructure,
isolate resources, or exclude the test from ordinary Python validation. DB tests
must declare `db` and use the supported disposable runner or prescribed CI
environment. Declare other infrastructure dependencies in their owning fixture
and execution profile.

Use recognizable synthetic secrets to detect leakage. External-order tests
prove admission and composition fail closed, without contacting a venue. A mock
can trap a forbidden call, but cannot replace the behavior the test claims to
validate.

### Control Time And Concurrency Directly

Coordinate ordering and progress with events, barriers, deferred promises,
injected clocks, or explicit queue drains. Timeouts may guard deadlocks; narrow
wall-clock thresholds should not stand in for a progress assertion. Real sleeps
are reasonable when elapsed time is the behavior or an external library offers
no controllable clock; explain that dependency and allow scheduling margin.

### Build Historical Database States Explicitly

Migration tests create the smallest valid historical schema in dependency order,
assert the pre-state, run the actual migration, and inspect the result. Do not
create today's full ORM schema and drop pieces to imitate history: current
constraints may depend on migration-owned functions.

Use fresh disposable database identities when ordering, destructive cutover,
extensions, or teardown are part of the claim. Exercise rollback and cleanup
failure paths. Fixtures must never discover or reuse a developer or live DSN.

### Use Generated Cases And Controlled Defects Selectively

Generated/property-based cases help where varying numeric bounds, transitions,
ordering, or normalization exposes a real rule. Prefer a concrete regression
example when it explains the risk clearly. For a high-consequence rule, a
temporary representative defect in a disposable copy may confirm the test
detects it. Remove the defect immediately; detection does not replace review or
system validation, and does not justify broad mutation tooling for a score.

## Regression Tests

A real bug usually merits a focused regression: reproduce the failing input or
state, assert the intended outcome, and keep unrelated internals free to change.
For determinism, compare the relevant outputs under identical frozen inputs,
parameters, and versions. For known-at behavior, show that a later observation
cannot change an earlier decision. For persistence or recovery, exercise the
real database or owned recovery topology when that boundary caused the bug.

## PR Test Expectations

Record what was exercised and what remains unknown:

- Identify the affected contract/risk and the checks actually run, including
  relevant environment and scope.
- Explain why no new test was needed when existing coverage protects the change
  or the proposed test would only mirror implementation.
- Report unavailable prerequisites as unavailable, not passing. An expected
  environment-specific skip must identify the check's actual execution route;
  otherwise record a current reason, owner, and exit condition or fix routing.
- Preserve known failures and reconcile them with focused reproduction or an
  explicit disposition. A later green suite does not erase them.

The host-run `pr-suite` is only the ordinary non-database screen. The complete
workflow also defines frontend, database, container/runtime, deployment
rehearsal, and recovery checks. Work-branch selection and release-targeting PR
requirements are described in the
[CI topology](ci-test-topology.md#conservative-work-branch-selection). Read the
actual required jobs, selected cases, results, and artifacts; a green selected
iteration is not a complete qualification run.

For broad DB/recovery handoff, the developer workflow permits a successful
complete CI attempt for the exact final integrated commit instead of repeating
the same broad checks locally. Its source/artifact and coverage requirements
still apply, including focused checks for omitted specialized cases. Report
accepted CI evidence as CI evidence, not as a local pass.

Contracts state intended behavior; code and workflow definitions show what is
implemented; completed checks show what was exercised. None alone establishes
universal correctness, production throughput, recovery on an actual host, or
production readiness. Deployment admission and authorization remain separate
under the [developer workflow](../developer-workflow.md#exact-source-ci-equivalence-for-broad-dbrecovery-handoff).
This strategy records policy and test boundaries, not the qualification status
of a particular release or archive candidate.
