# QT Core Promises

QT has six durable promises worth protecting as part of the system's identity.
They summarize the high-consequence boundaries a developer should consider
before changing data, research, execution, security, or persistence behavior.

The promises do not replace the platform contracts or accepted architecture
decisions. They provide a readable map from the outcome QT protects to the
documents and tests that define the details.

## 1. Causal and canonical truth

**What QT promises:** historical information cannot change an earlier decision.

QT must not let future-known market information alter an output that was
already knowable. Canonical market facts are corrected through new immutable
revisions, and reading stored canonical facts must not quietly call a provider
or invent a replacement value.

If this promise fails, research can appear better than it was and prior results
can become impossible to explain.

Supporting rules:

- [Known-at prefix invariance](architecture/decisions/0044-enforce-known-at-prefix-invariance.md)
- [Canonical Fact append-only revisions](architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md)
- [Provider-free canonical reads](architecture/decisions/0050-use-one-canonical-append-only-market-data-store.md)

QT normally checks these rules with causal regression tests and isolated
database tests for immutable revision behavior. Stronger verification is
justified when a research result must be trusted across an external publication,
release, customer, or capital boundary.

## 2. Frozen research authority

**What QT promises:** durable research evidence is frozen, replayable, and
explicitly admitted.

A durable backtest or Research Check must identify the exact frozen data it
used. Preview remains exploratory. Creating durable research memory is a
separate, explicit act, and a Check result cannot silently acquire authority it
was not given.

If this promise fails, QT can preserve conclusions that cannot be reproduced or
confuse an exploratory result with accepted evidence.

Supporting rules:

- [Frozen Dataset deterministic replay](architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md)
- [Exact frozen binding for canonical backtests](architecture/decisions/0051-require-frozen-datasets-for-canonical-backtests.md)
- [Check preview and durable evidence separation](architecture/decisions/0062-use-frozen-bindings-for-durable-check-evidence.md)
- [Explicit Check-to-Observation admission](architecture/decisions/0065-use-explicit-frozen-check-admission-for-new-research-observations.md)
- [Check verdict authority ceiling](architecture/research-orchestration/CHECK_EVIDENCE_BOUNDARY.md)

QT normally checks these rules with deterministic replay and boundary tests,
using an isolated database when persisted frozen bindings are involved.
Stronger verification is justified when QT publishes research for an
independent consumer or certifies it for a higher-authority decision.

## 3. Decision and execution authority

**What QT promises:** decision truth, execution behavior, and presentation
remain separate.

Indicators publish through one runtime timeline, Strategies produce decision
artifacts, and the execution runtime owns fills and execution-quality claims.
Playback, reporting, and visual projections may explain those facts but may not
quietly become alternate decision or execution engines.

If this promise fails, the same strategy can mean different things depending on
which screen, report, or execution path is used.

Supporting rules:

- [One derived-output runtime timeline](contracts/platform/01_runtime_contract.md)
- [Strategy decisions remain separate from execution](architecture/decisions/0005-keep-strategy-decisions-separate-from-execution.md)
- [Execution mode remains separate from playback](architecture/decisions/0006-keep-execution-semantics-independent-from-playback.md)
- [The runtime owns execution semantics and declares its quality ceiling](architecture/decisions/0056-pin-venue-neutral-execution-contexts-per-run.md)

QT normally checks these rules with cross-boundary regression tests and
negative tests that reject unauthorized reconstruction paths. Stronger
verification is justified before an execution boundary gains new authority,
especially any path that could affect external orders or capital.

## 4. Capital, order, and accounting integrity

**What QT promises:** orders, fills, exits, wallets, and accounting use one
controlled lifecycle.

External order submission stays closed. Within the supported simulation
boundary, exits are explicit, protective orders preserve residual state, order
history is append-only, fills enter accounting once, wallet state is replayable,
and terminal accounting reconciles to canonical fills.

If this promise fails, a simulation can misstate risk or P&L. If an external
execution boundary is ever introduced, the same class of failure could affect
real capital.

Supporting rules:

- [External order submission remains closed](architecture/decisions/0049-keep-live-order-submission-closed.md)
- [Execution and exit policy is explicit](architecture/decisions/0045-require-explicit-execution-and-exit-policy.md)
- [Protective exits preserve residual and terminal integrity](architecture/decisions/0041-use-canonical-execution-plan-and-order-fill-semantics.md)
- [Canonical order history is append-only](architecture/decisions/0057-use-append-only-canonical-order-lifecycle.md)
- [Canonical fill settlement is the single accounting ingress](architecture/decisions/0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md)
- [Wallet initialization and ledger commits are replayable](architecture/execution-runtime/WALLET_AND_CAPITAL_BOUNDARY.md)
- [Terminal accounting reconciles to canonical fills](architecture/decisions/0043-reconcile-accounting-from-canonical-fills-and-wallet-ledger.md)

QT normally checks these rules with execution and accounting regression tests.
External-order closure is also checked at every supported release boundary. No
test may enable external order submission to obtain evidence. Stronger
verification is required before real capital or external submission could ever
be authorized.

## 5. Credential confinement

**What QT promises:** provider secrets remain confined to the credential
boundary.

Application records carry credential references, not plaintext provider
secrets. Public metadata and normal logs must not expose secret material.

If this promise fails, provider accounts and any capital reachable through them
may be exposed.

Supporting rule:

- [Provider credential references](architecture/decisions/0024-use-provider-credential-references.md)

QT normally checks this rule with structural tests and isolated integration
tests using synthetic credentials. Live credentials are never required
evidence. Stronger verification is justified for an external security review,
a remotely exposed deployment, or a change that expands credential reach.

## 6. Durable persistence and recovery

**What QT promises:** durable state has one authority and destructive recovery
is verified.

PostgreSQL is the sole durable application-data authority. Destructive archive
or recovery operations require explicit bounds and must be rehearsed against
isolated, disposable resources before their procedure is trusted.

If this promise fails, QT can split its durable truth, lose recoverability, or
delete data without a reliable restoration path.

Supporting rules:

- [One PostgreSQL persistence boundary](architecture/decisions/0009-use-one-postgres-persistence-boundary-and-retained-event-ledger.md)
- [Tiered archive and recovery boundary](architecture/decisions/0053-use-tiered-market-structure-archive-and-replay-boundary.md)

QT normally checks these rules with schema-boundary validation, isolated
database integration, and an occasional separately authorized recovery
rehearsal. No production or live system is used to obtain evidence. Stronger
verification is justified for a supported release or recovery commitment, and
must use a real disposable source-and-restore rehearsal.

## What These Promises Do Not Mean

These promises are not claims of perfect coverage or external certification.
Passing tests provide evidence that the current implementation respects the
documented boundaries; they do not make a broader claim than the tests actually
exercise. Exact-build records or formal publication are added only when a real
audience and trust boundary justify their cost.
