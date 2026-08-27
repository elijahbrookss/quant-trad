# QT Core Promises

QT has six durable promises worth protecting as part of the system's identity.
They are intentionally written for an owner or developer deciding whether the
system can be trusted, not for an assurance specialist.

These promises summarize 22 records in the preserved guarantee inventory. They
do not activate those records: all 75 indexed guarantees remain unactivated.
The linked identifiers preserve the exact audit trail, while
[`assurance-maintenance.md`](engineering/assurance-maintenance.md) defines their
ongoing checking treatment.

## 1. Causal and canonical truth

**What QT promises:** historical information cannot change an earlier decision.

QT must not let future-known market information alter an output that was
already knowable. Canonical market facts are corrected through new immutable
revisions, and reading stored canonical facts must not quietly call a provider
or invent a replacement value.

If this promise fails, research can appear better than it was and prior results
can become impossible to explain.

Supporting properties (internal traceability):

- [`QT-GUAR-KNOWN-AT-PREFIX-INVARIANCE`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-known-at-prefix-invariance)
- [`QT-GUAR-CANONICAL-FACT-APPEND-ONLY`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-canonical-fact-append-only)
- [`QT-GUAR-PROVIDER-FREE-CANONICAL-READS`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-provider-free-canonical-reads)

Default protection is causal regression testing plus isolated database tests
for immutable revision behavior.

Stronger verification is justified when a research result must be trusted
across an external publication, release, customer, or capital boundary.

## 2. Frozen research authority

**What QT promises:** durable research evidence is frozen, replayable, and
explicitly admitted.

A durable backtest or Research Check must identify the exact frozen data it
used. Preview remains exploratory. Creating durable research memory is a
separate, explicit act, and a Check result cannot silently acquire authority it
was not given.

If this promise fails, QT can preserve conclusions that cannot be reproduced or
confuse an exploratory result with accepted evidence.

Supporting properties (internal traceability):

- [`QT-GUAR-FROZEN-DATASET-REPLAY`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-frozen-dataset-replay)
- [`QT-GUAR-BACKTEST-FROZEN-BINDING`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-backtest-frozen-binding)
- [`QT-GUAR-CHECK-PREVIEW-EVIDENCE-SEPARATION`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-check-preview-evidence-separation)
- [`QT-GUAR-CHECK-OBSERVATION-ADMISSION`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-check-observation-admission)
- [`QT-GUAR-CHECK-AUTHORITY-CEILING`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-check-authority-ceiling)

Default protection is deterministic replay and boundary testing, with an
isolated database where persisted frozen bindings are involved.

Stronger verification is justified when QT publishes a research result for an
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

Supporting properties (internal traceability):

- [`QT-GUAR-DERIVED-OUTPUT-TIMELINE`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-derived-output-timeline)
- [`QT-GUAR-STRATEGY-DECISION-ARTIFACT-SEPARATION`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-strategy-decision-artifact-separation)
- [`QT-GUAR-EXECUTION-MODE-PLAYBACK-SEPARATION`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-execution-mode-playback-separation)
- [`QT-GUAR-RUNTIME-EXECUTION-OWNERSHIP-QUALITY-CEILING`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-runtime-execution-ownership-quality-ceiling)

Default protection is ordinary cross-boundary regression testing and negative
tests that reject an unauthorized reconstruction path.

Stronger verification is justified before an execution boundary gains new
authority, especially any path that can affect external orders or capital.

## 4. Capital, order, and accounting integrity

**What QT promises:** orders, fills, exits, wallets, and accounting use one
controlled lifecycle.

External order submission stays closed unless a later explicit authority opens
it. Within the supported simulation boundary, exits are explicit, protective
orders preserve residual state, order history is append-only, fills enter
accounting once, wallet state is replayable, and terminal accounting reconciles
to canonical fills.

If this promise fails, a simulation can misstate risk or P&L; if an external
execution boundary is ever introduced, the same class of failure could affect
real capital.

Supporting properties (internal traceability):

- [`QT-GUAR-EXTERNAL-ORDER-SUBMISSION-CLOSED`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-external-order-submission-closed)
- [`QT-GUAR-EXPLICIT-EXECUTION-EXIT-POLICY`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-explicit-execution-exit-policy)
- [`QT-GUAR-PROTECTIVE-EXIT-RESIDUAL-TERMINAL-INTEGRITY`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-protective-exit-residual-terminal-integrity)
- [`QT-GUAR-CANONICAL-ORDER-LIFECYCLE`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-canonical-order-lifecycle)
- [`QT-GUAR-FILL-SETTLEMENT-SINGLE-INGRESS`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-fill-settlement-single-ingress)
- [`QT-GUAR-WALLET-INITIALIZATION-AND-LEDGER-REPLAY`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-wallet-initialization-and-ledger-replay)
- [`QT-GUAR-CANONICAL-FILL-ACCOUNTING-RECONCILIATION`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-canonical-fill-accounting-reconciliation)

Default protection is ordinary execution and accounting regression testing.
External-order closure is also checked at any real release boundary. No test may
enable external order submission to obtain evidence.

Stronger verification is required before real capital or external order
submission is ever authorized.

## 5. Credential confinement

**What QT promises:** provider secrets remain confined to the credential
boundary.

Application records carry credential references, not plaintext provider
secrets. Public metadata and normal logs must not expose secret material.

If this promise fails, provider accounts and any capital reachable through them
may be exposed.

Supporting property (internal traceability):

- [`QT-GUAR-PROVIDER-CREDENTIAL-REFERENCE-CONFINEMENT`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-provider-credential-reference-confinement)

Default protection is ordinary structural testing plus isolated integration
tests with synthetic credentials. Live credentials are never required evidence.

Stronger verification is justified for an external security review, a remotely
exposed deployment, or a change that expands credential reach.

## 6. Durable persistence and recovery

**What QT promises:** durable state has one authority and destructive recovery
is verified.

PostgreSQL is the sole durable application-data authority. Destructive archive
or recovery operations require explicit bounds and must be rehearsed against
isolated, disposable resources before their procedure is trusted.

If this promise fails, QT can split its durable truth, lose recoverability, or
delete data without a reliable restoration path.

Supporting properties (internal traceability):

- [`QT-GUAR-SOLE-POSTGRES-PERSISTENCE-AUTHORITY`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-sole-postgres-persistence-authority)
- [`QT-GUAR-DESTRUCTIVE-RECOVERY-VERIFICATION`](assurance/guarantees/GUARANTEES.md#guarantee-qt-guar-destructive-recovery-verification)

Default protection is ordinary schema-boundary validation, isolated database
integration, and an occasional separately approved recovery rehearsal. No
production or live system is used to obtain evidence.

Stronger verification is justified for a supported release or recovery
commitment, and it must use a real disposable source-and-restore rehearsal.

## What These Promises Do Not Mean

The promises do not claim perfect coverage, activation, a complete formal
evidence record, or an externally certified system. Passing tests and
engineering repairs are evidence; they do not independently change authority.
Strong exact-build evidence is introduced only when a real evidence audience
and trust boundary justify it.
