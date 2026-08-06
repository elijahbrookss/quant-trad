---
component: scientific-research-authority
subsystem: research-orchestration
layer: boundary
doc_type: architecture
status: active
tags:
  - research
  - protocols
  - datasets
  - holdout
  - statistics
  - autonomy
code_paths:
  - src/research_science
  - portal/backend/service/research/authority.py
  - portal/backend/service/research/authority_repository.py
  - portal/backend/controller/research.py
  - portal/backend/db/models.py
  - cli/main.py
  - tests/test_research_science
  - tests/test_portal/test_research_authority.py
---
# Scientific Research Authority

## Implemented boundary

Phase 4 is implemented inside the existing application and primary PostgreSQL
boundary. It is a scientific protocol, controlled operation set, and durable
state projection—not a collection of pretend institutional microservices.

```text
authorized immutable protocol
  -> one open experiment family
  -> budgeted train/validation attempts (all terminal outcomes retained)
  -> immutable candidate artifact bundle
  -> family closed with every attempt terminal
  -> database-unique one-use holdout reservation
  -> sealed internal evaluation
  -> scientific certificate
  -> holdout feedback release
```

The public API never provides a holdout dataset binding, reservation token, or
sealed result. The in-process holdout executor resolves the private binding only
with the one-time reservation capability. Provider fetch keys and dataset
bindings are recursively rejected from trial inputs.

## Dataset fence

Protocols are created before trials and pin exactly one chronological,
non-overlapping train, validation, and final holdout assignment. Every
assignment must resolve through the canonical market-data repository to an
existing `market_dataset.v1` artifact whose ID matches its dataset hash, whose
declared hash matches, whose series are non-empty, and whose frozen ranges cover
the assigned window. Resolution reads frozen storage and performs no provider
fetch.

The research agent receives the public protocol. For a sealed historical
holdout, its dataset ID, hash, and window are redacted. Trial registration
derives the dataset binding from the private protocol; the caller may choose
only `train` or `validation`, never a dataset. A material protocol change
requires a new protocol ID/hash and family.

## Immutable protocol manifest

`economic_claim_intent` is required before the protocol is admitted and is
part of the immutable protocol hash. A family and every attempt bind that hash;
an exploratory run therefore cannot later be relabeled selection-eligible.
Selection requires a fresh run under a selection protocol.

The same manifest pins the complete research contract:

- frozen instrument universe and train/validation/holdout assignments;
- allowed strategy mutation dimensions and complete search/feedback budget;
- primary metric direction and minimum benchmark-relative effect, plus
  secondary, safety, and benchmark metrics;
- minimum samples, trades, calendar coverage, and exposure;
- minimum execution-quality class and named execution stresses;
- walk-forward, purge, embargo, significance, multiplicity, and robustness
  rules;
- version identities for statistical methods and governing policies; and
- protocol authorizer and authorization request identity.

The application derives the authorizer and request identity from the admitted
authority operation. It verifies that every frozen dataset covers the declared
instrument universe. An economic protocol cannot require less than X2 or omit
execution stresses. A selection/promotion protocol cannot omit secondary,
safety, or benchmark metrics.

This is a workflow fence, not a claim about global knowledge. A person or
process with database, shell, repository, or independent provider access may
already know historical public data. Phase 4 does not attempt institution-grade
capability isolation.

## Assurance classes

| Class | Claim |
|---|---|
| `NONE` | No holdout non-exposure claim. |
| `AUTHOR_DECLARED` | The author declares non-exposure; QT cannot prove it. |
| `PLATFORM_CONTROLLED_HISTORICAL` | Normal research operations withheld the historical binding and feedback until the candidate and family were frozen. Prior external knowledge is not provable. |
| `EXTERNALLY_ATTESTED` | Reserved for a future external custodian and attestation artifact. Admission currently fails closed. |
| `FORWARD_UNSEEN` | Reserved for data that did not exist when the candidate was frozen. Admission currently fails closed until a forward-allocation authority exists. |

S3 requires at least `PLATFORM_CONTROLLED_HISTORICAL`. The stronger two labels
exist in the vocabulary but cannot be claimed by the current implementation.

## Search accounting and validation feedback

Each admitted attempt has a family ordinal, request identity, derived dataset
binding, immutable input hash, estimated/actual time and compute, lineage, and a
terminal result of `completed`, `failed`, `invalid`, or `abandoned`. Rejected
agent proposals are retained as append-only family events without pretending
they ran. Idempotent retries with the same request identity return the original
attempt. A new request with the same protocol-bound trial fingerprint is
retained as a terminal `duplicate`, links the original attempt, consumes an
attempt slot, and reserves no runtime or compute because it is not run.
Meaningfully different requests consume the declared resources.

The protocol caps attempts, estimated runtime, compute, and validation-feedback
uses. `parent_attempt_ids` must name attempts in the same family.
`influenced_by_attempt_ids` must name completed validation attempts and consumes
the separate feedback budget. Exhaustion fails before admission.
Every family evidence view reports maximum, used/reserved, and remaining budget
for attempts, runtime, compute, and validation feedback, plus terminal-status
and rejected-proposal counts.

## Walk-forward and leakage controls

The initial S2 boundary uses chronological folds and a context-only warm-up.
The contamination horizon is derived as the maximum of feature lookback, label
horizon, maximum holding period, and order expiration. Both purge and embargo
equal that declared derived horizon; callers cannot insert a smaller arbitrary
gap.

A completed validation attempt must prove:

- the exact protocol fold count;
- the derived purge and embargo;
- context-only warm-up;
- a flat position at each scoring boundary;
- no pending orders at each scoring boundary; and
- no signals admitted before scoring begins;
- execution quality at or above the protocol minimum;
- every named execution stress;
- every primary, secondary, and safety metric; and
- the benchmark-relative effect floor; and
- minimum sample, trade, calendar, and exposure thresholds.

The sealed holdout executor enforces the same execution, stress, metric, and
sufficiency contract before consuming the one-use holdout capability. A failed
admission leaves the holdout reserved and unconsumed so an operator can correct
the runner evidence without allocating a new dataset.

This class does not claim continuous-forward state across folds. That remains a
higher future protocol version.

## Frozen candidate

Before holdout reservation, the candidate fingerprint pins:

- strategy artifact and parameter artifact;
- execution-model artifact;
- metric/threshold contract;
- train and validation dataset hashes (the holdout remains private);
- the complete private protocol hash, which also pins the holdout, benchmark,
  statistical settings, and robustness requirements; and
- all source evidence hashes.

The repository verifies these fields against the completed validation attempt
and protocol. Candidate mutation is impossible after holdout reservation.

## Scientific quality

Scientific quality is independent of execution class, reproducibility, product
economics, and governance state.

| Class | Enforced meaning |
|---|---|
| `S0` | Reproducible exploration only. |
| `S1` | Immutable protocol, benchmark, complete search accounting, retained failures, and budget compliance. |
| `S2` | S1 plus chronological walk-forward, derived leakage/boundary controls, frozen candidate, benchmark-relative effect, minimum sample/trade/calendar/exposure, protocol-required execution quality, and safety metrics. |
| `S3` | S2 plus a database-unique one-use sealed holdout and candidate-before-holdout evidence. |
| `S4` | S3 plus declared robustness, adjusted multiplicity, positive uncertainty bound, cost stress, and latency stress. |

Bonferroni and Holm family-wise adjustments and a deterministic moving-block
bootstrap are implemented. No single p-value certifies a strategy. Claim intent
sets the minimum certificate: exploration S0, economic S2, selection S3, and
promotion S4.

## API and CLI

REST endpoints under `/api/research/authority` create/read protocols, create
families, register/complete attempts, create budgeted typed graphs, freeze
candidates, close families, reserve holdouts, certify, and read public evidence.
There is deliberately no public endpoint for holdout binding resolution or
evaluation.

The matching operator commands are under:

```text
./scripts/qt research authority ...
```

Applied operations always write database audit evidence. Disabling the optional
CLI log does not disable this durable authority trail.

## Operational boundary

Phase 4 grants controlled search and evidence authority only. It grants no
shadow, paper, live, external-order, credential, capital, or deployment access.
