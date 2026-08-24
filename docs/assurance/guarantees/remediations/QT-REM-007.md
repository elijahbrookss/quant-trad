---
remediation_id: QT-REM-007
guarantee_ids: QT-GUAR-RUNTIME-PERSISTENCE-FAILURE-BOUNDARY
lifecycle: proposed
owner: execution-runtime
required_reviewers: botlens-projections-owner,execution-runtime-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-007

This proposal records unproved failure combinations. It does not change the
durable-runtime or BotLens projection boundary.

## Gap

Named guards cover durable writer failure and degradable projection pressure,
but the mapped proof set is incomplete for overflow, drain timeout, terminal
flush, asynchronous consumer failure, and their combinations. That prevents an
adequate proof-maturity classification.

## Action

After execution-runtime and BotLens-projections review, define a bounded failure
matrix for canonical persistence and noncanonical projection paths. Add fault
injection for each admitted overflow, timeout, drain, and consumer combination
and retain explicit projection degradation state.

## Acceptance criteria

- Every reviewed durable failure combination prevents successful runtime
  completion and preserves the underlying error.
- Every reviewed projection-only failure leaves durable execution truth intact
  while exposing degraded visibility.
- Terminal drain behavior is bounded and has explicit success and failure
  outcomes.
- The matrix owns its denominator and exact proof selectors.

## Proof plan

Run the fault-injection matrix in the credential-free runtime test environment,
record every enumerated combination and selector, and retain results in a later
exact-source attestation.

## Review boundary

BotLens-projections owner and execution-runtime owner.
