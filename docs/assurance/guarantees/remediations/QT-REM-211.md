---
remediation_id: QT-REM-211
guarantee_ids: QT-GUAR-SHARED-WALLET-MARKET-TIME-ARBITRATION
lifecycle: proposed
owner: wallet
required_reviewers: execution-runtime-owner,testing-owner,wallet-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-211

**Close shared-wallet arbitration denominator**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Same-bar ordering and sparse-calendar blocking are tested, but the frozen evidence does not close the set of candidate sources, clocks, cost overlays, concurrency races, and failure outcomes that share one wallet.

## Action

Review the arbitration key and admitted candidate-source denominator, then add deterministic property and race tests for source ordering, sparse clocks, delayed costs, cancellation, and failure recovery.

## Acceptance criteria

- One reviewed total-order key deterministically orders every admitted same-wallet candidate.
- Unresolved earlier market time blocks later candidates without deadlock across reviewed sparse-calendar cases.
- Repeated runs with permuted arrival order produce the same admitted wallet decisions and lineage.

## Proof plan

Required proof definitions: `QT-PROOF-211`.

- Additional evidence: A reviewed candidate-source, clock, and arbitration-key inventory with permutation cases.

## Review boundary

Wallet and execution-runtime reviewers own arbitration semantics and sparse-clock scope; the glossary collision remains unresolved.
