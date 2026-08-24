---
remediation_id: QT-REM-210
guarantee_ids: QT-GUAR-WALLET-INITIALIZATION-AND-LEDGER-REPLAY
lifecycle: proposed
owner: execution-runtime
required_reviewers: execution-runtime-owner,persistence-owner,testing-owner,wallet-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-210

**Close wallet initialization and replay coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The active runtime contract and representative append/replay tests establish initialization and ordered ledger evidence, but initialization paths, restart recovery, and every wallet event family are not covered by one reviewed denominator.

## Action

Define the complete run-scoped wallet event model, enumerate all initialization and recovery paths, and add replay tests for missing, duplicate, late, reordered, and divergent initialization across every admitted wallet event family.

## Acceptance criteria

- Each run has exactly one absolute wallet-initialized fact before wallet activity.
- Replay applies wallet events in canonical run sequence and rejects missing, duplicate, late, or divergent initialization.
- Restart recovery reconstructs the same wallet state and evidence identity without a second mutation.

## Proof plan

Required proof definitions: `QT-PROOF-210`.

- Additional evidence: A reviewed wallet-event family and initialization/recovery path inventory.

## Review boundary

Execution-runtime, persistence, and wallet reviewers own initialization and replay semantics; no ledger conflict is resolved here.
