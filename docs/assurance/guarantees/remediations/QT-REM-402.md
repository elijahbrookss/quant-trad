---
remediation_id: QT-REM-402
guarantee_ids: QT-GUAR-BUDGETED-CLOSED-CANDLE-MARKET-STREAM
lifecycle: proposed
owner: live-market-data
required_reviewers: execution-runtime-owner,market-data-owner,provider-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-402

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The tests cover representative reconnect, budget, closed-candle, and store-failure cases but do not close fatal provider errors, heartbeat staleness, shutdown races, or every supported stream implementation.

## Action

Approve a provider-neutral reconnect and candle-admission failure matrix, inventory each supported live stream, and add deterministic tests for all classified terminal and recovery paths.

## Acceptance criteria

- Each supported stream maps fatal, transient, heartbeat-stale, and budget-exhausted conditions to one reviewed outcome.
- Only persisted closed candles become runtime-visible decision inputs.
- Recovered reconnects emit bounded inspectable diagnostics.
- Terminal failure is visible and no retry continues beyond the reviewed budget.

## Proof plan

Required proof definitions: `QT-PROOF-402`.

Required environment profile: `python-nondb`.

Run the deterministic failure matrix after owner approval and retain commit-bound evidence; this definition is not an execution result.

## Review boundary

Market-data, provider, and execution-runtime owners review stream behavior; no live label may be interpreted as external-order authority.
