---
remediation_id: QT-REM-309
guarantee_ids: QT-GUAR-DIAGNOSTICS-NOT-EXECUTION-TRUTH
lifecycle: proposed
owner: runtime-observability
required_reviewers: execution-runtime-owner,observability-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-309

**Close runtime diagnostic persistence and authority coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The selected tests cover diagnostic normalization and a mocked persistence handoff but do not prove durable persistence, loss visibility, and non-authority across the complete diagnostic family.

## Action

Define the runtime diagnostic inventory and add isolated persistence and authority-boundary tests for each classified diagnostic family.

## Acceptance criteria

- Each named diagnostic family has a durable or explicitly reviewed lossy evidence path.
- Persistence failure and evidence loss are observable.
- No diagnostic event directly mutates execution truth in the reviewed inventory.

## Proof plan

- Add isolated persistence selectors for clock-gap, Docker-lifecycle, and watchdog degradation evidence.
- Generate a diagnostic-to-execution-mutation dependency check.
- Retain QT-PROOF-309 as normalization and handoff evidence.

## Review boundary

Execution-runtime and observability owners review the diagnostic denominator and persistence expectations; this draft does not redefine execution truth.
