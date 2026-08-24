---
remediation_id: QT-REM-005
guarantee_ids: QT-GUAR-REPORTS-DURABLE-TRUTH-PROJECTION
lifecycle: proposed
owner: reporting
required_reviewers: execution-persistence-owner,reporting-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-005

This proposal records a reporting coverage gap and does not change report or
runtime truth ownership.

## Gap

The canonical RunResearchDataset builder is aligned with durable run evidence,
but the broad words “all reports” are not backed by a closed report-surface
inventory or an absence proof against alternate Indicator evaluation and
invented runtime evidence. `ARCH-COVERAGE-001` and `CI-TRACE-001` remain open.

## Action

After reporting and execution-persistence review, enumerate every report and
RunResearchDataset construction entrypoint. Bind each to durable inputs and add
a structural rule that rejects Indicator/runtime recomputation or fallback
evidence creation outside the owning runtime.

## Acceptance criteria

- A reviewed manifest accounts for every reporting construction surface.
- Each surface identifies its durable inputs, owning projection, and exact
  proof selector.
- Representative missing-evidence cases remain explicit rather than invoking
  an Indicator or inventing runtime facts.
- The claim scope is narrowed if owners cannot close the denominator.

## Proof plan

Run the reporting manifest/static rule and mapped credential-free report-data
pytest cases in `python-nondb`. Capture the enumerated report surface and exact
results in a later source-bound attestation.

## Review boundary

Execution-persistence owner and reporting owner.
