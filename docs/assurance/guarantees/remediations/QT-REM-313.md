---
remediation_id: QT-REM-313
guarantee_ids: QT-GUAR-PR-VERIFICATION-TOPOLOGY
lifecycle: proposed
owner: testing
required_reviewers: ci-owner,database-test-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-313

**Reconcile and prove pull-request verification topology**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The testing strategy has stale topology language, the frozen workflow has four jobs rather than five, and the existing selectors do not prove the complete job-to-suite-to-database-isolation mapping.

## Action

Produce the exact workflow topology and selector map for owner review, distinguishing jobs from steps, then add a structural contract test and reconcile documentation after approval.

## Acceptance criteria

- Each hosted job and step maps to its exact shell suite and pytest profile.
- Every database-marked selector requires the isolated disposable database guard.
- The reviewed documentation states four jobs and identifies clean bootstrap and database-marked verification as distinct steps in one job.

## Proof plan

- Add a deterministic workflow-to-suite topology parser and contract test.
- Retain QT-PROOF-313 for static workflow evidence and QT-PROOF-314 for isolated database behavior.
- Verify the isolated database selector against a disposable PostgreSQL service.

## Review boundary

CI, testing, and database-test owners review topology wording and required lanes; this draft does not edit the testing strategy or workflow.
