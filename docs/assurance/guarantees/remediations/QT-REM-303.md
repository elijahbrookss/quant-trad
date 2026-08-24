---
remediation_id: QT-REM-303
guarantee_ids: QT-GUAR-ASYNC-JOB-OWNERSHIP-FENCING
lifecycle: proposed
owner: async-jobs
required_reviewers: persistence-owner,research-orchestration-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-303

**Close asynchronous effect inventory and fencing coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The database tests cover key reclaim and transaction cases but no closed inventory proves that every job-owned effect uses the token-and-generation fence.

## Action

Generate and review the asynchronous effect write-path inventory, then bind each included effect to the fenced repository transaction or an explicit exception decision.

## Acceptance criteria

- Every asynchronous job completion and domain-effect write path is present in the reviewed inventory.
- Each included path validates both ownership token and generation in the committing transaction.
- New job-owned effect paths fail the inventory check until classified.

## Proof plan

- Add a deterministic static inventory of async-job effect commit paths.
- Extend isolated database race tests to every classified effect family.
- Retain QT-PROOF-303 as the initial transactional proof.

## Review boundary

Persistence and research-orchestration owners review the effect denominator and any exceptions; no new job semantics are adopted by this draft.
