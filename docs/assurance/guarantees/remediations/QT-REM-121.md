---
remediation_id: QT-REM-121
guarantee_ids: QT-GUAR-DETERMINISTIC-SEQUENTIAL-EXPERIMENT-PLAN
lifecycle: proposed
owner: experiment-orchestration
required_reviewers: experiment-orchestration-owner,research-orchestration-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-121

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The named examples exercise plan validation, sequential execution, and an explicit warning gate, but they do not fully prove immutable hash behavior, only-window/variant step mutation, declared-variant verification, deterministic permutations, or resume/tamper rejection.

## Action

After experiment and research orchestration review, add property and tamper tests over canonical hashing, plan expansion, step-diff constraints, declared-variant verification, sequential resume, and immutable completed-step state.

## Acceptance criteria

- Equivalent plan inputs produce the same canonical identity and step order across the reviewed permutation set.
- Each generated step differs only by its declared window and variant coordinates; undeclared mutations are rejected.
- Variant mismatch, plan tampering, and incompatible resume state fail before execution.
- Experiment-orchestration, research-orchestration, and testing reviewers approve the property domain before any disposition or activation change.

## Proof plan

Required proof definitions: `QT-PROOF-124`.

Required environment profile: `python-nondb`.

Run only after owner and reviewer approval; retain a clean commit-bound result, property domain, and tamper/resume cases. This proof definition is not a result.

## Review boundary

Classification proposal only; it does not change experiment policy, Strategy semantics, resume behavior, adopt terminology, or activate the guarantee.
