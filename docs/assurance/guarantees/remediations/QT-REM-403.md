---
remediation_id: QT-REM-403
guarantee_ids: QT-GUAR-PROVIDER-CAPABILITY-AUTHORIZATION
lifecycle: proposed
owner: provider-boundary
required_reviewers: provider-owner,security-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-403

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The selected Coinbase tests cover declared support and a public no-credential path, but no closed capability matrix proves authorization admission and credential-store isolation for every provider operation.

## Action

Generate the provider-operation capability and authorization matrix and add credential-store access spies plus missing-capability and missing-authorization tests for every classified operation.

## Acceptance criteria

- Every provider operation has one declared capability and authorization mode.
- Every public operation completes without credential-store reads.
- Missing capabilities and missing required authorization fail at admission.
- Provider and security owners approve the operation denominator.

## Proof plan

Required proof definitions: `QT-PROOF-403`.

Required environment profile: `python-nondb`.

Run only after the reviewed capability matrix is generated and commit-bound; this proof definition is not a result.

## Review boundary

Provider and security owners review authorization semantics; this remediation does not define credential encryption or adopt provider terminology.
