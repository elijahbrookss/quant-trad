---
remediation_id: QT-REM-404
guarantee_ids: QT-GUAR-TYPED-CONSUMER-FACT-REQUIREMENTS
lifecycle: proposed
owner: market-data-contracts
required_reviewers: consumer-contract-owner,data-owner,instrument-identity-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-404

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The requirement resolver enforces the named role and fallback boundaries, but the repository has no reviewed closed inventory of every consumer declaration and its allowed schema fields.

## Action

Generate a typed consumer-requirement inventory and a structural validation that rejects endpoint, table, schedule, or fallback-source fields outside the owned market-data contract.

## Acceptance criteria

- Every in-scope consumer declaration appears in the generated inventory.
- Each declaration uses typed fact requirements and canonical instrument roles.
- Provider endpoints, storage tables, collector schedules, and fallback sources are absent from consumer-owned schemas.
- Required unavailability fails and optional unavailability remains typed.

## Proof plan

Required proof definitions: `QT-PROOF-404`.

Required environment profile: `python-nondb`.

Bind the generated inventory and selected resolver tests to a clean commit; this proof definition is not a result.

## Review boundary

Consumer-contract, data, and identity owners approve the field denominator; source modules remain implementation evidence and terms remain unadopted.
