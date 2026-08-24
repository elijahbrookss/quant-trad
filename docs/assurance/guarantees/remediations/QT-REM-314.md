---
remediation_id: QT-REM-314
guarantee_ids: QT-GUAR-CONTRACT-DRIVEN-GENERIC-SURFACES
lifecycle: proposed
owner: platform-engineering
required_reviewers: platform-contract-owner,provider-owner,runtime-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-314

**Close generic-surface absence coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Representative provider and execution tests align with the engineering contract, but ARCH-COVERAGE-001 and REGISTRY-DYNAMIC-001 leave the generic-surface denominator and dynamically registered cases incomplete.

## Action

Define and review the complete generic-surface inventory, allowed registration mechanisms, forbidden named-instance branches, and missing-capability failure behavior.

## Acceptance criteria

- Every classified generic surface is discovered by a deterministic inventory.
- Named-instance branches and guessed fallbacks fail the contract check unless explicitly reviewed outside the generic layer.
- Missing capabilities fail through the documented contract boundary.

## Proof plan

- Generate registry and generic-module inventories from supported registration points.
- Extend static branch and fallback detection across the reviewed surface.
- Retain QT-PROOF-315 as representative provider and runtime evidence.

## Review boundary

Platform-contract, provider, and runtime owners review the denominator and registration model; this draft does not ban product-specific behavior outside generic surfaces.
