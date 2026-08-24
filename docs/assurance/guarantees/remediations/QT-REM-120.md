---
remediation_id: QT-REM-120
guarantee_ids: QT-GUAR-EFFECTIVE-STRATEGY-RESOLUTION-PARITY
lifecycle: proposed
owner: decision-layer
required_reviewers: decision-layer-owner,reporting-owner,strategy-service-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-120

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Named resolver and strategy-service paths are aligned, but the frozen baseline lacks a closed call-site inventory proving that every preview, Bot, runtime, reporting, and provenance consumer resolves the same effective configuration and hash.

## Action

After the named owner review, generate a shared-resolver call-site manifest and add cross-consumer parity tests showing that identical base Strategy and variant inputs yield the same effective configuration and stable identity without independent reconstruction.

## Acceptance criteria

- The reviewed manifest enumerates every in-scope preview, Bot, runtime, reporting, and provenance consumer.
- Every enumerated consumer calls the owned resolution boundary rather than independently reconstructing variant semantics.
- Cross-consumer tests produce the same effective configuration and hash for identical inputs and reject mismatched identity.
- Decision-layer, reporting, strategy-service, and testing reviewers approve the denominator before any disposition or activation change.

## Proof plan

Required proof definitions: `QT-PROOF-123`.

Required environment profile: `python-nondb`.

Run only after owner and reviewer approval; retain a clean commit-bound result and the reviewed call-site manifest. This proof definition is not a result.

## Review boundary

Classification proposal only; it does not redefine effective Strategy semantics, resolve reporting conflicts, adopt terminology, or activate the guarantee.
