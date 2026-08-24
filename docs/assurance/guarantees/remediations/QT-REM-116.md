---
remediation_id: QT-REM-116
guarantee_ids: QT-GUAR-PROJECTION-DOES-NOT-CHANGE-INDICATOR-TRUTH
lifecycle: proposed
owner: indicator-runtime
required_reviewers: botlens-projection-owner,indicator-runtime-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-116

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Representative overlay-detail tests preserve Indicator outputs and Strategy decisions, but the frozen baseline does not close every registered projection family or every supported projection-mode transition, and the registry ownership finding remains unresolved.

## Action

After indicator-runtime and BotLens projection review, define a reviewed inventory of registered projection families and supported mode transitions, then add output/decision equality and graceful-degradation tests without selecting or consolidating competing registry authorities.

## Acceptance criteria

- The reviewed inventory enumerates every admitted projection family and supported mode transition.
- Canonical Indicator outputs and Strategy decisions remain equal across every enumerated projection mode.
- Unavailable or disabled projection work degrades according to existing owned behavior without mutating canonical truth.
- Registry ownership remains explicitly unresolved until its separate normative review.

## Proof plan

Required proof definitions: `QT-PROOF-119`.

Required environment profile: `python-nondb`.

Run only after owner and reviewer approval; retain a clean commit-bound result and the reviewed projection-mode denominator. This proof definition is not a result.

## Review boundary

Classification proposal only; it does not choose a registry authority, change projection or Indicator semantics, adopt terminology, or activate the guarantee.
