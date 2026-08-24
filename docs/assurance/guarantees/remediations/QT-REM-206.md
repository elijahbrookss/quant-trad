---
remediation_id: QT-REM-206
guarantee_ids: QT-GUAR-EXECUTION-MODE-PLAYBACK-SEPARATION
lifecycle: proposed
owner: execution-runtime
required_reviewers: execution-runtime-owner,playback-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-206

**Close execution-mode and playback-mode coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

FAST and FULL are separated from playback and representative same-bar/fallback outcomes are tested, but no reviewed cross-product proves every execution-mode and playback-mode combination or defines the complete fallback denominator.

## Action

Review the execution/playback cross-product, clarify that shared causal discipline does not imply identical fills, and add deterministic cases for every admitted mode, intrabar availability state, and fallback outcome.

## Acceptance criteria

- Every admitted execution-mode and playback-mode combination is enumerated and independently selectable.
- FAST and FULL preserve their distinct reviewed causal resolution rules without claiming identical results.
- Missing or incomplete intrabar evidence produces one deterministic reviewed fallback and bounded warning behavior.

## Proof plan

Required proof definitions: `QT-PROOF-206`.

- Additional evidence: A reviewed execution/playback cross-product and intrabar-availability matrix.

## Review boundary

Execution-runtime and playback reviewers own the semantic distinction; classification cannot settle disputed mode wording.
