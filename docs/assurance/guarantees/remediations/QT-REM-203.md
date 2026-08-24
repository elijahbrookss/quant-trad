---
remediation_id: QT-REM-203
guarantee_ids: QT-GUAR-EXPLICIT-EXECUTION-EXIT-POLICY
lifecycle: proposed
owner: execution-runtime
required_reviewers: decision-layer-owner,execution-runtime-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-203

**Close explicit exit-policy configuration coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Representative trailing-stop and breakeven defaults are guarded, but there is no reviewed denominator for every exit-policy flag, stale field combination, configuration migration, and adapter path.

## Action

Enumerate the supported exit-policy surface, define explicit enablement and invalid stale-field combinations, and add parameterized tests across configuration parsing, normalization, and runtime execution.

## Acceptance criteria

- Every supported exit policy has one explicit activation field and reviewed default.
- Disabled or omitted policies cannot be re-enabled by stale distance or threshold fields.
- Invalid combinations fail before order or position state is mutated.

## Proof plan

Required proof definitions: `QT-PROOF-203`.

- Additional evidence: A reviewed parameter matrix covering every admitted exit-policy configuration.

## Review boundary

Decision-layer and execution-runtime reviewers own policy semantics; the remediation must not invent or change product exit behavior.
