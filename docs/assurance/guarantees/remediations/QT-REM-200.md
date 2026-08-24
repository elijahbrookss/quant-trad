---
remediation_id: QT-REM-200
guarantee_ids: QT-GUAR-MODE-AWARE-RUNTIME-COMPOSITION
lifecycle: proposed
owner: execution-runtime
required_reviewers: architecture-owner,execution-runtime-owner,testing-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-200

**Close runtime-composition coverage and documentation drift**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The frozen implementation guards representative mode-specific construction paths, but there is no closed collaborator inventory and DOC-RUNTIME-COMPOSITION-001 leaves paper/live lifecycle wording inconsistent.

## Action

Review the admitted composition denominator, reconcile explanatory lifecycle wording without expanding live authority, and add a generated or static inventory that binds every admitted collaborator to a capability guard.

## Acceptance criteria

- The reviewed denominator lists every admitted runtime mode and mode-specific collaborator with no unowned remainder.
- Unsupported requested capabilities fail before runtime execution on every admitted construction path.
- Explanatory paper/live lifecycle wording is reviewed against the accepted ADR and does not authorize external order submission.

## Proof plan

Required proof definitions: `QT-PROOF-200`.

- Additional evidence: A reviewed generated or static composition-root inventory bound to the exact source revision.

## Review boundary

Architecture, execution-runtime, and testing reviewers own the denominator and documentation reconciliation; this draft does not resolve DOC-RUNTIME-COMPOSITION-001 or activate the guarantee.
