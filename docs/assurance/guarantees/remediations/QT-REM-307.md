---
remediation_id: QT-REM-307
guarantee_ids: QT-GUAR-BOUNDED-NONCANONICAL-OBSERVABILITY
lifecycle: proposed
owner: observability
required_reviewers: observability-owner,persistence-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-307

**Close observability sink and bound inventory**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

Named repository and exporter tests cover representative bounds, but no closed sink inventory establishes boundedness and visible loss for every noncanonical observability path.

## Action

Inventory observability sinks and their cardinality, queue, retention, overflow, and dropped-evidence behavior, then add a generated completeness guard.

## Acceptance criteria

- Each observability sink has an explicit bound or a reviewed exclusion.
- Each lossy or overflow path emits inspectable diagnostic evidence.
- New sinks fail the inventory check until bounds and loss visibility are classified.

## Proof plan

- Generate the sink and bound inventory from source registrations and schema definitions.
- Extend representative overflow and cardinality tests per sink family.
- Retain QT-PROOF-307 as initial repository and exporter evidence.

## Review boundary

Observability and persistence owners review sink boundaries; the remediation cannot promote diagnostic evidence to canonical execution truth.
