---
remediation_id: QT-REM-306
guarantee_ids: QT-GUAR-BLOCKING-API-WORK-OFFLOAD
lifecycle: proposed
owner: api-runtime
required_reviewers: api-owner,runtime-services-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-306

**Close asynchronous API blocking-call coverage**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The static contract test enumerates known mixed asynchronous files and the named projector path, leaving newly added blocking calls outside a closed discovery rule.

## Action

Define and review a repository-wide asynchronous API surface inventory with typed blocking-call detection and an explicit exception process.

## Acceptance criteria

- Every asynchronous route and mixed asynchronous service is included in a generated inventory.
- Known blocking calls are offloaded or carry a reviewed exception.
- A new unclassified blocking call fails the static validation.

## Proof plan

- Extend the contract checker from the hand-maintained mixed-file list to generated discovery.
- Retain QT-PROOF-306 as representative validation.
- Add regression fixtures for newly introduced unoffloaded blocking calls.

## Review boundary

API and runtime-services owners review detection scope and exceptions; this draft does not prescribe a specific offload mechanism.
