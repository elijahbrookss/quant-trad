---
remediation_id: QT-REM-400
guarantee_ids: QT-GUAR-CANONICAL-MARKET-IDENTITY-ROUTING
lifecycle: proposed
owner: market-identity
required_reviewers: data-owner,decision-layer-owner,runtime-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-400

**Concrete assurance closure plan**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The frozen paths show canonical link preference and top-level API rejection, but nested slot aliases remain accepted as lookup inputs and no closed route inventory proves that aliases cannot become authority elsewhere.

## Action

After identity-owner review, generate the complete collection, strategy, and runtime routing inventory and classify every alias-bearing input as a lookup hint, compatibility field, or prohibited authority.

## Acceptance criteria

- Every collection and runtime market-data route names its canonical instrument, source, and series identity.
- Every strategy input that accepts provider or venue aliases resolves them to a canonical record before use.
- No stale snapshot or alias can override a linked canonical instrument.
- The data, decision-layer, and runtime owners approve the route denominator and compatibility boundary.

## Proof plan

Required proof definitions: `QT-PROOF-400`.

Required environment profile: `python-nondb`.

Run after the reviewed routing inventory is bound to a clean commit; this proof definition is not a result.

## Review boundary

The review may clarify compatibility input scope but cannot adopt QT-TERM-027 through QT-TERM-029, change product identity semantics, or activate the guarantee.
