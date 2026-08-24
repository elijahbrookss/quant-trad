---
remediation_id: QT-REM-003
guarantee_ids: QT-GUAR-PROVIDER-FREE-CANONICAL-READS
lifecycle: proposed
owner: market-data
required_reviewers: market-data-owner,provider-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-003

This proposal narrows a proof-coverage gap. It does not redefine which reads are
canonical or change acquisition behavior.

## Gap

Provider traps cover representative feed and structured-Fact reads, but no
closed registry enumerates every canonical read entrypoint. The current
evidence therefore cannot support a repository-wide provider-free assertion;
`ARCH-COVERAGE-001` remains open.

## Action

After market-data and provider-owner review, generate an inventory of canonical
read entrypoints and bind each entry to a provider trap or an equivalent static
dependency check. Keep explicit acquisition workflows outside that denominator
and document the ownership rule used to make that distinction.

## Acceptance criteria

- The reviewed inventory contains every canonical read entrypoint and no
  acquisition operation.
- Each entrypoint has an exact provider-trap or static-boundary proof mapping.
- Missing stored data never causes an inventoried read to invoke provider
  transport or synthesize provider-backed truth.
- Any dynamic registration point has an explicit attestation rule rather than
  being silently treated as a closed set.

## Proof plan

Run the generated inventory and credential-free provider traps in
`python-nondb`, plus the structured-Fact path in `python-db-isolated`. Retain
the discovered entrypoint set and results in a later exact-source attestation.

## Review boundary

Market-data owner and provider owner.
