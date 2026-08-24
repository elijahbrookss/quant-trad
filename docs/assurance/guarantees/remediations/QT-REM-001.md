---
remediation_id: QT-REM-001
guarantee_ids: QT-GUAR-KNOWN-AT-PREFIX-INVARIANCE
lifecycle: proposed
owner: execution-runtime
required_reviewers: execution-runtime-owner,market-data-owner,platform-contract-reviewer
required_review: true
review_status: pending
---

# Remediation QT-REM-001

This proposed record does not change causal semantics or activate the indexed
guarantee. The named reviewers own the denominator and any later implementation
work.

## Gap

The frozen implementation has substantial known-at guards and representative
prefix/suffix tests, but no closed inventory proves that every causal input and
runtime output path uses the same availability boundary. `ARCH-COVERAGE-001`
and `CI-TRACE-001` therefore leave the repository-wide wording unproved.

## Action

After execution-runtime, market-data, and platform-contract review, define a
generated inventory or static dependency rule for all causal source and output
paths. Bind each admitted path to its known-at guard and extend deterministic
backtest and paper prefix/suffix cases for any uncovered family.

## Acceptance criteria

- The reviewed denominator enumerates every admitted causal source and output
  family without an unowned remainder.
- Every enumerated family links to an enforcement point and an exact proof
  selector.
- Backtest and paper comparisons reject future-known influence and preserve an
  already evaluated prefix after suffix extension.
- The platform-contract reviewer approves the claim scope before any
  classification or activation change.

## Proof plan

Run the generated inventory/static rule and the mapped credential-free
prefix/suffix pytest cases in the `python-nondb` environment. Retain the exact
source revision, discovered denominator, selectors, and results in a later
attestation; this record contains no proof result.

## Review boundary

Execution-runtime owner, market-data owner, and platform-contract reviewer.
