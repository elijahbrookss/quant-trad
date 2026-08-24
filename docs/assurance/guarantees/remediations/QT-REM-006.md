---
remediation_id: QT-REM-006
guarantee_ids: QT-GUAR-RUN-LIFECYCLE-LEDGER-AUTHORITY
lifecycle: proposed
owner: execution-persistence
required_reviewers: database-owner,execution-runtime-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-006

This proposal adds missing transaction proof without changing Bot Run lifecycle
semantics.

## Gap

The frozen repository and validators align with event-ledger lifecycle
ownership, but the strongest same-transaction, rollback, and concurrent
sequence claims lack isolated real-database proof. `SCHEMA-AUTH-001` remains
relevant to the enforcement surface.

## Action

After database and execution-runtime owner review, add disposable-database
cases for concurrent lifecycle appends, rejected invalid/backdated/post-terminal
transitions, same-transaction summary projection, and rollback after injected
projection failure.

## Acceptance criteria

- Concurrent accepted transitions receive a valid unique ledger sequence.
- Invalid, backdated, and post-terminal transitions leave ledger and summary
  state unchanged.
- An injected same-transaction projection failure rolls back both append and
  projection.
- Rebuild from the ledger reproduces the reviewed lifecycle summary.

## Proof plan

Run the repository tests against a clean disposable PostgreSQL/Timescale schema
under `python-db-isolated`, capture transaction and schema evidence, and bind
the results to a later commit-specific attestation.

## Review boundary

Database owner and execution-runtime owner.
