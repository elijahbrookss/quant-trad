---
remediation_id: QT-REM-305
guarantee_ids: QT-GUAR-SOLE-POSTGRES-PERSISTENCE-AUTHORITY
lifecycle: proposed
owner: persistence
required_reviewers: database-operations-owner,persistence-owner
required_review: true
review_status: pending
---

# Remediation QT-REM-305

**Close PostgreSQL authority and drift integration proof**

This record is a proposed remediation plan. It changes no product or normative
semantics, supplies no proof result, and activates no guarantee.

## Gap

The current proof strongly checks bootstrap structure and drift rejection through mocks but does not exercise the complete migration and schema-drift matrix against an isolated PostgreSQL instance.

## Action

Add a reviewed isolated-database proof for PG_DSN authority, clean bootstrap, supported migration, and representative incompatible schema drift.

## Acceptance criteria

- Clean bootstrap and supported migration complete only through PG_DSN.
- Representative table, column, constraint, and index drift fail before runtime repair.
- Unset or invalid PG_DSN fails only when database use begins and no alternate PostgreSQL authority is selected.

## Proof plan

- Add isolated PostgreSQL selectors for clean bootstrap and drift mutations.
- Inventory PostgreSQL engine construction sites for alternate authorities.
- Retain QT-PROOF-305 as structural prerequisite coverage.

## Review boundary

Persistence and database-operations owners review supported migration and drift cases; AGENTS.md remains nonactivating governance evidence.
