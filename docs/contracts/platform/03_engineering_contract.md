# Engineering Contract

## Failure Semantics

- Fail loud with actionable context.
- Include IDs, symbol/timeframe, and phase.
- Do not hide invalid state transitions.

## Boundary Design

Use interfaces at real boundaries:
- providers
- storage
- execution adapters

Keep leaf logic explicit and simple.

## Schema Semantics

- No runtime migrations/backfills in app paths.
- Missing table: provision once with operator-visible warning.
- Missing columns: fail loud with actionable error.

## Observability Contract

Lifecycle boundaries should be observable via structured logs.
Correlation fields should include IDs and timing context when available.

## Optimization Rule

Preserve correctness and determinism first.
Performance work is valid when semantics remain unchanged.
