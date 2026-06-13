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

Framework-style surfaces must be contract-driven, not domain-shaped. A generic
surface may render, rank, compare, route, or validate facts only through fields
declared by the producing contract. It must not import indicator, strategy, bot,
or report-family knowledge to make a generic workflow look smarter.

If a reusable surface needs ordering or interpretation, that intent must be
explicit in the request or emitted contract. Missing rank keys, metric
directions, grouping fields, or required semantics are contract errors, not
places to guess.

Hidden fallbacks are not allowed. A second path is valid only when it is an
explicit contract branch with clear inputs, outputs, and operator-visible
context.

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
