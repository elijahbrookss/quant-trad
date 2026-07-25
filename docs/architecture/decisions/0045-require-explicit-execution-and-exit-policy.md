---
component: adr-explicit-execution-exit-policy
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - execution
  - exits
  - stops
  - breakeven
  - fail-loudly
  - cleanup
code_paths:
  - src/atm
  - src/engines/bot_runtime/core/execution_plan.py
  - src/engines/bot_runtime/core/domain
  - src/engines/bot_runtime/core/entry_execution.py
  - docs/architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md
---
# ADR 0045: Require Explicit Execution And Exit Policy

## Status

Accepted on 2026-07-25.

**Retroactive cleanup ADR:** this records the strict execution boundary,
including removal of implicit move-to-breakeven behavior.

## Context

Permissive normalization and hidden defaults can turn malformed or omitted exit
rules into real trading behavior. An ATM strategy with no stop adjustments must
not begin moving stops because a runtime class supplied a convenient default.

## Decision

ATM schema v2 and `RuntimeExecutionPlan` are the only authoring and runtime
interpretations of entry and exit intent. Strategy and standalone-template
writes normalize and compile before persistence. Runtime consumes the immutable
compiled plan and resolved policy objects.

Omitted `stop_adjustments` means no stop adjustment. Breakeven and trailing
behavior are opt-in. Unknown fields, aliases, enums, references, same-bar
policies, liquidity roles, event types, reasons, non-finite values, and
contradictory rules fail before they can influence a fill.

## Invariants

- No execution or exit behavior is enabled by omission.
- A stop may tighten only in the favorable direction and never loosen.
- Same-bar stop/target conflicts use the declared canonical policy; unsupported
  policy values raise.
- Targets have stable IDs and explicit allocations totaling one.
- Target, stop, fixed-horizon, and terminal exits carry explicit event type,
  reason, price source, and liquidity role.
- Instrument economics come from the execution profile, never from ATM policy.

## Consequences

Older permissive templates may be rejected and must be corrected rather than
silently adapted. Config admission is stricter, but production behavior is
inspectable and a missing field cannot activate a hidden risk rule.

## Rejected Alternatives

- Preserve legacy ATM shapes through compatibility normalization.
- Insert a default move-to-breakeven rule.
- Ignore malformed stop adjustments or unresolved target references.
- Choose optimistic same-bar behavior when data is ambiguous.
- Let reports reinterpret exit reasons after execution.

## Enforcing Tests Or Evidence

- `tests/integration/runtime/test_execution_semantics.py` covers strict enums,
  long/short same-bar resolution, pessimistic fallback, repeatability, and the
  disabled domain-level breakeven default.
- `tests/integration/runtime/test_bot_runtime_entry_execution.py` covers
  explicit stop adjustment, omitted-adjustment behavior, maker timing,
  post-only rejection, trailing monotonicity, fees, and target allocation.
- Cleanup commits `66aac0b` and `c5d3c76` remove implicit breakeven defaults.
- Cleanup commit `484ad56` makes malformed execution invariants fail loudly.

## References

- [ADR 0040: Runtime Exit Plans And Liquidity Roles](0040-use-runtime-exit-plans-and-liquidity-roles.md)
- [ADR 0041: Canonical Execution Plans And Order Fill Semantics](0041-use-canonical-execution-plan-and-order-fill-semantics.md)
- [Execution Runtime Boundary](../execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md)
