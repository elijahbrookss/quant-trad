---
component: adr-canonical-execution-plan-order-semantics
subsystem: execution-runtime
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - runtime
  - execution
  - orders
  - fills
  - atm
  - maker-taker
code_paths:
  - src/atm/template.py
  - src/engines/bot_runtime/core/execution_plan.py
  - src/atm/schema.py
  - src/engines/bot_runtime/core/execution_order.py
  - src/engines/bot_runtime/core/domain/engine.py
  - src/engines/bot_runtime/core/domain/position.py
  - src/engines/bot_runtime/core/entry_execution.py
  - src/engines/bot_runtime/core/execution_adapter.py
  - portal/backend/service/strategies/strategy_service/facade.py
  - docs/architecture/execution-runtime/EXECUTION_RUNTIME_BOUNDARY.md
---
# ADR 0041: Use Canonical Execution Plans And Order Fill Semantics

## Status

Accepted on 2026-06-16.

## Context

Runtime execution behavior had accumulated in several places:

- ATM template normalization accepted multiple shapes but did not consistently
  convert them before runtime use.
- Entry orders and exit fills used different semantic models.
- Position code could still interpret raw stop-adjustment dictionaries.
- Limit-maker entries could evaluate against the full signal bar even though
  the signal is known at bar close.
- Some unsupported shapes, such as ATR trailing as a stop-adjustment action,
  were accepted but not implemented by runtime.

That made the execution layer harder to audit than the rest of the runtime.

## Decision

Runtime now has two canonical execution layers:

- `RuntimeExecutionPlan` compiles normalized ATM lifecycle config into typed
  entry, initial-stop, take-profit, fixed-horizon, breakeven, trailing, and
  stop-adjustment plans.
- `FillOrder` carries executable fill semantics: side, quantity, price,
  `order_type`, `liquidity_role`, `price_source`, and fee rate.

ATM schema version 2 has one accepted authoring shape: snake-case field names,
`take_profit_orders` with explicit stable IDs and `size_fraction` values,
`exit_plan.fixed_horizon`, mapping-valued `breakeven` and `trailing`, and
flattened `stop_adjustments`. Alternative spellings, wrapper objects, nested
trigger/action objects, implicit target allocation, and instrument economics
are rejected as unsupported fields. Target fractions must sum to one. Omitted
`stop_adjustments` normalize to an empty list; stop movement occurs only when
the template explicitly declares it.

The normalizer and compiler both enforce the schema so direct compiler callers
cannot bypass the admission contract. Position state receives resolved runtime
stop-adjustment objects, not raw template dictionaries. Invalid numbers,
unsupported enums, ambiguous target definitions, unresolved references, and
malformed rules fail loudly; no layer discards or weakens them.

Limit-maker entries are immediate signal-close submissions. Runtime may reject
them as post-only crosses at the signal price, but they cannot fill from the
already-known signal bar range. Once accepted, they rest into future bars for
their configured validity window.

Runtime supports only `signal_price` as the immediate limit-maker anchor.
Next-bar entry requires a dedicated pending signal-entry lifecycle and must not
be faked by a price anchor.

Strategy create/update and standalone ATM-template save operations normalize and
compile the template before persistence. Cross-target references and other
whole-plan constraints therefore fail at admission, not at run startup.

Stop-adjustment actions are limited to explicit one-time stop movement:
`move_to_breakeven` and `move_to_r`. ATR trailing must use top-level `trailing`
config until runtime implements it as a separate stop-adjustment action.
Target references must resolve to a configured take-profit ID before runtime
starts. For top-level R-activated trailing, `r_multiple` is the activation
threshold; `ticks` is a fixed trailing distance, while `atr_multiplier` selects
an ATR-derived distance. Runtime never selects between multiple raw
interpretations.

## Consequences

- Execution semantics are easier to trace from config to order to fill to
  wallet settlement.
- Strategy ATM policy and instrument execution economics have separate owners.
- Maker/taker and order-type fields are attached before fills reach adapters.
- Disabled trailing cannot activate from stale distance fields.
- Malformed targets and exit rules fail during normalization or plan
  compilation instead of silently becoming market entry, disabled policy, or
  omitted legs.
- Same-bar bar-resolution defaults are pessimistic unless a caller explicitly
  asks for target-first behavior.
