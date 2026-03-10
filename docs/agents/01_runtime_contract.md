# Runtime Contract

## Canonical Runtime Flow

All derived outputs should follow one timeline:

1. `initialize(context)`
2. `apply_bar(state, candle)`
3. `snapshot(state)`
4. consume snapshot payload

## Artifact Contract

Derived artifacts should include timing metadata:
- `known_at` (preferred)
- `created_at`
- `finalized_at`

Consumers must enforce timing gates before use/visibility.

## Cache Contract

Caching is valid only when it preserves runtime semantics:
- key includes semantic inputs
- outputs match non-cached replay
- timing gates are unchanged

## Single-Path Rule

Do not add alternate reconstruction paths for the same artifact class.
If required data is missing in snapshot payload, extend the snapshot contract.
