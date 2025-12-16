# Walk-Forward Execution Rules

This document defines the walk-forward constraints used across the system.

---

## Definition: Walk-Forward

At time T, the system may ONLY use information that would have been available at time T in live trading.

This applies to:
- Candles
- Indicators
- Indicator signals
- Merges
- Derived features
- Overlays

---

## Forbidden Patterns

Agents MUST NOT:
- Precompute indicators on full history and replay results
- Render overlays that rely on future candles
- Allow strategies or bots to see finalized structures before they would exist live

Any shortcut that uses future data is data-snooping.

---

## Required Pattern

Indicators and features must be:
- Evaluated incrementally
- Cached per step
- Revealed only when valid

Preferred design:
- Step-wise evaluation (`t0 → t1 → t2`)
- Explicit timestamps for when artifacts become known

---

## Known-At-Time Rule

Every derived artifact SHOULD have one of:
- `created_at`
- `known_at`
- `finalized_at`

Bot playback must respect these timestamps.

If `known_at > playback_time`, the artifact MUST NOT be visible or usable.

---

## Strategy vs Bot
- Strategy preview MAY shortcut execution realism
- Bot execution MUST be strictly walk-forward

If unsure, default to strict walk-forward.
