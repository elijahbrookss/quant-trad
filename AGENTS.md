# Agent Guidelines (Hub)

## Agent TL;DR (Do Not Skip)

- QuantLab = research only
- Strategy = decision logic only
- Bot = execution + realism only
- All backtests are walk-forward
- Indicators and overlays must respect known_at timing
- Market Profile must NEVER appear in front of price
- Logs must tell the story of the userflow
- Duplicate logic = refactor signal
- Avoid indicator-specific logic in engine/runtime; use registries or adapters.
- AVOID ADDING FALLBACKS / DEFAULTS


This repository uses a distributed agent-context model.
Agents MUST read the documents listed below before making architectural or behavioral changes.

## Required Context (Read in Order)
- docs/agents/00_userflow.md
- docs/agents/01_walk_forward_rules.md
- docs/agents/02_market_profile_no_snooping.md
- docs/agents/03_botlens_playback.md

If you violate these constraints you will create incorrect simulations, data-snooping bias, or broken UX guarantees.

---

## Fallbacks

Ideally we want no fallbacks. but if you're unsure prompt the user

## Logging Standards (Readability + Traceability > Volume)

Logging is not optional. Logs should tell the story of a user session and make it possible to trace:
QuantLab → Strategy → Bot → Trades → Playback.

### Principles
- Prefer structured logs (key=value or JSON) with consistent keys.
- Log boundaries and phase transitions, not noise.
- Every major action should be traceable by IDs and correlation fields.
- Avoid duplicate log lines: one event, one log statement, with full context.

### Required Correlation Fields (include when applicable)
Always include these when they exist:
- `request_id` (or equivalent) and/or `session_id`
- `provider`, `venue`, `exchange`
- `symbol`, `timeframe`
- `indicator_id`, `indicator_type`, `indicator_version`
- `strategy_id`
- `bot_id`, `bot_mode` (backtest/paper/live)
- `run_id` (bot run / backtest run identifier)
- `trade_id` (during execution)
- `playback_time` / `bar_time` (walk-forward step timestamp)

### Log Levels (use consistently)
- `DEBUG`: internal mechanics that help diagnose (caches, computed counts, branching)
- `INFO`: lifecycle events and state transitions (start/end, created/attached, run summaries)
- `WARN`: unexpected states but recovery is possible (missing optional data, fallback path used)
- `ERROR`: action failed or results invalid; include exception + context; never swallow

### Required Lifecycle Events (minimum)
Log at INFO for:
- QuantLab:
  - candle fetch start/end (rows, date range)
  - indicator create/update/delete
  - indicator overlay render/build start/end
  - signal generation start/end (counts)
- Strategy:
  - strategy create/update
  - indicator attach/detach to strategy
  - strategy signal preview start/end (counts)
- Bot:
  - run start (mode, strategy_id, symbol/timeframe set, config hash/version)
  - per-phase transitions (load data, compute indicators, generate signals, execute trades)
  - run end summary (trades, pnl, fees, drawdown, duration)
- Execution:
  - trade open (entry, stop, targets, size, rationale tags)
  - stop adjustment events (what changed + why)
  - trade close (reason, realized pnl, fees)
- Walk-forward:
  - step progress sampling (not every bar unless debugging): include `bar_time`, step index, state

### Anti-Patterns (do not do these)
- Logging without IDs (untraceable)
- Logging giant payloads (entire DataFrames, full candle arrays)
- Repeating the same message every bar at INFO
- “silent fallback” logic without WARN + explanation
- “works on my machine” logs that omit provider/symbol/timeframe

### Implementation Guidance
- Create a small logging helper / context builder used everywhere.
- Prefer one-liners with structured context over multi-line narrative spam.
- When debugging complex indicators (e.g. Market Profile), add focused DEBUG counters:
  - `profiles_total`, `profiles_finalized`, `profiles_visible_now`, `merge_candidates`, etc.

---

## Commenting Standards (Explain Why, Not What)

Comments exist to preserve intent and prevent future regressions.

### Write comments for:
- Walk-forward constraints (“why we delay visibility”, “known_at semantics”)
- Non-obvious merges/heuristics (MPF merge criteria, overlap thresholds)
- Anything that looks like a shortcut but is required for correctness
- Public APIs / stable interfaces (what must remain backward compatible)

### Avoid comments that:
- Restate the code line-by-line
- Explain obvious Python/JS syntax
- Drift from reality (if behavior changes, update/remove the comment)

### Preferred format
- Use docstrings for module/class/function intent and invariants.
- Use short inline comments for sharp constraints:
  - `# MUST be known_at <= t to avoid data snooping`

---

## Refactor Discipline (Avoid Spaghetti, Optimize for Iteration)

You do not need “perfect code.” You DO need code that stays easy to extend without fear.

### Refactor when:
- The same logic appears in 2+ places (especially indicator build paths)
- A function/class grows beyond a single responsibility
- A feature requires touching 3+ unrelated files to implement one change
- Conditional logic becomes a pile of special cases
- You find “one-off” code that should be a reusable pattern (providers, indicators, signals)

### Don’t refactor when:
- You’re guessing the abstraction without real usage pressure
- It would change core behavior without tests/log proof
- It delays shipping a small fix (but leave a TODO with a ticket/issue)

### Refactor goals
- Remove duplication first (shared helpers / shared interfaces)
- Make flows explicit (QuantLab vs Strategy vs Bot)
- Keep “walk-forward correctness” centralized and hard to bypass
- Preserve readability: optimize for the next developer reading logs + code

---

## Avoid Duplicate Code: Use Registries + Decorators

Prefer explicit registration over scattered imports and hardcoded maps.

### Patterns to use
- Decorator-based registration for intentionally modular components:
  - Indicators
  - Signal generators
  - Data providers
  - Execution models / fee models (if applicable)

### Rules
- If adding a new indicator/provider requires editing multiple switch statements, refactor.
- New modules should be discoverable by registration + metadata, not hidden wiring.

### Metadata expectations (per registered module)
- stable name/type
- version
- supported timeframes (if relevant)
- required inputs (columns/features)
- determinism / walk-forward constraints (if special)

---

## Non-Negotiable: Walk-Forward Integrity
- Never show or use indicator artifacts before they are valid (`known_at` semantics).
- Market Profile must follow “no brand new MPF in front of price.”
- If unsure, default to strict incremental computation and delayed visibility.

If you need to choose between convenience and correctness: choose correctness.
