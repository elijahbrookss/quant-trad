# Quant-Trad Agent Context

This file is the **entry point for all agents and contributors**.

It defines the expectations, principles, and engineering discipline required
to work safely inside the Quant-Trad codebase.

If behavior conflicts with this document or the docs under `docs/agents/`,
the code is wrong.

---

## Agent TL;DR (Read This First)

- QuantLab = research only
- Strategy = decision logic only
- Bot = execution + realism only
- All bot runs are walk-forward
- Derived artifacts must respect known-at timing
- Playback is a debugger, not a demo
- Fail loud; never swallow errors
- Prefer simple designs early; refactor when patterns are proven
- Abstractions belong in core components, not everywhere
- Prefer interfaces at real boundaries
- Duplicate logic is a refactor signal

> **Infrastructure Rule:** Only one DSN exists (`PG_DSN`). New persistence layers must use it directly—no additional DSN env vars or mapper layers.

---

## Canonical Context (Required Reading)

Agents MUST understand these documents before making architectural or behavioral changes:

- `docs/agents/00_core_principles.md`
- `docs/agents/01_layer_model.md`
- `docs/agents/02_temporal_rules.md`
- `docs/agents/03_artifact_lifecycle.md`
- `docs/agents/04_execution_and_playback.md`
- `docs/agents/05_engineering_principles.md`

These define the system contract.

---

## System Philosophy (Quant-Trad Specific)

Quant-Trad models markets as **incrementally discovered systems**.

Indicators, regimes, and profiles:
- summarize observed behavior
- do not predict or assume future state
- become known at specific points in time

Nothing “snaps into existence” retroactively.

If an artifact would not exist yet in live trading, it must not exist yet in the system.

---

## Logging Is Part of the Product

Logging is not optional and not cosmetic.

Logs must make it possible to trace:
QuantLab → Strategy → Bot → Trades → Playback

### Logging Principles
- Prefer structured logs (key=value or JSON)
- Log lifecycle boundaries, not noise
- One event = one log line with full context
- Never swallow errors to “keep things running”

### Debugging Guidance
- If the root cause isn’t clear, add targeted, temporary logs to observe state transitions—do not ship workarounds that mask the issue.
- Prefer stabilizing dependencies (refs, memoized callbacks) before adding logs; throttle diagnostics and remove them once the fix is in.

### Required Correlation Fields (when applicable)
Include these whenever they exist:
- `run_id`, `bot_id`, `bot_mode`
- `strategy_id`
- `indicator_id`, `indicator_type`, `indicator_version`
- `provider`, `venue`, `exchange`
- `symbol`, `timeframe`
- `trade_id`
- `bar_time` / `playback_time`

### Log Levels
- **DEBUG** — internal mechanics, cache behavior, counters
- **INFO** — lifecycle events and phase transitions
- **WARN** — unexpected but recoverable states (always explain why)
- **ERROR** — failed actions or invalid results (never swallowed)

If a fallback is used, it must emit a WARN explaining why.

---

## Error Handling Rules

- Do not swallow exceptions
- Do not silently skip invalid states
- Prefer failing early over producing incorrect output
- Errors must include context (IDs, symbol, timeframe, phase)

A system that hides errors cannot be trusted or improved.

---

## Engineering Discipline

### Prefer Simplicity Early
- Solve the current problem clearly
- Avoid speculative abstractions
- Refactor when duplication or pressure appears

### Abstractions Belong in Core
Use interfaces and abstractions when:
- multiple implementations already exist
- behavior varies by environment (providers, execution)
- testing requires substitution

Do not abstract leaf logic “just in case.”

### Prefer Interfaces at Boundaries
Good boundaries include:
- data providers
- execution adapters
- storage layers
- fee / margin models

Avoid switch statements in core services.
Use registries and explicit registration instead.

### Schema Expectations
- No runtime migrations or backfills live in the codebase.
- If a table is missing, create it once and log a WARN so operators know it was provisioned.
- If columns are missing, fail loud with an actionable error; do not attempt to patch or alter in-place.
- All schema changes must come from clean table definitions (drop/recreate out-of-band if needed).

---

## Refactor Signals

Refactor when:
- logic appears in 2+ places
- a class or function has multiple responsibilities
- adding a feature requires touching unrelated files
- conditionals become a pile of special cases

Do not refactor blindly.
Refactor with logs, tests, or concrete pressure.

---

## Non-Negotiable Rule

> If you must choose between convenience and correctness, choose correctness.

Quant-Trad is designed to be explainable first.
Performance, polish, and optimization come second.
