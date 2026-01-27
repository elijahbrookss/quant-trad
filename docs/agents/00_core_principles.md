# Quant-Trad Core Principles

These principles define how the Quant-Trad engine is built and how agents must reason
when extending or modifying the system.

If code violates these principles, the implementation is wrong — even if it “works.”

---

## 1. Walk-Forward Is the Default Reality

Quant-Trad behaves as if it is trading live at all times.

- Candles arrive sequentially
- Indicators update incrementally
- Signals fire only when conditions are met
- Trades evolve candle by candle

Backtests are simulated live runs, not historical replays.

If an artifact would not exist yet in live trading, it must not exist yet in the system.

---

## 2. Everything Has a Known-At Moment

Derived data does not exist immediately.

Indicators, regimes, profiles, signals, and overlays:
- are discovered over time
- become *known* at a specific candle
- may only be used after that moment

If something appears before it could have been known, the system is leaking future data.

---

## 3. Correctness Beats Convenience

If there is a trade-off between:
- realism vs speed
- correctness vs simplicity
- clarity vs cleverness

Correctness always wins.

Shortcuts that violate correctness are bugs, not optimizations.

---

## 4. Determinism Is Required

Given:
- the same candles
- the same parameters
- the same versions

The system must produce the same results.

If behavior changes without a version change, the system is no longer explainable —
and therefore not tradable.

---

## 5. Framework First, Strategy Second

Quant-Trad is a framework before it is a collection of strategies.

Core systems must:
- be indicator-agnostic
- be strategy-agnostic
- support future extensions without rewrites

Strategies adapt to the framework — not the other way around.
