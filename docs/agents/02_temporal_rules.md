# Temporal & Walk-Forward Rules

Time is the primary constraint in Quant-Trad.

---

## Incremental Evaluation

All computations must be valid under step-by-step evaluation:
t0 → t1 → t2 → ...

Precomputation is allowed only if visibility is delayed
to preserve walk-forward correctness.

---

## Known-At Enforcement

Every derived artifact SHOULD expose one of:
- known_at
- created_at
- finalized_at

If known_at > current playback time, the artifact must not be visible or usable.

---

## No Retroactive Knowledge

The system must never:
- revise past indicator values
- reveal finalized structures early
- allow strategies to see future-derived artifacts

If unsure, default to stricter timing.
