# Artifact Lifecycle & Discovery

Quant-Trad treats derived data as discovered structure.

---

## Artifact Types

Artifacts include:
- indicators
- regimes
- profiles
- signals
- overlays
- execution artifacts

All artifacts must declare when they become valid.

---

## Discovery Over Assumption

Artifacts:
- summarize observed behavior
- do not assume future state
- do not “snap” into existence retroactively

Derived structure must appear behind price, not in front of it.

---

## Visibility Rules

Artifacts may exist internally before they are visible,
but must not be exposed until their known-at condition is satisfied.

Visibility is part of correctness, not UI.
