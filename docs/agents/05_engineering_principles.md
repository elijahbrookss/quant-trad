# Software Engineering Principles (Quant-Trad)

These principles govern how code should be written inside the Quant-Trad codebase.

---

## 1. Fail Loud, Not Silent

Errors must:
- be logged
- propagate meaningfully
- never be swallowed “to keep things running”

A system that hides errors is untestable and untrustworthy.

---

## 2. Prefer Simple, Composable Designs

Early implementations should:
- be readable
- be testable
- solve the immediate problem

Avoid premature generalization.
Refactor when patterns are proven.

---

## 3. Abstractions Belong in Core Components

Abstractions are valuable only when:
- multiple implementations already exist
- the boundary is stable
- the abstraction simplifies usage

Core components (providers, indicators, execution adapters) may use interfaces.
Leaf logic should not.

---

## 4. Prefer Interfaces at Boundaries

Use interfaces where:
- implementations may vary (providers, execution, storage)
- testing requires substitution
- behavior differs by environment

Do not introduce interfaces for hypothetical futures.

---

## 5. Explicit Is Better Than Clever

Code should make:
- data flow obvious
- timing obvious
- dependencies obvious

If behavior is surprising, it is wrong.

---

## 6. Instrumentation Is Part of the Feature

Logging, metrics, and traceability are not optional.

If behavior cannot be observed, it cannot be trusted.
