# Component Documentation Standard

This file defines the writing standard for component architecture docs in Quant-Trad.

Audience:
- Intermediate engineers.

Goal:
- Understand the design quickly.
- Understand why the design exists.
- Understand tradeoffs and risks.
- Understand the strict behavior contract.

---

## Principles

- State shipped behavior, not aspirational behavior.
- Declare one source of truth for each state domain.
- Name the causality/order key that determines correct processing order; timestamps are not ordering keys unless explicitly stated.
- Define boundaries so ownership and responsibility are explicit.
- Treat failure behavior as part of the design, not an exception.
- Write contracts that are executable and validatable in code, logs, storage, or metrics.
- Maintain a clear reader path: the document reads top-to-bottom like a short chapter.

---

## Required Header Block

Every component architecture doc must start with:

- `Component`: component name
- `Owner/Domain`: owning team or domain area (optional if unknown)
- `Doc Version`: incrementing doc version
- `Related Contracts`: links/paths to governing contracts, schemas, and interfaces (must include at least one, or explicitly `none`)

---

## Required Structure

Each component architecture doc should include these sections in order:

1. Problem and scope
- What system problem this component solves.
- What is in scope vs out of scope.
- Include a `Non-goals` subsection that explicitly lists what is not guaranteed and what is assumed upstream.

2. Architecture at a glance
- One diagram (flowchart or sequence) that explains the core flow.

Optional section: Mentor Notes (Non-Normative)
- Typical length: 5-10 lines.
- Optional mental models, intuition, or brief analogies for reader comprehension.
- Use 1-3 mental models maximum.
- MUST NOT introduce new guarantees.
- Normative guarantees belong in `9. Strict contract`.
- If this conflicts with Strict contract, Strict contract wins.

3. Inputs, outputs, and side effects
- Inputs: triggers, events, schedules, API calls.
- Dependencies: required upstream contracts and guarantees.
- Outputs: state changes, emitted events, persisted rows, and telemetry.
- Side effects: external calls, ledger writes, network I/O.

4. Core components and data flow
- Main parts and how data/control moves between them.
- Include important IDs/cursors/timestamps used for correctness.

5. State model
- Distinguish authoritative state vs derived state.
- Define persistence boundaries (what is persisted where, and what is in-memory only).

6. Why this architecture
- Why this shape was chosen.
- What alternatives were considered and why they were not chosen.

7. Tradeoffs
- Costs accepted by the chosen design.
- Performance/complexity/operational implications.

8. Risks accepted
- Known risks that still exist.
- Mitigations and operational guardrails.

9. Strict contract
- Non-negotiable invariants and interface-level guarantees.
- Failure behavior (fail loud vs degrade).
- Retry and idempotency semantics are required: exactly-once, or at-least-once with idempotency keys, or explicitly `not guaranteed`.
- If degrade modes exist, include a small state machine (for example `RUNNING -> DEGRADED -> HALTED`) and define in-flight work behavior per state.
- Document sim vs live differences, or explicitly state `no differences`.
- If the component emits errors, define canonical error codes/reasons.

10. Versioning and compatibility
- Schema/version rules.
- Compatibility expectations and migration behavior.

---

## Writing Rules

- Write in present tense about actual behavior, not aspirational behavior.
- Use clear, direct language; avoid low-value implementation detail dumps.
- Prefer explaining semantics over listing classes/functions.
- Include exact field names where correctness depends on them.
- When behavior is not guaranteed, label it explicitly as a risk or limitation.

### Repetition budget

- Normative guarantees belong in `9. Strict contract` once.
- Other sections reference `9. Strict contract` instead of re-stating guarantees.
- Repetition is allowed only for a short summary table/list at the end when needed.

### Optional module: Worked Example (Non-Normative)

- Include one small scenario using real field names (for example cursor/order/failure flow) when invariants are non-trivial.
- Keep it brief and explanatory.
- MUST NOT add or modify guarantees; normative guarantees remain in `9. Strict contract`.

### Allowed patterns

- `Claim -> because -> consequence`
- `Guaranteed` / `Not guaranteed` / `Risk` labeling in adjacent lines.
- Boundary sentence pattern: `Inside boundary: ...` / `Outside boundary: ...`.
- Ordering sentence pattern: `Order key is <field>; processing follows this key.`

### Disallowed outcomes

- MUST NOT contain recommendation prose as requirements.
- MUST NOT contain future-tense guarantees for behavior that is not shipped.
- MUST NOT force readers to infer authority, ordering, or failure semantics.
- MUST NOT introduce normative guarantees outside `9. Strict contract`.

### Micro-example

- Bad: `We should retry this call and maybe dedupe later.`
- Good: `Delivery is at-least-once; duplicates are ignored by event_id. This keeps retries safe under reconnect.`

---

## Diagram Rules

- Include at least one diagram in mermaid.
- Keep diagrams small enough to understand in under 30 seconds.
- Use canonical names from code/contracts (for example `run_id`, `seq`, `known_at`).
- Show a clear component boundary (inside vs outside). If not shown in diagram, state boundary explicitly in text.

---

## Contract Rules

- The contract section is normative.
- If implementation and contract conflict, either update implementation or narrow/clarify the contract to match shipped behavior.
- Do not leave ambiguous guarantees.
- Each invariant must include a validation hook when applicable (test, log field, storage constraint, or metric/alert).

---

## Validation Checklist

Before finishing a component doc:

1. Contains "Why this architecture", "Tradeoffs", and "Risks accepted".
2. Contains at least one diagram.
3. Contains "Inputs, outputs, and side effects".
4. Contains "State model".
5. Contains a strict contract section including retry/idempotency semantics and sim-vs-live note.
6. Uses real field/cursor names from implementation.
7. Does not contain recommendation prose.
8. If Mentor Notes exist, they introduce zero new guarantees and defer normative claims to `9. Strict contract`.
9. Repetition budget is respected (normative guarantees are not duplicated across multiple sections).
