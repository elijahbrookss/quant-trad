# Runtime Boundary Drift and Hidden Dependencies (2026-03-16)

## Incident

- Scope: runtime, strategy-series preparation, indicator access, storage/reporting integration, package import surfaces
- Symptom: the codebase felt over-abstracted and still tightly coupled at the same time
- User-facing effect: editing the runtime required tracing lazy imports, compatibility wrappers, and deep cross-layer imports before it was clear what was authoritative
- Engineering effect: the architecture docs said one thing, but the import graph said another

This was not a production outage. It was an architecture incident.

The important signal was developer discomfort:

- dependencies felt hidden instead of explicit
- package boundaries felt performative instead of real
- the runtime looked canonical in docs but still depended on portal service modules
- compatibility paths stayed around long enough to become part of the mental model

That discomfort was correct.

## What We Observed

### 1. The documented canonical runtime was not actually isolated

The documented canonical runtime lives under `src/engines/bot_runtime/`.

In practice, engine/runtime modules still imported portal service code for:

- storage writes
- report persistence
- candle fetching
- indicator service access
- runtime reporting helpers
- bot runtime strategy-series construction

This meant the dependency direction was inverted:

- desired: `portal -> engine`
- actual in many places: `engine -> portal`

### 2. Compatibility wrappers created a second runtime home

Compatibility wrappers under `portal/backend/service/bots/bot_runtime/runtime/` were intended as temporary re-exports.

Instead, they preserved a second namespace for the same runtime concepts:

- readers had to know both paths
- imports could continue to drift
- “canonical” became documentation-only instead of code-enforced

### 3. Lazy package facades hid ownership

Several package `__init__` modules used `__getattr__`, function proxies, or lazy module forwarding.

These existed for real reasons:

- import resilience
- optional dependency handling
- backward compatibility
- test monkeypatching

But the cost was high:

- static ownership became hard to see
- grep-based discovery became less reliable
- imports looked cheap but were not conceptually cheap
- package boundaries started behaving like service locators

### 4. Some modules became too large to preserve clear seams

Large runtime modules accumulated orchestration, boundary calls, and operational concerns together.

Notable examples:

- `src/engines/bot_runtime/runtime/mixins/setup_prepare.py`
- `src/engines/bot_runtime/runtime/mixins/runtime_events.py`
- `portal/backend/service/bots/bot_runtime/strategy/series_builder_parts/series_construction.py`
- `portal/backend/service/indicators/indicator_service/signals.py`

This made it harder to separate domain logic from wiring logic.

### 5. Abstractions were added without fully enforcing the boundary they implied

The codebase had:

- a composition root
- wrappers
- facades
- compatibility shims
- runtime/service splits

But the underlying imports still crossed layers freely.

That combination created the worst version of abstraction:

- more files
- more indirection
- no stronger guarantee

## Root Cause

This incident came from three compounding decisions.

### 1. We optimized for migration convenience over architectural truth

Compatibility wrappers and lazy imports were introduced to keep the code moving without breaking callers.

That was reasonable short-term.

The mistake was allowing temporary seams to become long-lived structure.

### 2. We created abstract boundaries before enforcing dependency direction

The code described engine/service boundaries, but the import graph still allowed the engine to reach upward into portal services.

A boundary that does not constrain dependency direction is not a real boundary.

### 3. We treated many internal relationships as if they needed DI

Not every dependency is a boundary.

Real boundaries in this repo are things like:

- database-backed strategy loading
- candle/data-provider access
- storage persistence
- report persistence
- provider/execution adapters

Internal runtime collaboration does not need a DI framework or lazy package indirection.

It needs explicit ownership and ordinary imports inside the owning subsystem.

## What Was Actually Wrong

The issue was not “too little abstraction” or “too much documentation.”

The issue was:

- hidden dependency direction
- duplicate authoritative paths
- adapters living longer than migrations
- package-level indirection masking coupling

In short:

> We built abstractions around the edges of the problem without simplifying the center.

## Architectural Decision

### Decision 1: keep one canonical runtime

`src/engines/bot_runtime/` is the canonical bot runtime implementation.

If that remains true, then:

- runtime code under `src/engines/bot_runtime/` must not import `portal.*`
- portal code may compose and call the runtime
- compatibility wrappers must be treated as temporary and deleted after migration

If that cannot be enforced, then the docs are wrong and should say so plainly.

The preferred decision is to enforce it.

### Decision 2: compose real boundaries explicitly, not everything

Use a small explicit dependency bundle for the runtime.

For example:

- strategy source
- candle source
- indicator runtime access
- runtime event sink
- trade/report persistence

This should be a plain dataclass or small set of narrow collaborators.

It should not become a framework.

### Decision 3: keep runtime-domain behavior with the runtime

Not everything currently under `portal/backend/service/bots/bot_runtime/strategy/` belongs in portal.

A useful rule:

- DB reads and application composition can live in portal
- runtime-domain construction of `StrategySeries`, runtime execution state, and series execution behavior belongs with the runtime domain

This avoids moving runtime behavior into “adapter” folders just because the current path is messy.

### Decision 4: reduce package magic

Package-level lazy exports should be the exception, not the default.

Prefer:

- explicit `api.py` or `facade.py` modules where needed
- direct module imports from concrete files
- minimal compatibility shims during migrations only

Avoid package namespaces that dynamically forward half the subsystem.

## Target Shape

This is the intended simplified shape.

```text
src/
  engines/
    bot_runtime/
      core/
      runtime/
      strategy/
      deps.py
  indicators/
  signals/
  data_providers/
  strategies/
  risk/
  atm/
  utils/

portal/
  backend/
    controller/
    service/
      bots/
        runtime_composition.py
        runtime_control_service.py
        bot_service.py
      storage/
      reports/
      market/
      providers/
      indicators/
      strategies/
```

### Notes on this target shape

- `src/engines/bot_runtime/strategy/` should own runtime-domain series preparation if that logic is truly part of runtime execution semantics.
- Portal should provide concrete collaborators to the runtime, not become the home of the runtime.
- Do not create a large adapter tree unless pressure appears. Start with a small dependency bundle and a few concrete collaborators in the composition root.

## What To Remove

The simplification is not just moving files. It is deleting unnecessary indirection.

### Remove or shrink

- compatibility wrappers under `portal/backend/service/bots/bot_runtime/runtime/`
- broad package-level `__getattr__` export indirection where direct imports are acceptable
- runtime code that deep-imports portal service modules

### Keep

- the portal composition root
- explicit app/service modules that own HTTP, DB, worker, and operational wiring
- import-safe seams only where optional dependencies or test ergonomics genuinely require them

## Concrete Migration Order

### Phase 1: freeze the truth

1. Declare `src/engines/bot_runtime/` the only canonical runtime.
2. Stop adding new imports to `portal/backend/service/bots/bot_runtime/runtime/`.
3. Treat wrapper modules as migration-only.

### Phase 2: introduce a small runtime dependency bundle

Add a small runtime dependency contract for the real boundaries:

- strategy loading
- candle fetching
- indicator/runtime lookup
- runtime event persistence
- trade/report persistence

Pass that bundle from the portal composition layer into runtime construction.

Do not inject internal helpers that are owned entirely by the runtime package.

### Phase 3: remove upward imports from engine to portal

Replace direct imports from `portal.backend.service.*` inside `src/engines/bot_runtime/*` with calls through the dependency bundle.

This is the first major simplification step because it makes the boundary honest.

### Phase 4: move runtime-domain series logic to the runtime package

Re-home runtime-domain `StrategySeries` construction if needed so that:

- portal remains the source of application composition and DB-backed adapters
- runtime remains the owner of execution-time domain preparation

Keep the database-backed strategy loader in portal if it is fundamentally an application/storage adapter.

### Phase 5: delete compatibility wrappers

After imports are migrated:

- remove `portal/backend/service/bots/bot_runtime/runtime/*` wrappers
- update docs to remove wrapper references
- make the canonical path visible in tests and code review

### Phase 6: simplify lazy facades

Reduce package magic one subsystem at a time:

- indicators
- strategies
- bot runtime

Prefer explicit module imports and small public entry modules.

Keep import resilience only where there is a proven need.

### Phase 7: clean packaging metadata

Update packaging metadata so the installable package list matches the actual import surface.

This is cleanup, not the primary fix.

## Guardrails For Future Work

### Rule 1

`src/engines/*` must not import `portal.*`.

### Rule 2

Temporary compatibility wrappers must have an explicit deletion plan.

### Rule 3

Do not add lazy package exports to avoid making a dependency decision.

### Rule 4

Use interfaces only at real boundaries.

If a dependency is:

- an external system
- environment-specific behavior
- storage/provider/execution integration
- something tests genuinely need to substitute

then a seam is justified.

If it is just internal collaboration inside the owning subsystem, prefer ordinary imports and explicit module ownership.

### Rule 5

If docs say “canonical,” the import graph must enforce it.

## What We Learned

- Hidden dependencies feel bad because they are bad.
- Compatibility layers are useful only if they disappear.
- Large files are often a symptom of mixed ownership, not just a need to split methods.
- Clean architecture in this codebase does not mean more abstraction.
- Clean architecture here means fewer false seams and clearer dependency direction.

## Follow-Up Work

1. Add import-boundary tests that fail if `src/engines/*` imports `portal.*`.
2. Create the small runtime dependency bundle and migrate one engine module at a time.
3. Remove runtime compatibility wrappers after callers are updated.
4. Reduce package-level lazy exports to explicit public modules.
5. Update runtime architecture docs after the migration is real, not before.

## Final Summary

This incident was a case of architectural drift hidden behind compatibility and indirection.

The fix is not to add more abstraction.

The fix is to:

- keep one canonical runtime
- enforce dependency direction
- inject only real boundaries
- delete migration scaffolding when the migration is done
- prefer simple, explicit module ownership over clever package behavior
