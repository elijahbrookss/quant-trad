---
component: runtime-composition-root
subsystem: portal-runtime
layer: service
doc_type: architecture
status: active
tags:
  - composition
  - dependency-injection
  - runtime
  - portal
  - runtime-mode
code_paths:
  - portal/backend/service/bots/runtime_composition.py
  - portal/backend/service/bots/bot_service.py
  - portal/backend/service/bots/runtime_control_service.py
---

# Runtime Composition Root

## Purpose

`runtime_composition.py` is the explicit composition root for portal runtime services. It is the single place that wires runtime-facing collaborators used by bot service APIs.

## Responsibilities

- Build a `RuntimeComposition` dataclass with runtime collaborators and explicit `RuntimeMode`.
- Keep the service layer honest by wiring concrete collaborators in one place.
- Provide a singleton accessor for production (`get_runtime_composition`).
- Provide an override seam for tests (`set_runtime_composition_for_tests`).

## Runtime Modes

Supported composition modes:

- `backtest` (current default)
- `paper` (prepared seam; currently shares the same collaborator shape)
- `live` (prepared seam; currently shares the same collaborator shape)

Mode selection defaults from `BOT_RUNTIME_MODE` and can be overridden when calling `get_runtime_composition(mode=...)`.

## Collaborators

`RuntimeComposition` currently wires:

- `BotStreamManager`
- `BotConfigService`
- `BotRuntimeControlService`
- `BotStorageGateway` (storage boundary)
- `BotWatchdog`

Related worker-runtime boundary:
- `portal/backend/service/bots/runtime_dependencies.py` builds the explicit `BotRuntimeDeps` bundle used by `container_runtime.py`.

## Design Rules

- Composition-time choices belong in `runtime_composition.py`, not in leaf service modules.
- Runtime services should consume composed collaborators rather than deep-importing storage or bootstrapping concerns.
- Compatibility is preserved by keeping `bot_service.py` API surface stable while migrating internals to composition access.

## Migration Notes

- Existing callers of `portal.backend.service.bots.bot_service` remain valid.
- Runtime wiring for storage and watchdog is now explicit through composition.
- Mode-specific branches should be implemented in `build_paper_runtime_composition` and `build_live_runtime_composition` before introducing mode-specific business logic in service modules.
