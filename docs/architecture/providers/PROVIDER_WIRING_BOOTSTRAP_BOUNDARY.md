---
component: provider-wiring-bootstrap-boundary
subsystem: providers
layer: service
doc_type: architecture
status: active
tags:
  - providers
  - bootstrap
  - import-boundary
code_paths:
  - portal/backend/service/providers/__init__.py
  - portal/backend/service/providers/persistence_bootstrap.py
  - portal/backend/service/providers/provider_service.py
---

# Provider Wiring / Bootstrap Boundary

Provider modules must keep package imports side-effect free. Persistence bootstrap remains explicit and can be invoked via `ensure_provider_persistence_bootstrap()` where required.

## Rules

- No hidden persistence wiring in package `__init__` imports.
- Provider service modules may call bootstrap explicitly at composition/wiring boundaries.
- Optional provider dependencies should fail when used, not during unrelated import collection.
