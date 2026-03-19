---
component: centralized-configuration-boundary
subsystem: portal-runtime
layer: service
doc_type: architecture
status: active
tags:
  - configuration
  - runtime
  - yaml
  - environment
  - bootstrap
code_paths:
  - src/core/settings.py
  - config/defaults.yaml
  - config/dev.yaml
  - config/prod.yaml
  - portal/backend/main.py
  - portal/backend/run_backend.py
  - portal/backend/service/bots/runner.py
  - portal/backend/service/bots/container_runtime.py
  - portal/frontend/src/config/appConfig.js
---

# Centralized Configuration Boundary

## Purpose

Quant-Trad configuration now follows one boundary:

- grouped non-secret defaults live in YAML under `config/`
- deploy-specific overrides and secrets come from canonical environment variables
- application code reads typed settings from `src/core/settings.py`

This replaces scattered `os.getenv(...)` lookups and duplicated fallback values across services.

## Rules

- `src/core/settings.py` is the only application configuration loader.
- New application code must not read environment variables directly outside process-boundary code such as supervisor env copying or container launch env assembly.
- Defaults belong in YAML, not repeated in leaf modules.
- `PG_DSN` remains the only canonical non-prefixed env variable.
- Canonical application env names use single underscores, for example `QT_BOT_RUNTIME_IMAGE`.
- Frontend build-time env access is centralized in `portal/frontend/src/config/appConfig.js`.

## Precedence

Configuration resolves in this order:

1. `config/defaults.yaml`
2. profile overlay from `config/<profile>.yaml`
3. optional file from `QT_CONFIG_FILE`
4. canonical environment overrides

`QT_CONFIG_PROFILE` selects the profile. If unset, the default profile is `dev`.

## Design Intent

- Keep settings discoverable and grouped by concern.
- Preserve container and deployment ergonomics through env overrides.
- Make runtime behavior testable by loading one typed settings object at startup.
- Allow UI and service catalogs to render from the same source of truth instead of re-declaring defaults.

## Migration Guidance

- When adding a new setting, first add it to the YAML/default schema, then bind the canonical env name in `src/core/settings.py`.
- Prefer module-level settings aliases when a value is process-wide and immutable for the lifetime of the process.
- Bot-level runtime overrides remain explicit bot config payload values rather than global process settings.
