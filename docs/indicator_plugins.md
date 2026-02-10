# Indicator Plugin Manifests

Indicator runtime integration now has **one registration path**: `indicator_plugin_manifest` in
`src/engines/bot_runtime/core/indicator_state/plugins/registry.py`.

Each plugin manifest must define:
- `indicator_type`
- `engine_factory` (**required**)
- `evaluation_mode` (`session` or `rolling`)

Optional capabilities:
- `signal_emitter` (derive signals from engine snapshot state)
- `overlay_projector` (derive overlay projection entries)
- `signal_rules` / `signal_overlay_adapter` (bridges to signal registry via same decorator)

## Rules
- Signals must be derivable from engine state alone.
- Runtime must work headless (without projector).
- Session-based indicators own session reset behavior and do not rely on global bounded lookback.

## Adding a new indicator plugin
1. Implement an `IndicatorStateEngine`.
2. Add `@indicator_plugin_manifest(...)` registration.
3. Set `evaluation_mode` correctly (`session` vs `rolling`).
4. Add engine smoke test ensuring `apply_bar` yields a state delta.


Plugin registration is initialized at webserver startup via `ensure_builtin_indicator_plugins_registered()` in `portal/backend/main.py`, before bot runtime usage.
