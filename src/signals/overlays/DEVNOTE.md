# Overlay registry notes

- All overlay payloads **must** be wrapped with `build_overlay` before they leave the backend.
- Every overlay type must be registered in the overlay registry with `pane_views` and payload keys.
  - Indicators should use `@overlay_type` in their overlay adapters.
  - Non-indicator overlays should be registered in `builtins.py` or at the runtime boundary.
- If you add a new overlay type, update both the registry metadata and any frontend consumers that
  rely on `pane_views`.
- Use `ui_color` in the registry to provide consistent UI accents for overlay toggles or legends.
