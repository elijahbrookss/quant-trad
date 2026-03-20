# Overlay registry notes

- All overlay payloads **must** be wrapped with `build_overlay` before they leave the backend.
- Every overlay type must be registered in the overlay registry with `pane_views` and payload keys.
- Overlay registrations may also declare `pane_key`.
  - `price` is the default main price pane.
  - additional panes such as `volatility` and `oscillator` are routed declaratively from overlay metadata.
  - normalized oscillators should not share panes with price-unit overlays.
  - frontend pane lifecycle should be driven from the active `pane_key` set in the current frame, not hardcoded per indicator.
- Frontend pane definitions belong in the shared pane registry, not inside individual chart consumers.
- Use shared overlay builders for common payload shapes like single-line `polylines`.
- Indicators should use `@overlay_type` in their overlay adapters.
  - Non-indicator overlays should be registered in `builtins.py` or at the runtime boundary.
- If you add a new overlay type, update both the registry metadata and any frontend consumers that
  rely on `pane_views`.
- Use `ui_color` and `label` in the registry to provide consistent UI accents and compact pane legends.
