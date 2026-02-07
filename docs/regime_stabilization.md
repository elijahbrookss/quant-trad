# Regime Stabilization & Block Tuning

Quant-Trad now stabilizes raw per-candle regimes into longer-lived, debuggable regime blocks.
This keeps regime analysis aligned with walk-forward timing while reducing flicker in overlays.

## Stabilization Stages (high level)

1. **Raw regime classification** using existing thresholds.
2. **Smoothing (optional)** on key features (EMA) before decisions.
3. **Hysteresis** for structure + volatility (enter vs exit thresholds).
4. **N-bar confirmation** before switching to a new state.
5. **Confidence gating** prevents low-confidence flips (with a volatility override).
6. **Regime blocks** merge short-lived switches into the prior block.

## Default Tuning Values

These defaults are defined in `RegimeStabilizerConfig` and `RegimeBlockConfig`
(see `portal/backend/service/market/regime_config.py`):

- `min_confidence`: **0.60**
- `confirm_bars`:
  - structure: **3**
  - volatility: **4**
  - liquidity: **3**
  - expansion: **3**
- structure hysteresis (`directional_efficiency`):
  - enter trend: **0.62**
  - exit trend: **0.52**
- volatility hysteresis (`atr_ratio`):
  - enter high: **1.15**
  - exit high: **1.10**
  - enter low: **0.85**
  - exit low: **0.90**
- volatility hysteresis (`tr_pct`):
  - enter high: **0.02**
  - exit high: **0.015**
  - enter low: **0.008**
  - exit low: **0.010**
- smoothing:
  - EMA alpha: **0.25**
  - default axes: **structure + volatility**
  - features: `directional_efficiency`, `atr_ratio`, `atr_zscore`, `volume_zscore`, `atr_slope`
- regime blocks:
  - minimum block length: **10 bars**

## Persistence

Regime blocks are stored in the `regime_blocks` table (configurable via `REGIME_BLOCKS_TABLE`).
Each block row includes `block_id`, start/end timestamps, regime version, and the full block JSON.

## Tuning Guidance

- **Reduce flicker**: increase `confirm_bars` and/or `min_block_bars`.
- **React faster**: lower `confirm_bars` or `min_confidence`.
- **Avoid trend whipsaws**: widen `structure_enter_trend`/`structure_exit_trend`.
- **Handle volatility spikes**: increase `hard_volatility_high_*` thresholds.

When tuning, keep parameters deterministic and avoid introducing prediction or
look-ahead behavior—regimes must remain known-at valid.
