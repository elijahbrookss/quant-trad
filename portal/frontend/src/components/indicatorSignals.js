import { createLogger } from '../utils/logger.js';

const signalsLogger = createLogger('IndicatorSignals');

export const hexToRgba = (hex, a = 0.18) => {
  if (!hex || !hex.startsWith('#')) return `rgba(156,163,175,${a})`;
  const v = hex.slice(1);
  const n = v.length === 3
    ? v.split('').map(c => parseInt(c + c, 16))
    : [parseInt(v.slice(0, 2), 16), parseInt(v.slice(2, 4), 16), parseInt(v.slice(4, 6), 16)];
  return `rgba(${n[0]},${n[1]},${n[2]},${a})`;
};

export const applyIndicatorColors = (overlays = [], colors = {}) =>
  (overlays || []).map(ov => {
    if (!ov || !ov.ind_id || !ov.payload) return ov;
    const color = colors[ov.ind_id] || ov.color;
    if (!color) return ov;

    const price_lines = Array.isArray(ov.payload.price_lines)
      ? ov.payload.price_lines.map(pl => (pl ? { ...pl, color } : pl))
      : ov.payload.price_lines;

    const markers = Array.isArray(ov.payload.markers)
      ? ov.payload.markers.map(m => (m ? { ...m, color } : m))
      : ov.payload.markers;

    const boxes = Array.isArray(ov.payload.boxes)
      ? ov.payload.boxes.map(b => {
          if (!b) return b;
          return { ...b, color: hexToRgba(color, 0.1), border: { color: hexToRgba(color, 0.7), width: 1 } };
        })
      : ov.payload.boxes;

    const tintHex = hexToRgba(color, 0.7);

    const segments = Array.isArray(ov.payload.segments)
      ? ov.payload.segments.map(s => (s ? { ...s, color: tintHex } : s))
      : ov.payload.segments;

    const polylines = Array.isArray(ov.payload.polylines)
      ? ov.payload.polylines.map(l => (l ? { ...l, color: tintHex } : l))
      : ov.payload.polylines;

    return {
      ...ov,
      color,
      payload: {
        ...ov.payload,
        price_lines,
        markers,
        boxes,
        segments,
        polylines,
      },
    };
  });

export async function runSignalGeneration({
  indicator,
  chartId,
  chartState,
  startISO,
  endISO,
  getChart,
  updateChart,
  setError,
  signalsAdapter,
}) {
  if (!indicator) {
    signalsLogger.warn('signal_generation_skipped_indicator_missing', { chartId });
    setError?.('Cannot generate signals: indicator not found.');
    return false;
  }

  if (!chartState || !chartState.symbol || !chartState.interval) {
    signalsLogger.warn('signal_generation_skipped_chart_inputs', {
      chartId,
      hasChartState: Boolean(chartState),
    });
    setError?.('Cannot generate signals: missing chart symbol or interval.');
    return false;
  }

  if (!startISO || !endISO) {
    signalsLogger.warn('signal_generation_skipped_window', { chartId, startISO, endISO });
    setError?.('Cannot generate signals: chart window is not ready.');
    return false;
  }

  const scopedLogger = signalsLogger.child({ chartId, indicatorId: indicator.id });
  scopedLogger.info('signal_generation_start', {
    start: startISO,
    end: endISO,
    interval: chartState.interval,
    datasource: chartState.datasource,
    exchange: chartState.exchange,
  });

  const loadingState = getChart(chartId) || {};
  const prevLoadingMap = loadingState?.signalsLoadingByIndicator && typeof loadingState.signalsLoadingByIndicator === 'object'
    ? loadingState.signalsLoadingByIndicator
    : {};
  const nextLoadingMap = { ...prevLoadingMap, [indicator.id]: true };
  const nextActiveIds = Object.keys(nextLoadingMap);
  updateChart(chartId, {
    signalsLoading: nextActiveIds.length > 0,
    signalsLoadingFor: nextActiveIds[0] || indicator.id,
    signalsLoadingByIndicator: nextLoadingMap,
    signalsLoadingCount: nextActiveIds.length,
  });

  try {
    const requestPayload = {
      start: startISO,
      end: endISO,
      interval: chartState.interval,
      symbol: chartState.symbol,
    };
    scopedLogger.debug('signal_generation_request', { requestPayload });

    if (chartState.datasource) {
      requestPayload.datasource = chartState.datasource;
    }
    if (chartState.exchange) {
      requestPayload.exchange = chartState.exchange;
    }

    const response = await signalsAdapter(indicator.id, requestPayload);

    const rawSignals = Array.isArray(response?.signals) ? response.signals : [];
    const prevSignals = getChart(chartId)?.signalResults || {};
    updateChart(chartId, {
      signalResults: { ...prevSignals, [indicator.id]: rawSignals },
    });

    scopedLogger.info('signal_generation_complete', {
      signals: rawSignals.length,
    });

    setError?.(null);
    return true;
  } catch (err) {
    const msg = err?.message || 'Failed to generate signals.';
    scopedLogger.error('signal_generation_failed', { message: msg }, err);
    setError?.(msg);
    return false;
  } finally {
    const latest = getChart(chartId) || {};
    const currentLoadingMap = latest?.signalsLoadingByIndicator && typeof latest.signalsLoadingByIndicator === 'object'
      ? latest.signalsLoadingByIndicator
      : {};
    const reducedLoadingMap = { ...currentLoadingMap };
    delete reducedLoadingMap[indicator.id];
    const activeIds = Object.keys(reducedLoadingMap);
    updateChart(chartId, {
      signalsLoading: activeIds.length > 0,
      signalsLoadingFor: activeIds[0] || null,
      signalsLoadingByIndicator: activeIds.length ? reducedLoadingMap : null,
      signalsLoadingCount: activeIds.length,
    });
  }
}
