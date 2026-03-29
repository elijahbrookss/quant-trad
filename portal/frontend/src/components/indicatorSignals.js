import { createLogger } from '../utils/logger.js';
import {
  getIndicatorSignalColor,
  getPaletteOverlayColor,
} from '../utils/indicatorColors.js';
import { enabledSignalOutputNames } from '../utils/indicatorOutputs.js';
import { normalizeIndicatorArtifactResponse } from './indicatorArtifacts.js';
import {
  rebuildIndicatorArtifactsFromCache,
  seedIndicatorArtifactSliceCache,
  writeIndicatorArtifactSliceCache,
} from './indicatorOverlaySlices.js';

const signalsLogger = createLogger('IndicatorSignals');

export const hexToRgba = (hex, a = 0.18) => {
  if (!hex || !hex.startsWith('#')) return `rgba(156,163,175,${a})`;
  const v = hex.slice(1);
  const n = v.length === 3
    ? v.split('').map(c => parseInt(c + c, 16))
    : [parseInt(v.slice(0, 2), 16), parseInt(v.slice(2, 4), 16), parseInt(v.slice(4, 6), 16)];
  return `rgba(${n[0]},${n[1]},${n[2]},${a})`;
};

const normalizeColor = (value) => (
  typeof value === 'string' && value.trim() ? value.trim() : null
);

const buildIndicatorMetaMap = (indicators = []) => {
  if (!Array.isArray(indicators)) return {};
  return indicators.reduce((acc, indicator) => {
    if (!indicator?.id) return acc;
    acc[indicator.id] = indicator;
    return acc;
  }, {});
};

export const applyIndicatorColors = (overlays = [], colors = {}, indicators = []) => {
  const indicatorsById = Array.isArray(indicators) ? buildIndicatorMetaMap(indicators) : (indicators || {});
  return (overlays || []).map(ov => {
    if (!ov || !ov.ind_id || !ov.payload) return ov;
    const indicator = indicatorsById?.[ov.ind_id] || null;
    const colorPolicy = ov?.ui?.color_policy;
    const paletteOverlayColor = normalizeColor(getPaletteOverlayColor(indicator, ov?.type));
    const signalColor = ov?.source === 'signal'
      ? normalizeColor(getIndicatorSignalColor(indicator))
      : null;
    const lockedOverlayColor = colorPolicy === 'overlay' ? normalizeColor(ov?.ui?.color) || normalizeColor(ov?.color) : null;
    const color = signalColor || paletteOverlayColor || lockedOverlayColor || colors[ov.ind_id] || ov.color;
    if (!color) return ov;
    const forceExplicitColor = Boolean(signalColor || paletteOverlayColor);

    const price_lines = Array.isArray(ov.payload.price_lines)
      ? ov.payload.price_lines.map(pl => (pl ? { ...pl, color: forceExplicitColor ? color : (pl.color || color) } : pl))
      : ov.payload.price_lines;

    const markers = Array.isArray(ov.payload.markers)
      ? ov.payload.markers.map(m => (m ? { ...m, color: forceExplicitColor ? color : (m.color || color) } : m))
      : ov.payload.markers;

    const boxes = Array.isArray(ov.payload.boxes)
      ? ov.payload.boxes.map(b => {
          if (!b) return b;
          return {
            ...b,
            color: forceExplicitColor ? hexToRgba(color, 0.1) : (b.color || hexToRgba(color, 0.1)),
            border: forceExplicitColor
              ? { ...(b.border || {}), color: hexToRgba(color, 0.7), width: b?.border?.width || 1 }
              : (b.border || { color: hexToRgba(color, 0.7), width: 1 }),
          };
        })
      : ov.payload.boxes;

    const tintHex = hexToRgba(color, 0.7);

    const segments = Array.isArray(ov.payload.segments)
      ? ov.payload.segments.map(s => (s ? { ...s, color: forceExplicitColor ? tintHex : (s.color || tintHex) } : s))
      : ov.payload.segments;

    const polylines = Array.isArray(ov.payload.polylines)
      ? ov.payload.polylines.map(l => (l ? { ...l, color: forceExplicitColor ? tintHex : (l.color || tintHex) } : l))
      : ov.payload.polylines;

    return {
      ...ov,
      color,
      ui: {
        ...(ov.ui || {}),
        color,
        color_policy: colorPolicy || 'indicator',
      },
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
};

const buildVisibleArtifactSets = (indicators = [], visibilityByIndicator = {}, activeInspection = null) => {
  const visibleIndicatorIds = new Set(
    (indicators || [])
      .filter((entry) => entry?.enabled !== false && visibilityByIndicator?.[entry.id] !== false)
      .map((entry) => entry?.id)
      .filter(Boolean)
  );
  const inspectionIndicatorId = activeInspection?.indicatorId;
  const indicatorIds = new Set(visibleIndicatorIds);
  const inspectionIds = new Set();
  if (inspectionIndicatorId && visibleIndicatorIds.has(inspectionIndicatorId)) {
    indicatorIds.delete(inspectionIndicatorId);
    inspectionIds.add(inspectionIndicatorId);
  }
  return {
    indicator: indicatorIds,
    signal: visibleIndicatorIds,
    inspection: inspectionIds,
  };
};

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

  if (!chartState || !chartState.symbol || !chartState.interval || !chartState.instrument_id) {
    signalsLogger.warn('signal_generation_skipped_chart_inputs', {
      chartId,
      hasChartState: Boolean(chartState),
    });
    setError?.('Cannot generate signals: missing chart instrument or interval.');
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
    instrument_id: chartState.instrument_id,
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
    if (chartState.instrument_id) {
      requestPayload.instrument_id = chartState.instrument_id;
    }
    if (Array.isArray(indicator?.typed_outputs)) {
      requestPayload.config = {
        enabled_signal_outputs: enabledSignalOutputNames(indicator),
      };
    }

    const response = await signalsAdapter(indicator.id, requestPayload);

    const rawSignals = Array.isArray(response?.signals) ? response.signals : [];
    const signalOverlays = normalizeIndicatorArtifactResponse(indicator, response, { defaultSource: 'signal' });
    const latestState = getChart(chartId) || {};
    const retainBySource = buildVisibleArtifactSets(
      Array.isArray(latestState?.indicators) ? latestState.indicators : [],
      latestState?.indicatorVisibilityById && typeof latestState.indicatorVisibilityById === 'object'
        ? latestState.indicatorVisibilityById
        : {},
      latestState?.activeSignalInspection || null,
    );
    const nextSliceCache = writeIndicatorArtifactSliceCache(
      seedIndicatorArtifactSliceCache(latestState?.indicatorArtifactSlices || {}, latestState?.overlays || []),
      {
        indicatorId: indicator.id,
        source: 'signal',
        nextSlice: signalOverlays,
      },
    );
    const indicatorList = Array.isArray(latestState?.indicators) ? latestState.indicators : [];
    const indicatorColors = indicatorList.reduce((acc, entry) => {
      if (!entry?.id) return acc;
      const color = normalizeColor(entry?.color);
      if (color) acc[entry.id] = color;
      return acc;
    }, {});
    const nextOverlays = applyIndicatorColors(
      rebuildIndicatorArtifactsFromCache(nextSliceCache, retainBySource),
      indicatorColors,
      indicatorList,
    );
    const prevSignals = latestState?.signalEventsByIndicator || {};
    updateChart(chartId, {
      overlays: nextOverlays,
      indicatorArtifactSlices: nextSliceCache,
      signalEventsByIndicator: { ...prevSignals, [indicator.id]: rawSignals },
    });

    scopedLogger.info('signal_generation_complete', {
      signals: rawSignals.length,
      overlays: signalOverlays.length,
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
