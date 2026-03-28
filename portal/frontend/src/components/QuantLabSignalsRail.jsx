import { useCallback, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, ChevronDown, ChevronUp, Copy, Crosshair, Search } from 'lucide-react';

import { fetchIndicatorOverlays } from '../adapters/indicator.adapter.js';
import { useChartState, useChartValue } from '../contexts/ChartStateContext.jsx';
import { createLogger } from '../utils/logger.js';
import { normalizeIndicatorArtifactResponse } from './indicatorArtifacts.js';
import {
  buildColorMap,
  buildVisibleOverlaysFromCache,
  normalizedVisibilityMap,
} from './indicatorChartArtifacts.js';
import { writeIndicatorArtifactSliceCache } from './indicatorOverlaySlices.js';
import {
  buildSignalInspectionKey,
  collectSignalBubbleEpochs,
  formatSignalEventLabel,
  formatSignalIdSuffix,
  formatSignalTimestamp,
  resolveSignalChartEpoch,
  resolveSignalCursorEpoch,
  resolveSignalId,
  sortSignalsNewestFirst,
} from './indicatorSignalDebug.js';

const flattenSignals = (
  signalEventsByIndicator = {},
  indicatorsById = {},
  bubbleEpochBySignalId = new Map(),
) => {
  const rows = [];
  Object.entries(signalEventsByIndicator || {}).forEach(([indicatorId, signals]) => {
    if (!Array.isArray(signals) || !signals.length) return;
    const indicator = indicatorsById?.[indicatorId] || null;
    sortSignalsNewestFirst(signals).forEach((signal) => {
      const signalId = resolveSignalId(signal);
      const signalKey = buildSignalInspectionKey(signal);
      const chartEpoch = (signalId && bubbleEpochBySignalId.get(signalId))
        ?? resolveSignalChartEpoch(signal)
        ?? null;
      rows.push({
        indicator,
        indicatorId,
        signal,
        signalId,
        signalKey,
        signalSuffix: formatSignalIdSuffix(signal),
        label: formatSignalEventLabel(signal?.event_key),
        timestamp: formatSignalTimestamp(signal),
        epoch: resolveSignalCursorEpoch(signal) || 0,
        chartEpoch,
        direction: typeof signal?.direction === 'string' ? signal.direction.trim() : '',
        outputName: typeof signal?.output_name === 'string' ? signal.output_name.trim() : '',
        seriesKey: typeof signal?.series_key === 'string' ? signal.series_key.trim() : '',
      });
    });
  });
  return rows.sort((left, right) => right.epoch - left.epoch);
};

const buildSearchHaystack = (entry) => {
  const tokens = [
    entry?.label,
    entry?.signalId,
    entry?.signalSuffix,
    entry?.direction,
    entry?.outputName,
    entry?.seriesKey,
    entry?.indicator?.name,
    entry?.indicator?.type,
    entry?.timestamp,
  ];
  return tokens
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
};

const toISO = (value) => {
  if (!value) return null;
  if (typeof value === 'string') return value;
  if (typeof value?.toISOString === 'function') return value.toISOString();
  return null;
};

const getInspectContext = (chart = {}) => {
  const [rangeStart, rangeEnd] = Array.isArray(chart?.dateRange) ? chart.dateRange : [];
  return {
    symbol: chart?.symbol || null,
    start: toISO(chart?.start || rangeStart),
    end: toISO(chart?.end || rangeEnd),
    interval: chart?.interval || chart?.timeframe || null,
    datasource: chart?.datasource || chart?.provider || null,
    exchange: chart?.exchange || null,
    instrument_id: chart?.instrument_id || null,
  };
};

const buildInspectionState = (entry, payload) => ({
  indicatorId: entry.indicatorId,
  signalId: entry.signalId,
  signalKey: entry.signalKey,
  eventKey: entry.signal?.event_key || null,
  label: entry.label,
  cursorEpoch: entry.epoch,
  cursorTime: payload?.overlay_state?.cursor_time || null,
});

export default function QuantLabSignalsRail({ chartId }) {
  const chart = useChartValue(chartId) || {};
  const { getChart, updateChart } = useChartState();
  const [selectedSignalKey, setSelectedSignalKey] = useState(null);
  const [detailOpen, setDetailOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [inspectionBusyKey, setInspectionBusyKey] = useState(null);
  const [railError, setRailError] = useState(null);
  const [copiedSignalId, setCopiedSignalId] = useState(null);
  const logger = useMemo(() => createLogger('QuantLabSignalsRail', { chartId }), [chartId]);
  const { info, warn, error: logError } = logger;

  const indicators = Array.isArray(chart?.indicators) ? chart.indicators : [];
  const indicatorsById = useMemo(
    () => indicators.reduce((acc, indicator) => {
      if (indicator?.id) acc[indicator.id] = indicator;
      return acc;
    }, {}),
    [indicators],
  );
  const signalEventsByIndicator = chart?.signalEventsByIndicator && typeof chart.signalEventsByIndicator === 'object'
    ? chart.signalEventsByIndicator
    : {};
  const activeSignalInspection = chart?.activeSignalInspection && typeof chart.activeSignalInspection === 'object'
    ? chart.activeSignalInspection
    : null;
  const bubbleEpochBySignalId = useMemo(
    () => collectSignalBubbleEpochs(Array.isArray(chart?.overlays) ? chart.overlays : []),
    [chart?.overlays],
  );

  const allSignals = useMemo(
    () => flattenSignals(signalEventsByIndicator, indicatorsById, bubbleEpochBySignalId),
    [bubbleEpochBySignalId, indicatorsById, signalEventsByIndicator],
  );

  const filteredSignals = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();
    if (!query) return allSignals;
    return allSignals.filter((entry) => buildSearchHaystack(entry).includes(query));
  }, [allSignals, searchQuery]);

  useEffect(() => {
    const allowedKeys = new Set(filteredSignals.map((entry) => entry.signalKey));
    const preferredKey = activeSignalInspection?.signalKey || filteredSignals[0]?.signalKey || null;
    if (selectedSignalKey && allowedKeys.has(selectedSignalKey)) return;
    if (preferredKey) {
      setSelectedSignalKey(preferredKey);
    } else if (selectedSignalKey) {
      setSelectedSignalKey(null);
    }
  }, [activeSignalInspection?.signalKey, filteredSignals, selectedSignalKey]);

  useEffect(() => {
    if (activeSignalInspection?.signalKey) {
      setDetailOpen(true);
    }
  }, [activeSignalInspection?.signalKey]);

  useEffect(() => {
    if (!copiedSignalId) return undefined;
    const timer = window.setTimeout(() => setCopiedSignalId(null), 1200);
    return () => window.clearTimeout(timer);
  }, [copiedSignalId]);

  const selectedEntry = useMemo(
    () => filteredSignals.find((entry) => entry.signalKey === selectedSignalKey)
      || allSignals.find((entry) => entry.signalKey === activeSignalInspection?.signalKey)
      || filteredSignals[0]
      || null,
    [activeSignalInspection?.signalKey, allSignals, filteredSignals, selectedSignalKey],
  );

  const selectedIndex = useMemo(
    () => (selectedEntry ? filteredSignals.findIndex((entry) => entry.signalKey === selectedEntry.signalKey) : -1),
    [filteredSignals, selectedEntry],
  );

  const navigateToSignal = useCallback((entry) => {
    if (!entry) return;
    setSelectedSignalKey(entry.signalKey);
    setRailError(null);
    const focusEpoch = Number(entry.chartEpoch);
    info('signal_navigation_request', {
      signalId: entry.signalId,
      signalKey: entry.signalKey,
      indicatorId: entry.indicatorId,
      chartEpoch: entry.chartEpoch,
      cursorEpoch: entry.epoch,
      label: entry.label,
    });
    if (!Number.isFinite(focusEpoch)) {
      setRailError('Cannot focus signal: plotted signal time is missing.');
      return;
    }
    updateChart(chartId, {
      activeSignalSelection: {
        signalKey: entry.signalKey,
        signalId: entry.signalId,
        cursorEpoch: focusEpoch,
        selectedAt: Date.now(),
      },
    });
    const handles = getChart(chartId)?.handles;
    const focusAtTime = handles?.focusAtTime;
    if (typeof focusAtTime !== 'function') {
      warn('signal_navigation_unavailable', {
        signalId: entry.signalId,
        indicatorId: entry.indicatorId,
      });
      setRailError('Chart focus is not available yet.');
      return;
    }
    const focused = focusAtTime(Number(focusEpoch), { zoomMode: 'signal' });
    if (!focused) {
      warn('signal_navigation_failed', {
        signalId: entry.signalId,
        indicatorId: entry.indicatorId,
        focusEpoch,
      });
      setRailError('Unable to focus that signal on the chart.');
    }
  }, [chartId, getChart, info, updateChart, warn]);

  const openSignalDetail = useCallback((entry) => {
    if (!entry) return;
    setDetailOpen(true);
    navigateToSignal(entry);
  }, [navigateToSignal]);

  const clearInspection = useCallback(() => {
    const latestChart = getChart(chartId) || {};
    const latestIndicators = Array.isArray(latestChart?.indicators) ? latestChart.indicators : [];
    const nextState = buildVisibleOverlaysFromCache(
      latestChart?.indicatorArtifactSlices || {},
      latestIndicators,
      buildColorMap(latestIndicators),
      normalizedVisibilityMap(latestChart?.indicatorVisibilityById),
      null,
      latestChart?.overlays || [],
    );
    updateChart(chartId, {
      indicatorArtifactSlices: nextState.sliceCache,
      overlays: nextState.overlays,
      activeSignalInspection: null,
      activeSignalSelection: null,
    });
  }, [chartId, getChart, updateChart]);

  const inspectSignal = useCallback(async (entry) => {
    if (!entry) return;
    const indicator = indicatorsById?.[entry.indicatorId];
    if (!indicator) {
      setRailError('Cannot inspect signal: indicator not found.');
      return;
    }

    navigateToSignal(entry);

    const context = getInspectContext(getChart(chartId) || {});
    const missing = ['symbol', 'start', 'end', 'interval', 'datasource', 'instrument_id']
      .filter((key) => !context[key]);
    if (missing.length) {
      setRailError(`Cannot inspect signal: missing ${missing.join(', ')}.`);
      return;
    }

    setInspectionBusyKey(entry.signalKey);
    setRailError(null);
    info('signal_overlay_inspection_start', {
      signalId: entry.signalId,
      indicatorId: entry.indicatorId,
      cursorEpoch: entry.epoch,
    });

    try {
      const payload = await fetchIndicatorOverlays(entry.indicatorId, {
        ...context,
        cursor_epoch: entry.epoch,
      });
      const overlays = normalizeIndicatorArtifactResponse(indicator, payload, { defaultSource: 'inspection' });
      const latestChart = getChart(chartId) || {};
      const latestIndicators = Array.isArray(latestChart?.indicators) ? latestChart.indicators : [];
      const nextInspection = buildInspectionState(entry, payload);
      const nextSliceCache = writeIndicatorArtifactSliceCache(
        latestChart?.indicatorArtifactSlices || {},
        {
          indicatorId: entry.indicatorId,
          source: 'inspection',
          nextSlice: overlays,
        },
      );
      const nextState = buildVisibleOverlaysFromCache(
        nextSliceCache,
        latestIndicators,
        buildColorMap(latestIndicators),
        normalizedVisibilityMap(latestChart?.indicatorVisibilityById),
        nextInspection,
        latestChart?.overlays || [],
      );
      updateChart(chartId, {
        indicatorArtifactSlices: nextState.sliceCache,
        overlays: nextState.overlays,
        activeSignalInspection: nextInspection,
      });
      info('signal_overlay_inspection_complete', {
        signalId: entry.signalId,
        indicatorId: entry.indicatorId,
        overlays: overlays.length,
      });
    } catch (err) {
      const message = err?.message || 'Failed to inspect signal overlay state.';
      setRailError(message);
      logError('signal_overlay_inspection_failed', {
        signalId: entry.signalId,
        indicatorId: entry.indicatorId,
      }, err);
    } finally {
      setInspectionBusyKey(null);
    }
  }, [chartId, getChart, indicatorsById, info, logError, navigateToSignal, updateChart]);

  const stepSelection = useCallback((delta) => {
    if (!filteredSignals.length) return;
    const baseIndex = selectedIndex >= 0 ? selectedIndex : 0;
    const nextIndex = Math.min(filteredSignals.length - 1, Math.max(0, baseIndex + delta));
    const nextEntry = filteredSignals[nextIndex];
    if (!nextEntry) return;
    openSignalDetail(nextEntry);
  }, [filteredSignals, openSignalDetail, selectedIndex]);

  const copySignalId = useCallback(async (signalId) => {
    if (!signalId || !navigator?.clipboard?.writeText) return;
    try {
      await navigator.clipboard.writeText(signalId);
      setCopiedSignalId(signalId);
    } catch (err) {
      warn('signal_id_copy_failed', { signalId }, err);
    }
  }, [warn]);

  const isInspectingSelected = Boolean(
    selectedEntry
    && activeSignalInspection?.signalKey
    && activeSignalInspection.signalKey === selectedEntry.signalKey,
  );

  return (
    <aside className="qt-signals-rail flex min-h-0 flex-col rounded-[6px] border border-white/10 bg-[#0d1422]/92 shadow-[0_24px_80px_-56px_rgba(0,0,0,0.88)]">
      <div className="border-b border-white/6 px-3 py-3">
        <div className="flex items-center justify-between gap-3">
          <div>
            <p className="text-[9px] uppercase tracking-[0.22em] text-slate-500">Signals</p>
            <h3 className="mt-1 text-[12px] font-semibold text-slate-100">Chart context</h3>
          </div>
          <span className="rounded-[6px] border border-white/10 bg-white/5 px-2.5 py-1 text-[9px] font-semibold text-slate-200">
            {allSignals.length}
          </span>
        </div>
        <label className="relative mt-2.5 block">
          <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-500" />
          <input
            type="text"
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            placeholder="Search label or signal id"
            className="w-full rounded-[6px] border border-white/10 bg-[#0a101b] px-10 py-2 text-[12px] text-slate-100 placeholder-slate-500 outline-none transition focus:border-[color:var(--accent-alpha-40)] focus:ring-2 focus:ring-[color:var(--accent-ring-strong)]"
          />
        </label>
      </div>

      {railError ? (
        <div className="mx-3 mt-3 rounded-[6px] border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-[10px] text-rose-100">
          {railError}
        </div>
      ) : null}

      {detailOpen && selectedEntry ? (
        <div className="mx-3 mt-3 rounded-[8px] border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-08)] p-3">
          <div className="mb-3 flex items-center justify-between gap-3">
            <button
              type="button"
              onClick={() => setDetailOpen(false)}
              className="inline-flex items-center gap-2 rounded-[7px] border border-white/10 px-2.5 py-1.5 text-[9px] font-semibold uppercase tracking-[0.12em] text-slate-300 transition hover:border-white/20 hover:text-white"
            >
              <ArrowLeft className="size-3.5" />
              Back
            </button>
            {selectedEntry.signalSuffix ? (
              <span className="rounded-[8px] border border-white/10 bg-black/30 px-2 py-1 font-mono text-[9px] text-slate-200">
                {selectedEntry.signalSuffix}
              </span>
            ) : null}
          </div>

          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="truncate text-[12px] font-semibold text-slate-100">{selectedEntry.label}</div>
              <div className="mt-1 text-[10px] text-slate-400">
                {selectedEntry.indicator?.name || selectedEntry.indicator?.type || 'Indicator'}
                {selectedEntry.outputName ? ` • ${selectedEntry.outputName}` : ''}
              </div>
            </div>
          </div>

          <div className="mt-3 space-y-2 text-[10px] text-slate-300">
            <div className="flex items-center justify-between gap-3">
              <span className="text-slate-500">Signal ID</span>
              <button
                type="button"
                onClick={() => copySignalId(selectedEntry.signalId)}
                className="inline-flex min-w-0 items-center gap-2 rounded-[7px] border border-white/10 bg-black/20 px-2 py-1 text-slate-200 transition hover:border-white/20 hover:text-white"
                disabled={!selectedEntry.signalId}
              >
                  <span className="truncate font-mono text-[9px]">
                  {selectedEntry.signalId || 'Unavailable'}
                </span>
                <Copy className="size-3.5" />
              </button>
            </div>
            <div className="flex items-center justify-between gap-3">
              <span className="text-slate-500">Time</span>
              <span className="text-right">{selectedEntry.timestamp || 'Unavailable'}</span>
            </div>
            <div className="flex items-center justify-between gap-3">
              <span className="text-slate-500">Direction</span>
              <span className="text-right capitalize">{selectedEntry.direction || 'n/a'}</span>
            </div>
            <div className="flex items-center justify-between gap-3">
              <span className="text-slate-500">Inspect</span>
              <span className={`font-medium ${isInspectingSelected ? 'text-emerald-200' : 'text-slate-300'}`}>
                {isInspectingSelected ? 'Active' : 'Inactive'}
              </span>
            </div>
          </div>

          <div className="mt-3 grid grid-cols-2 gap-2">
            <button
              type="button"
              onClick={() => navigateToSignal(selectedEntry)}
              className="inline-flex items-center justify-center gap-2 rounded-[8px] border border-white/10 bg-white/5 px-3 py-2 text-[10px] font-semibold text-slate-100 transition hover:border-white/20 hover:bg-white/10"
            >
              <Crosshair className="size-3.5" />
              Reveal
            </button>
            <button
              type="button"
              onClick={() => inspectSignal(selectedEntry)}
              disabled={inspectionBusyKey === selectedEntry.signalKey}
              className="rounded-[8px] bg-[color:var(--accent-alpha-20)] px-3 py-2 text-[10px] font-semibold text-[color:var(--accent-text-strong)] transition hover:bg-[color:var(--accent-alpha-30)] disabled:cursor-wait disabled:opacity-70"
            >
              {inspectionBusyKey === selectedEntry.signalKey ? 'Inspecting…' : 'Inspect'}
            </button>
          </div>

          <div className="mt-2 grid grid-cols-3 gap-2">
            <button
              type="button"
              onClick={() => stepSelection(-1)}
              disabled={selectedIndex <= 0}
              className="inline-flex items-center justify-center gap-1 rounded-[7px] border border-white/10 px-2 py-1.5 text-[9px] font-semibold text-slate-200 transition hover:border-white/20 hover:text-white disabled:cursor-not-allowed disabled:text-slate-600"
            >
              <ChevronUp className="size-3.5" />
              Prev
            </button>
            <button
              type="button"
              onClick={() => stepSelection(1)}
              disabled={selectedIndex < 0 || selectedIndex >= filteredSignals.length - 1}
              className="inline-flex items-center justify-center gap-1 rounded-[7px] border border-white/10 px-2 py-1.5 text-[9px] font-semibold text-slate-200 transition hover:border-white/20 hover:text-white disabled:cursor-not-allowed disabled:text-slate-600"
            >
              <ChevronDown className="size-3.5" />
              Next
            </button>
            <button
              type="button"
              onClick={clearInspection}
              disabled={!activeSignalInspection}
              className="rounded-[7px] border border-white/10 px-2 py-1.5 text-[9px] font-semibold text-slate-200 transition hover:border-white/20 hover:text-white disabled:cursor-not-allowed disabled:text-slate-600"
            >
              Exit inspect
            </button>
          </div>

          {copiedSignalId && copiedSignalId === selectedEntry.signalId ? (
            <div className="mt-2 text-[9px] text-emerald-200">Signal ID copied.</div>
          ) : null}
        </div>
      ) : null}

      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
        {!filteredSignals.length ? (
          <div className="flex h-full items-center justify-center rounded-[8px] border border-dashed border-white/10 bg-[#0a101b]/70 px-5 text-center">
            <div className="space-y-2">
              <div className="text-[12px] font-semibold text-slate-100">
                {allSignals.length ? 'No matching signals' : 'No signals yet'}
              </div>
              <p className="text-[10px] text-slate-500">
                {allSignals.length
                  ? 'Adjust the rail search to find a different signal.'
                  : 'Generate signals from an indicator and they will appear here with a stable signal id.'}
              </p>
            </div>
          </div>
        ) : (
          <div className="space-y-2">
            {filteredSignals.map((entry) => {
              const isSelected = selectedEntry?.signalKey === entry.signalKey;
              const isInspecting = activeSignalInspection?.signalKey === entry.signalKey;
              return (
                <button
                  key={entry.signalKey}
                  type="button"
                  onClick={() => openSignalDetail(entry)}
                  className={`w-full rounded-[8px] border px-3 py-2.5 text-left transition ${
                    detailOpen && isSelected
                      ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-08)]'
                      : isInspecting
                        ? 'border-emerald-400/30 bg-emerald-500/8'
                        : 'border-white/8 bg-[#0a101b] hover:border-white/16 hover:bg-[#0c1320]'
                  }`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="truncate text-[12px] font-semibold text-slate-100">
                        {entry.label}
                      </div>
                      <div className="mt-1 truncate text-[10px] text-slate-400">
                        {entry.indicator?.name || entry.indicator?.type || 'Indicator'}
                        {entry.timestamp ? ` • ${entry.timestamp}` : ''}
                      </div>
                    </div>
                    <div className="shrink-0 text-right">
                      {entry.signalSuffix ? (
                        <div className="rounded-[8px] border border-white/10 bg-black/25 px-2 py-0.5 font-mono text-[9px] text-slate-200">
                          {entry.signalSuffix}
                        </div>
                      ) : null}
                      {isInspecting ? (
                        <div className="mt-1 text-[8px] font-semibold uppercase tracking-[0.12em] text-emerald-200">
                          Inspecting
                        </div>
                      ) : null}
                    </div>
                  </div>
                </button>
              );
            })}
          </div>
        )}
      </div>
    </aside>
  );
}
