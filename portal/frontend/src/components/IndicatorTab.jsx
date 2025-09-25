import { useState, useEffect, useMemo } from 'react'
import { Switch, Popover, Transition, PopoverButton, PopoverPanel } from '@headlessui/react'
import { Plus, X } from 'lucide-react'
import {
  fetchIndicators,
  createIndicator,
  updateIndicator,
  deleteIndicator,
  fetchIndicatorOverlays,
  generateIndicatorSignals,
} from '../adapters/indicator.adapter'
import { applyIndicatorColors, runSignalGeneration } from './indicatorSignals.js'
// import IndicatorModal from './IndicatorModal'
import IndicatorModalV2 from './IndicatorModal.v2.jsx'
const IndicatorModal = IndicatorModalV2; // for now, swap in new version under old name
import { useChartState } from '../contexts/ChartStateContext'
import IndicatorCard from './IndicatorCard.jsx';
import { createLogger } from '../utils/logger.js';
import LoadingOverlay from './LoadingOverlay.jsx';


// Gold, Maroon, Orange, Purple, Lime, Gray
const COLOR_SWATCHES = [
  '#facc15', '#b91c1c', '#f97316', '#a855f7', '#84cc16', '#6b7280',
  '#3b82f6', '#10b981', '#ec4899', '#14b8a6', '#eab308', '#f43f5e'
];

const DEFAULT_INDICATOR_COLOR = '#60a5fa';
const INDICATOR_PAGE_SIZE = 6;

const buildColorMap = (list = []) => {
  if (!Array.isArray(list)) return {};
  return list.reduce((acc, indicator) => {
    if (!indicator?.id) return acc;
    const raw = typeof indicator?.color === 'string' ? indicator.color.trim() : '';
    acc[indicator.id] = raw || DEFAULT_INDICATOR_COLOR;
    return acc;
  }, {});
};

const shallowEqualMap = (a = {}, b = {}) => {
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  for (const key of keysA) {
    if (a[key] !== b[key]) return false;
  }
  return true;
};

const formatIndicatorType = (type) => {
  if (!type) return 'Custom';
  return type
    .split(/[_-]+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
};

const toInt = (v) => {
  if (typeof v === 'number') return Math.trunc(v);
  if (typeof v === 'string') {
    const n = Number(v.trim());
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }
  return null;
};

const toIntList = (v) => {
  if (Array.isArray(v)) return v.map(toInt).filter((n) => n !== null);
  if (typeof v === 'string') {
    const tokens = v.split(/[\s,;]+/).filter(Boolean);
    return tokens.map(toInt).filter((n) => n !== null);
  }
  if (v == null) return [];
  const n = toInt(v);
  return n !== null ? [n] : [];
};

// normalize known params (add more keys here if needed)
const normalizeParams = (params) => {
  const p = { ...params };
  if (p.lookbacks !== undefined) p.lookbacks = toIntList(p.lookbacks);
  return p;
};

// Manages the list of indicators and syncs enabled ones to the chart context
export const IndicatorSection = ({ chartId }) => {
  const [indicators, setIndicators] = useState([])
  const [modalOpen, setModalOpen] = useState(false)
  const [editing, setEditing] = useState(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState(null)
  const [indColors, setIndColors] = useState({});
  const [showEnabledOnly, setShowEnabledOnly] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [typeFilter, setTypeFilter] = useState('all');
  const [currentPage, setCurrentPage] = useState(1);


  const { updateChart, getChart } = useChartState()

  const logger = useMemo(() => createLogger('IndicatorSection', { chartId }), [chartId])
  const { debug, info, warn, error: logError } = logger

  // Read current chart slice
  const chartState = getChart(chartId)

  useEffect(() => {
    if (!Array.isArray(indicators)) {
      setIndColors((prev) => (Object.keys(prev).length ? {} : prev));
      return;
    }
    const next = buildColorMap(indicators);
    setIndColors((prev) => (shallowEqualMap(prev, next) ? prev : next));
  }, [indicators]);

  useEffect(() => {
    debug('indicator_chart_state_snapshot', {
      hasState: Boolean(chartState),
      version: chartState?._version ?? 0,
      overlayCount: chartState?.overlays?.length ?? 0,
    });
  }, [chartState, debug]);

  // Derive ISO start/end from dateRange
  const [startISO, endISO] = useMemo(() => {
    const [s, e] = chartState?.dateRange || []
    const sISO = typeof s === 'string' ? s : s?.toISOString()
    const eISO = typeof e === 'string' ? e : e?.toISOString()
    return [sISO, eISO]
  }, [chartState?.dateRange?.[0], chartState?.dateRange?.[1]])

  useEffect(() => {
    if (!chartState || !chartState._version) {
      warn('indicator_refresh_skipped_version', { reason: 'no_version' });
      setIsLoading(false);
      return;
    }
    if (!chartState.symbol || !chartState.interval) {
      warn('indicator_refresh_skipped_inputs', {
        symbol: chartState.symbol,
        interval: chartState.interval,
      });
      setIsLoading(false);
      return;
    }

    // clear overlays immediately
    updateChart(chartId, { overlays: [] });

    let isMounted = true;
    setIsLoading(true);

    (async () => {
      try {
        await refreshEnabledOverlays(); // uses current indicators list; patches params before overlays
      } catch (e) {
        if (isMounted) {
          setError(e.message);
          logError('indicator_refresh_failed', e);
        }
      } finally {
        if (isMounted) setIsLoading(false);
      }
    })();

    return () => { isMounted = false; };
  }, [chartId, chartState?._version]);

  // When indicator colors change, recolor overlays in chart context (post-render).
  useEffect(() => {
    const overlays = (getChart(chartId)?.overlays) || [];
    if (!overlays.length) return;
    const recolored = applyIndicatorColors(overlays, indColors);
    updateChart(chartId, { overlays: recolored });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [indColors, chartId]);

  // Refresh overlays for enabled indicators
  // ensure enabled indicators carry current chart symbol/interval before fetching overlays
  // re-fetch indicators and ensure enabled indicators' params match current chart before overlays
  // patch enabled indicators to current chart symbol/interval, then compute overlays
  const refreshEnabledOverlays = async (list = indicators) => {
    updateChart(chartId, { overlayLoading: true }); // show loading state

    if (!chartState) return;

    // if list is empty/undefined, try one fetch to seed; otherwise use provided/current list
    let working = Array.isArray(list) && list.length ? list : indicators;
    if (!Array.isArray(working) || working.length === 0) {
      try {
        working = (await fetchIndicators({ symbol: chartState.symbol, interval: chartState.interval })) || [];
        setIndicators(working);
        updateChart(chartId, { indicators: working });
      } catch (e) {
        logError('indicator_seed_failed', e);
        updateChart(chartId, { overlays: [] });
        return;
      }
    }

    // patch params for enabled indicators if symbol/interval mismatch
    const enabled = working.filter(i => i?.enabled);
    info('overlay_refresh_start', {
      enabled: enabled.length,
      symbol: chartState.symbol,
      interval: chartState.interval,
    });
    const patched = await Promise.all(enabled.map(async (ind) => {
      const p = ind?.params || {};
      const desiredSymbol = chartState.symbol;
      const desiredInterval = chartState.interval;
      const needPatch = p.symbol !== desiredSymbol || p.interval !== desiredInterval;

      if (!needPatch) return ind;

      try {
        const nextParams = { ...p, symbol: desiredSymbol, interval: desiredInterval, start: startISO, end: endISO };
        const updated = await updateIndicator(ind.id, { type: ind.type, params: nextParams, name: ind.name });
        return updated || { ...ind, params: nextParams };
      } catch (e) {
        warn('indicator_param_patch_failed', { indicatorId: ind.id, message: e?.message }, e);
        // fall back locally so overlays still align this session
        return { ...ind, params: { ...p, symbol: desiredSymbol, interval: desiredInterval, start: startISO, end: endISO } };
      }
    }));

    // merge patched back into full list and persist
    const byId = new Map(patched.map(p => [p.id, p]));
    const merged = working.map(ind => byId.get(ind.id) || ind);
    if (merged !== working) {
      setIndicators(merged);
      updateChart(chartId, { indicators: merged });
    }

    // compute overlays for enabled indicators using current chart window
    const body = {
      start: startISO,
      end: endISO,
      interval: chartState.interval,
      symbol: chartState.symbol,
    };

    const results = await Promise.all(
      patched.map(async (ind) => {
        try {
          const payload = await fetchIndicatorOverlays(ind.id, body);
          info('overlay_fetch_success', {
            indicatorId: ind.id,
            indicatorType: ind.type,
            hasPayload: Boolean(payload),
          });
          return payload ? { ind_id: ind.id, type: ind.type, payload } : null;
        } catch (e) {
          const msg = String(e?.message ?? e);
          if (
            msg.includes('Indicator not found') ||
            msg.includes('No candles available') ||
            msg.includes('No overlays computed')
          ) {
            warn('overlay_fetch_skipped', { indicatorId: ind.id, message: msg });
            return null;
          }
          logError('overlay_fetch_failed', { indicatorId: ind.id }, e);
          return null;
        }
      })
    );

    const overlaysPayload = results.filter(Boolean);
    const nextColorMap = buildColorMap(merged);
    setIndColors((prev) => (shallowEqualMap(prev, nextColorMap) ? prev : nextColorMap));

    const colored = applyIndicatorColors(overlaysPayload, nextColorMap);
    updateChart(chartId, { overlays: colored, overlayLoading: false });
    info('overlay_refresh_complete', {
      overlays: colored.length,
      indicatorsProcessed: patched.length,
      enabledCount: enabled.length,
    });
  };

  // Handlers for modal save/delete
  const handleSave = async (meta) => {
    try {
      const core = normalizeParams(meta.params);

      if ('lookbacks' in core) {
        if (!Array.isArray(core.lookbacks) || core.lookbacks.length === 0) {
          setError('Lookbacks must be a comma/space-separated list of integers, e.g., "5, 10, 20".');
          return;
        }
      }

      const params = {
        ...core,
        start: startISO,
        end: endISO,
        symbol: chartState?.symbol,
        interval: chartState?.interval,
      };

      let result;
      let indicatorId = meta.id;
      if (meta.id) {
        const existing = indicators.find((i) => i.id === meta.id);
        result = await updateIndicator(meta.id, {
          type: meta.type,
          params,
          name: meta.name,
          color: existing?.color ?? null,
        });
        indicatorId = result?.id ?? meta.id;
        setIndicators((prev) => {
          const next = prev.map((i) => (i.id === result.id ? result : i));
          queueMicrotask(() => { void refreshEnabledOverlays(next); });
          return next;
        });
      } else {
        result = await createIndicator({ type: meta.type, params, name: meta.name });
        indicatorId = result?.id ?? null;
        setIndicators((prev) => {
          const next = [...prev, result];
          queueMicrotask(() => { void refreshEnabledOverlays(next); });
          return next;
        });
      }

      if (indicatorId) {
        const ruleSelection = Array.isArray(meta.signalRules) ? meta.signalRules : null;
        const currentConfig = getChart(chartId)?.signalsConfig || {};
        const currentEnabled = currentConfig.enabledRules || {};
        const nextEnabled = { ...currentEnabled };
        if (ruleSelection && ruleSelection.length) {
          nextEnabled[indicatorId] = ruleSelection;
        }
        const nextSignalsConfig = {
          ...currentConfig,
          enabledRules: nextEnabled,
        };
        updateChart(chartId, { signalsConfig: nextSignalsConfig });
      }

      setModalOpen(false);
      setError(null);
    } catch (e) {
      setError(e.message);
      logError('indicator_save_failed', e);
    }
  };

  const handleDelete = async (id) => {
    try {
      await deleteIndicator(id)
      setIndicators(prev => prev.filter(i => i.id !== id))
      const currentConfig = getChart(chartId)?.signalsConfig
      const enabledRules = currentConfig?.enabledRules
      if (enabledRules && Object.prototype.hasOwnProperty.call(enabledRules, id)) {
        const nextEnabled = { ...enabledRules }
        delete nextEnabled[id]
        updateChart(chartId, {
          signalsConfig: { ...currentConfig, enabledRules: nextEnabled },
        })
      }
    } catch (e) {
      setError(e.message)
      logError('indicator_delete_failed', e)
    }
  }

  // refresh overlays immediately after toggling; pass the fresh list to avoid stale closures
  const toggleEnable = (id) => {
    setIndicators(prev => {
      const next = prev.map(i => i.id === id ? { ...i, enabled: !i.enabled } : i);
      queueMicrotask(() => { void refreshEnabledOverlays(next); }); // microtask prevents state timing issues
      return next;
    });
  };


  // Regenerate signals (not yet implemented)
  const generateSignals = async (id) => {
    const indicator = indicators.find((ind) => ind.id === id);
    await runSignalGeneration({
      indicator,
      chartId,
      chartState,
      startISO,
      endISO,
      indColors,
      getChart,
      updateChart,
      setError,
      signalsAdapter: generateIndicatorSignals,
    });
  };


  const openEditModal = (indicator = null) => {
    if (indicator) {
      const enabledRules = chartState?.signalsConfig?.enabledRules?.[indicator.id] || []
      setEditing({ ...indicator, signalRules: [...enabledRules] })
    } else {
      setEditing(null)
    }
    setModalOpen(true)
    setError(null)
  }

  const handleSelectColor = async (indicatorId, color) => {
    const indicator = indicators.find((ind) => ind.id === indicatorId);
    if (!indicator) return;

    const normalizedColor = typeof color === 'string' && color.trim()
      ? color.trim()
      : DEFAULT_INDICATOR_COLOR;

    const patchedParams = {
      ...indicator.params,
      symbol: indicator.params?.symbol ?? chartState?.symbol ?? undefined,
      interval: indicator.params?.interval ?? chartState?.interval ?? undefined,
      start: indicator.params?.start ?? startISO ?? undefined,
      end: indicator.params?.end ?? endISO ?? undefined,
    };

    setIndColors((prev) => ({ ...prev, [indicatorId]: normalizedColor }));

    const optimisticIndicators = indicators.map((ind) =>
      ind.id === indicatorId ? { ...ind, color: normalizedColor ?? null, params: patchedParams } : ind,
    );
    setIndicators(optimisticIndicators);
    updateChart(chartId, { indicators: optimisticIndicators });

    try {
      const updated = await updateIndicator(indicatorId, {
        type: indicator.type,
        name: indicator.name,
        params: patchedParams,
        color: normalizedColor,
      });
      if (updated) {
        setIndicators((prev) => {
          const next = prev.map((ind) => (ind.id === indicatorId ? updated : ind));
          updateChart(chartId, { indicators: next });
          return next;
        });
        setIndColors((prev) => ({
          ...prev,
          [indicatorId]: updated.color?.trim() ? updated.color : DEFAULT_INDICATOR_COLOR,
        }));
      }
    } catch (e) {
      setError(e.message);
      logError('indicator_color_update_failed', e);
      setIndColors((prev) => ({
        ...prev,
        [indicatorId]: indicator.color?.trim() ? indicator.color : DEFAULT_INDICATOR_COLOR,
      }));
      setIndicators((prev) => {
        const next = prev.map((ind) => (
          ind.id === indicatorId
            ? {
                ...ind,
                color: indicator.color ?? null,
                params: indicator.params ?? ind.params,
              }
            : ind
        ));
        updateChart(chartId, { indicators: next });
        return next;
      });
    }
  };

  const isSignalsLoading = !!chartState?.signalsLoading
  const signalsLoadingFor = chartState?.signalsLoadingFor

  const typeOptions = useMemo(() => {
    const unique = new Set();
    indicators.forEach((indicator) => {
      if (indicator?.type) unique.add(indicator.type);
    });
    return Array.from(unique).sort();
  }, [indicators]);

  const trimmedSearchQuery = searchQuery.trim();

  const filteredIndicators = useMemo(() => {
    const base = showEnabledOnly ? indicators.filter((ind) => ind?.enabled) : indicators;

    const byType = typeFilter === 'all'
      ? base
      : base.filter((indicator) => (indicator?.type ?? '') === typeFilter);

    const query = trimmedSearchQuery.toLowerCase();
    if (!query) return byType;

    return byType.filter((indicator) => {
      const name = (indicator?.name ?? '').toLowerCase();
      const type = (indicator?.type ?? '').toLowerCase();
      const paramsString = JSON.stringify(indicator?.params ?? {}).toLowerCase();
      return name.includes(query) || type.includes(query) || paramsString.includes(query);
    });
  }, [indicators, showEnabledOnly, trimmedSearchQuery, typeFilter]);

  const enabledCount = useMemo(
    () => indicators.filter((indicator) => indicator?.enabled).length,
    [indicators]
  );

  const totalCount = indicators.length;
  const filteredCount = filteredIndicators.length;
  const totalPages = filteredCount ? Math.ceil(filteredCount / INDICATOR_PAGE_SIZE) : 1;
  const pageStartIndex = (currentPage - 1) * INDICATOR_PAGE_SIZE;
  const paginatedIndicators = filteredIndicators.slice(
    pageStartIndex,
    pageStartIndex + INDICATOR_PAGE_SIZE,
  );

  useEffect(() => {
    setCurrentPage(1);
  }, [trimmedSearchQuery, typeFilter, showEnabledOnly]);

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages || 1);
    }
  }, [currentPage, totalPages]);

  const indicatorSummary = useMemo(() => {
    if (!totalCount) return 'No indicators created yet.';
    if (!filteredCount) {
      if (showEnabledOnly && enabledCount === 0) return 'No enabled indicators found.';
      return 'No indicators match your filters yet.';
    }

    const pageStart = pageStartIndex + 1;
    const pageEnd = Math.min(pageStartIndex + INDICATOR_PAGE_SIZE, filteredCount);
    const pageSummary = `${pageStart}-${pageEnd} of ${filteredCount} matching ${filteredCount === 1 ? 'indicator' : 'indicators'}`;

    if (!showEnabledOnly && !trimmedSearchQuery && (typeFilter === 'all' || typeFilter === '')) {
      if (filteredCount === totalCount) return `${pageSummary} (out of ${totalCount} total)`;
      return `${pageSummary} (from ${totalCount} total)`;
    }

    return pageSummary;
  }, [
    totalCount,
    filteredCount,
    showEnabledOnly,
    enabledCount,
    pageStartIndex,
    trimmedSearchQuery,
    typeFilter,
  ]);

  const noIndicatorsMessage = useMemo(() => {
    if (!totalCount) return 'No indicators yet. Create one to get started.';
    if (showEnabledOnly && !trimmedSearchQuery && (typeFilter === 'all' || typeFilter === '')) {
      return 'No enabled indicators yet. Toggle the filter to view all indicators.';
    }
    return 'No indicators match your filters yet. Try adjusting the filters or search terms.';
  }, [totalCount, showEnabledOnly, trimmedSearchQuery, typeFilter]);

  if (!chartState || !chartId) return <div className="text-red-500">Error: No chart state found</div>

  return (
    <div className="space-y-6">
      {error && (
        <div className="relative rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-100 shadow-inner">
          <div className="pr-6">
            <p className="font-medium text-red-200">Request failed</p>
            <p className="mt-1 text-red-100">{error}</p>
          </div>
          <button
            type="button"
            onClick={() => setError(null)}
            className="absolute right-3 top-3 text-red-200/80 hover:text-red-100"
            aria-label="Dismiss error"
          >
            <X className="size-4" />
          </button>
        </div>
      )}

      {/* List of indicators */}
      <section className="relative rounded-2xl border border-white/10 bg-[#0d0d11]/80 shadow-inner shadow-black/30">
        <LoadingOverlay show={isLoading} message="Loading indicators…" />
        <div
          className={`flex flex-col gap-6 p-6 transition ${
            isLoading ? 'pointer-events-none select-none blur-sm opacity-40' : 'opacity-100'
          }`}
        >
          <header className="flex flex-col gap-4 border-b border-white/5 pb-4 md:flex-row md:items-start md:justify-between">
            <div className="space-y-1">
              <p className="text-[11px] uppercase tracking-[0.32em] text-slate-500">Indicators</p>
              <h3 className="text-base font-semibold text-slate-100">Manage overlay configurations</h3>
              <p className="text-xs text-slate-400">
                Review saved indicators, toggle their availability, and open edits without leaving the console.
              </p>
            </div>
            <div className="flex items-center gap-3">
              <button
                onClick={() => openEditModal()}
                className="inline-flex items-center gap-2 rounded-full border border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-20)] px-4 py-2 text-sm font-semibold text-[color:var(--accent-text-strong)] shadow-[0_12px_32px_-18px_var(--accent-shadow-strong)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-30)] hover:text-[color:var(--accent-text-bright)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]"
              >
                <Plus className="size-4" aria-hidden="true" />
                Create indicator
              </button>
            </div>
          </header>

          <div className="flex flex-col gap-3 rounded-xl border border-white/5 bg-white/5 p-3 text-xs md:flex-row md:items-center md:justify-between">
            <div className="flex flex-1 flex-wrap items-center gap-3">
              <span className="text-[11px] uppercase tracking-[0.28em] text-slate-500">Filters</span>
              <label className="inline-flex items-center gap-2 rounded-lg border border-white/10 bg-[#11131b] px-3 py-2 font-medium text-slate-200">
                <input
                  type="checkbox"
                  className="size-4 rounded border border-slate-600/80 bg-slate-900 accent-[color:var(--accent-base)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]"
                  checked={showEnabledOnly}
                  onChange={(event) => setShowEnabledOnly(event.target.checked)}
                />
                Show enabled only
              </label>

              <div className="flex items-center gap-2 rounded-lg border border-white/10 bg-[#11131b] px-3 py-2 text-slate-200">
                <span className="text-[11px] uppercase tracking-[0.24em] text-slate-500">Type</span>
                <select
                  value={typeFilter}
                  onChange={(event) => setTypeFilter(event.target.value)}
                  className="min-w-[8rem] rounded-md border border-white/10 bg-[#0d0f18] px-2 py-1 text-xs text-slate-200 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]"
                >
                  <option value="all">All types</option>
                  {typeOptions.map((type) => (
                    <option key={type} value={type}>
                      {formatIndicatorType(type)}
                    </option>
                  ))}
                </select>
              </div>

              <div className="flex min-w-[12rem] flex-1 items-center gap-2 rounded-lg border border-white/10 bg-[#11131b] px-3 py-2 text-slate-200 md:max-w-xs">
                <span className="text-[11px] uppercase tracking-[0.24em] text-slate-500">Search</span>
                <input
                  type="search"
                  value={searchQuery}
                  onChange={(event) => setSearchQuery(event.target.value)}
                  placeholder="Name, type, or param"
                  className="w-full bg-transparent text-xs text-slate-200 placeholder:text-slate-500 focus:outline-none"
                />
              </div>
            </div>
            <p className="text-xs text-slate-400">{indicatorSummary}</p>
          </div>

          <div className="space-y-4">
            {paginatedIndicators.map(indicator => {
              const isGenerating = isSignalsLoading && signalsLoadingFor === indicator.id
              const disableSignals = isSignalsLoading && signalsLoadingFor !== indicator.id
              return (
                <IndicatorCard
                  key={indicator.id}
                  indicator={indicator}
                  color={indColors[indicator.id] ?? DEFAULT_INDICATOR_COLOR}
                  onToggle={toggleEnable}
                  onEdit={openEditModal}
                  onDelete={handleDelete}
                  onGenerateSignals={generateSignals}
                  onSelectColor={handleSelectColor}
                  colorSwatches={COLOR_SWATCHES}
                  isGeneratingSignals={isGenerating}
                  disableSignalAction={disableSignals}
                />
              )
            })}

            {!isLoading && paginatedIndicators.length === 0 && (
              <div className="rounded-lg border border-dashed border-neutral-800/70 bg-neutral-900/40 px-4 py-6 text-center text-sm text-neutral-400">
                {noIndicatorsMessage}
              </div>
            )}
          </div>

          {filteredCount > INDICATOR_PAGE_SIZE && (
            <nav className="flex flex-col gap-2 rounded-lg border border-white/10 bg-[#11131b] px-4 py-3 text-xs text-slate-300 md:flex-row md:items-center md:justify-between" aria-label="Indicator pagination">
              <span>
                Page {currentPage} of {totalPages}
              </span>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => setCurrentPage((prev) => Math.max(prev - 1, 1))}
                  disabled={currentPage === 1}
                  className={`rounded-full border px-3 py-1 transition ${
                    currentPage === 1
                      ? 'cursor-not-allowed border-white/10 text-slate-500'
                      : 'border-white/15 text-slate-200 hover:border-[color:var(--accent-alpha-40)] hover:text-[color:var(--accent-text-strong)]'
                  }`}
                >
                  Previous
                </button>
                <div className="flex items-center gap-1">
                  {Array.from({ length: totalPages }).map((_, index) => {
                    const pageNumber = index + 1;
                    const isActive = pageNumber === currentPage;
                    return (
                      <button
                        key={pageNumber}
                        type="button"
                        onClick={() => setCurrentPage(pageNumber)}
                        className={`size-8 rounded-full border text-xs font-medium transition ${
                          isActive
                            ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)]'
                            : 'border-white/10 text-slate-300 hover:border-[color:var(--accent-alpha-40)] hover:text-[color:var(--accent-text-strong)]'
                        }`}
                        aria-current={isActive ? 'page' : undefined}
                      >
                        {pageNumber}
                      </button>
                    );
                  })}
                </div>
                <button
                  type="button"
                  onClick={() => setCurrentPage((prev) => Math.min(prev + 1, totalPages))}
                  disabled={currentPage === totalPages}
                  className={`rounded-full border px-3 py-1 transition ${
                    currentPage === totalPages
                      ? 'cursor-not-allowed border-white/10 text-slate-500'
                      : 'border-white/15 text-slate-200 hover:border-[color:var(--accent-alpha-40)] hover:text-[color:var(--accent-text-strong)]'
                  }`}
                >
                  Next
                </button>
              </div>
            </nav>
          )}
        </div>
      </section>

      <IndicatorModal
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        initial={editing}
        onSave={handleSave}
        error={error}
      />
    </div>
  )
}
