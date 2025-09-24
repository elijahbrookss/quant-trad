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


  const { updateChart, getChart } = useChartState()

  const logger = useMemo(() => createLogger('IndicatorSection', { chartId }), [chartId])
  const { debug, info, warn, error: logError } = logger

  // Read current chart slice
  const chartState = getChart(chartId)

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
    const colored = applyIndicatorColors(overlaysPayload, indColors);
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

      // light validation for lookbacks
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
      if (meta.id) {
        result = await updateIndicator(meta.id, { type: meta.type, params, name: meta.name });
        setIndicators((prev) => {
          const next = prev.map((i) => (i.id === result.id ? result : i));
          queueMicrotask(() => { void refreshEnabledOverlays(next); });
          return next;
        });
      } else {
        result = await createIndicator({ type: meta.type, params, name: meta.name });
        setIndicators((prev) => {
          const next = [...prev, result];
          queueMicrotask(() => { void refreshEnabledOverlays(next); });
          return next;
        });
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
    setEditing(indicator)
    setModalOpen(true)
    setError(null)
  }

  const handleSelectColor = (indicatorId, color) => {
    setIndColors(prev => ({ ...prev, [indicatorId]: color }));
  };

  const isSignalsLoading = !!chartState?.signalsLoading
  const signalsLoadingFor = chartState?.signalsLoadingFor

  const filteredIndicators = useMemo(
    () => (showEnabledOnly ? indicators.filter((ind) => ind?.enabled) : indicators),
    [showEnabledOnly, indicators]
  );

  const enabledCount = useMemo(
    () => indicators.filter((indicator) => indicator?.enabled).length,
    [indicators]
  );

  const totalCount = indicators.length;
  const filteredCount = filteredIndicators.length;

  const indicatorSummary = useMemo(() => {
    if (!totalCount) return 'No indicators created yet.';
    if (showEnabledOnly) {
      return enabledCount
        ? `${enabledCount} enabled ${enabledCount === 1 ? 'indicator' : 'indicators'} visible`
        : 'No enabled indicators found.';
    }
    return `Showing ${filteredCount} of ${totalCount} indicators`;
  }, [enabledCount, filteredCount, showEnabledOnly, totalCount]);

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
      <section className="relative overflow-hidden rounded-2xl border border-white/10 bg-[#0d0d11]/80 shadow-inner shadow-black/30">
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
                className="inline-flex items-center gap-2 rounded-lg bg-sky-500/90 px-4 py-2 text-sm font-semibold text-slate-950 shadow-lg shadow-sky-500/20 transition hover:bg-sky-400 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-300"
              >
                <Plus className="size-4" aria-hidden="true" />
                Create indicator
              </button>
            </div>
          </header>

          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <div className="flex flex-wrap items-center gap-3 text-xs">
              <span className="text-[11px] uppercase tracking-[0.28em] text-slate-500">Filters</span>
              <label className="inline-flex items-center gap-2 rounded-lg border border-white/10 bg-white/5 px-3 py-2 font-medium text-slate-200">
                <input
                  type="checkbox"
                  className="size-4 rounded border border-slate-600/80 bg-slate-900 accent-sky-500 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400"
                  checked={showEnabledOnly}
                  onChange={(event) => setShowEnabledOnly(event.target.checked)}
                />
                Show enabled only
              </label>
            </div>
            <p className="text-xs text-slate-500">{indicatorSummary}</p>
          </div>

          <div className="space-y-4">
            {filteredIndicators.map(indicator => {
              const isGenerating = isSignalsLoading && signalsLoadingFor === indicator.id
              const disableSignals = isSignalsLoading && signalsLoadingFor !== indicator.id
              return (
                <IndicatorCard
                  key={indicator.id}
                  indicator={indicator}
                  color={indColors[indicator.id] || '#60a5fa'}
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

            {!isLoading && filteredIndicators.length === 0 && (
              <div className="rounded-lg border border-dashed border-neutral-800/70 bg-neutral-900/40 px-4 py-6 text-center text-sm text-neutral-400">
                {showEnabledOnly
                  ? 'No enabled indicators yet. Toggle the filter to view all indicators.'
                  : 'No indicators yet. Create one to get started.'}
              </div>
            )}
          </div>
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
