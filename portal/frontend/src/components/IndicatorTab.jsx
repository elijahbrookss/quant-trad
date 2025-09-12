import  { useState, useEffect, Fragment, useMemo } from 'react'
import { Switch, Popover, Transition, PopoverButton, PopoverPanel } from '@headlessui/react'
import {
  fetchIndicators,
  createIndicator,
  updateIndicator,
  deleteIndicator,
  fetchIndicatorOverlays,
} from '../adapters/indicator.adapter'
import IndicatorModal from './IndicatorModal'
import { useChartState } from '../contexts/ChartStateContext'

const COLOR_SWATCHES = ['#60a5fa', '#f97316', '#22c55e', '#eab308', '#ef4444'];

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


  const { updateChart, getChart } = useChartState()

  // Read current chart slice
  const chartState = getChart(chartId)
  console.log('[IndicatorSection] chartId:', chartId, 'chartState:', chartState)

  // Derive ISO start/end from dateRange
  const [startISO, endISO] = useMemo(() => {
    const [s, e] = chartState?.dateRange || []
    const sISO = typeof s === 'string' ? s : s?.toISOString()
    const eISO = typeof e === 'string' ? e : e?.toISOString()
    return [sISO, eISO]
  }, [chartState?.dateRange?.[0], chartState?.dateRange?.[1]])

  useEffect(() => {
    if (!chartState || !chartState._version) {
      console.warn('[IndicatorSection] No chart state version yet, skipping fetch');
      setIsLoading(false);
      return;
    }
    if (!chartState.symbol || !chartState.interval) {
      console.warn('[IndicatorSection] Missing symbol/interval, skipping fetch');
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
          console.error('[IndicatorSection] Refresh failed:', e);
        }
      } finally {
        if (isMounted) setIsLoading(false);
      }
    })();

    return () => { isMounted = false; };
  }, [chartId, chartState?._version]);

  // Refresh overlays for enabled indicators
  // ensure enabled indicators carry current chart symbol/interval before fetching overlays
  // re-fetch indicators and ensure enabled indicators' params match current chart before overlays
  // patch enabled indicators to current chart symbol/interval, then compute overlays
  const refreshEnabledOverlays = async (list = indicators) => {
    console.log('[IndicatorSection - Overlays] Refresh start for chartId:', chartId);
    if (!chartState) return;

    // if list is empty/undefined, try one fetch to seed; otherwise use provided/current list
    let working = Array.isArray(list) && list.length ? list : indicators;
    if (!Array.isArray(working) || working.length === 0) {
      try {
        working = (await fetchIndicators({ symbol: chartState.symbol, interval: chartState.interval })) || [];
        setIndicators(working);
        updateChart(chartId, { indicators: working });
      } catch (e) {
        console.error('[IndicatorSection - Overlays] Failed to seed indicators:', e);
        updateChart(chartId, { overlays: [] });
        return;
      }
    }

    // patch params for enabled indicators if symbol/interval mismatch
    const enabled = working.filter(i => i?.enabled);
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
        console.warn('[IndicatorSection - Overlays] Param patch failed for', ind.id, e);
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
          return payload ? { ind_id: ind.id, type: ind.type, payload } : null;
        } catch (e) {
          const msg = String(e?.message ?? e);
          if (
            msg.includes('Indicator not found') ||
            msg.includes('No candles available') ||
            msg.includes('No overlays computed')
          ) {
            console.warn(`[IndicatorSection - Overlays] Skipping ${ind.id}: ${msg}`);
            return null;
          }
          console.error(`[IndicatorSection - Overlays] Overlay error for ${ind.id}:`, e);
          return null;
        }
      })
    );

    const overlaysPayload = results.filter(Boolean);
    const colored = applyIndicatorColors(overlaysPayload, indColors);
    updateChart(chartId, { overlays: colored });
    console.log('[IndicatorSection] Updated overlays (colored):', colored);
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
      console.error('[IndicatorSection] Error saving indicator:', e);
    }
  };

  const handleDelete = async (id) => {
    try {
      await deleteIndicator(id)
      setIndicators(prev => prev.filter(i => i.id !== id))
    } catch (e) {
      setError(e.message)
      console.error('[IndicatorSection] Error deleting indicator:', e)
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
    console.log('[IndicatorSection] generateSignals not yet implemented', id);
  };


  const openEditModal = (indicator = null) => {
    setEditing(indicator)
    setModalOpen(true)
    setError(null)
  }

    // apply selected colors to overlays' price_lines and markers
  const applyIndicatorColors = (overlays = [], colors = {}) =>
    (overlays || []).map(ov => {
      if (!ov || !ov.ind_id || !ov.payload) return ov;
      const color = colors[ov.ind_id];
      if (!color) return ov;

      // price lines → uniform color
      const price_lines = Array.isArray(ov.payload.price_lines)
        ? ov.payload.price_lines.map(pl => ({ ...pl, color }))
        : ov.payload.price_lines;

      // markers (touch + regular) → override color
      const markers = Array.isArray(ov.payload.markers)
        ? ov.payload.markers.map(m => (m ? { ...m, color } : m))
        : ov.payload.markers;

      return { ...ov, payload: { ...ov.payload, price_lines, markers } };
    });

  // set color, then recolor currently-displayed overlays in context
  const handleSelectColor = (indicatorId, color) => {
    setIndColors(prev => {
      const next = { ...prev, [indicatorId]: color };
      const overlays = (getChart(chartId)?.overlays) || [];
      const recolored = applyIndicatorColors(overlays, next);
      updateChart(chartId, { overlays: recolored });
      return next;
    });
  };

  if (isLoading) return <div>Loading indicators…</div>
  if (error) return <div className="text-red-500">Error: {error}</div>
  if (!chartState || !chartId) return <div className="text-red-500">Error: No chart state found</div>


  return (
    <div className="space-y-6">
      <button
        onClick={() => openEditModal()}
        className="flex flex-col items-center w-full px-4 py-3 rounded-lg bg-neutral-900 text-neutral-400 hover:text-neutral-100 shadow-lg cursor-pointer transition-colors"
      >
        {/* plus icon preserved */}
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6 mb-2">
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v6m3-3H9m12 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
        </svg>
        Create Indicator
      </button>

      {/* List of indicators */}
      <div className="space-y-1">
        {indicators.map(indicator => (
          <div key={indicator.id} className="flex items-center justify-between px-4 py-3 rounded-lg bg-neutral-900 shadow-lg">
          <div>
            <div className="flex items-center gap-2">
              <div className="font-medium text-white">{indicator.name}</div>

              {/* color selector popover */}
              <Popover className="relative cursor-pointer">
                {({ close }) => (
                  <>
                    <PopoverButton
                      className="h-4 w-4 rounded-sm border border-neutral-500 shadow-[inset_0_0_0_1px_rgba(255,255,255,.08)] cursor-pointer"
                      style={{ backgroundColor: indColors[indicator.id] || '#60a5fa' }}
                      title="Set color"
                    />
                    <Transition
                      enter="transition ease-out duration-100"
                      enterFrom="opacity-0 translate-y-1"
                      enterTo="opacity-100 translate-y-0"
                      leave="transition ease-in duration-75"
                      leaveFrom="opacity-100 translate-y-0"
                      leaveTo="opacity-0 translate-y-1"
                    >
                      <PopoverPanel className="absolute z-20 mt-2 rounded-md bg-neutral-800 p-2 shadow-lg ring-1 ring-black/20 cursor-pointer">
                        <div className="flex gap-2">
                          {COLOR_SWATCHES.map(c => (
                            <button
                              key={c}
                              className="h-5 w-5 rounded-sm border border-white/20 focus:outline-none focus:ring-2 focus:ring-white/40 cursor-pointer"
                              style={{ backgroundColor: c }}
                              onClick={() => { handleSelectColor(indicator.id, c); close(); }}
                              aria-label={`Set color ${c}`}
                            />
                          ))}
                        </div>
                      </PopoverPanel>
                    </Transition>
                  </>
                )}
              </Popover>
            </div>

            <div className="text-sm text-gray-500">{indicator.type}</div>
            <div className="text-xs text-gray-600 italic">
              Params: {Object.entries(indicator.params).map(([k, v]) => `${k}=${v}`).join(', ')}
            </div>
          </div>
            <div className="flex items-center gap-4">
              {/* Enable/Disable switch */}
              <Switch
                checked={indicator.enabled}
                onChange={() => toggleEnable(indicator.id)}
                className={`${indicator.enabled ? 'bg-indigo-500' : 'bg-gray-600'} relative inline-flex h-6 w-11 items-center rounded-full cursor-pointer`}
              >
                <span className={`${indicator.enabled ? 'translate-x-6' : 'translate-x-1'} inline-block h-4 w-4 transform rounded-full bg-white transition`} />
              </Switch>

              {/* Edit Button */}
              <button
                onClick={() => openEditModal(indicator)}
                className="text-gray-400 hover:text-white cursor-pointer transition-colors"
              >
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                  <path strokeLinecap="round" strokeLinejoin="round" d="m16.862 4.487 1.687-1.688a1.875 1.875 0 1 1 2.652 2.652L10.582 16.07a4.5 4.5 0 0 1-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 0 1 1.13-1.897l8.932-8.931Zm0 0L19.5 7.125M18 14v4.75A2.25 2.25 0 0 1 15.75 21H5.25A2.25 2.25 0 0 1 3 18.75V8.25A2.25 2.25 0 0 1 5.25 6H10" />
                </svg>
              </button>

              {/* Generate Signals */}
              <button
                  onClick={() => generateSignals(indicator.id)}
                  className="text-green-400 hover:text-green-200 cursor-pointer transition-colors"
              >
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
                    <path strokeLinecap="round" strokeLinejoin="round" d="M15.91 11.672a.375.375 0 0 1 0 .656l-5.603 3.113a.375.375 0 0 1-.557-.328V8.887c0-.286.307-.466.557-.327l5.603 3.112Z" />
                </svg>
              </button>

              {/* Delete Button with confirmation */}
            <Popover className="relative">
              {({ close }) => (
                <>
                  {/* Delete trigger */}
                  <PopoverButton
                    className="text-red-400 hover:text-red-200 cursor-pointer transition-colors"
                    title="Delete"
                  >
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                      <path strokeLinecap="round" strokeLinejoin="round" d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0" />
                    </svg>
                  </PopoverButton>

                  {/* Tiny confirmation tooltip */}
                  <Transition
                    as={Fragment}
                    enter="transition ease-out duration-100"
                    enterFrom="opacity-0 scale-95"
                    enterTo="opacity-100 scale-100"
                    leave="transition ease-in duration-75"
                    leaveFrom="opacity-100 scale-100"
                    leaveTo="opacity-0 scale-95"
                  >
                    <PopoverPanel
                      className="absolute z-50 -top-2 right-0 -translate-y-full
                                rounded-md border border-neutral-700 bg-neutral-900
                                shadow-xl p-1"
                    >
                      <div className="flex items-center gap-1">
                        {/* Confirm */}
                        <button
                          aria-label="Confirm delete"
                          onClick={() => { handleDelete(indicator.id); close(); }}
                          className="p-1 rounded hover:bg-green-600/20 text-green-400 hover:text-green-300"
                        >
                          {/* check icon */}
                          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"
                              fill="none" stroke="currentColor" strokeWidth="1.8"
                              className="size-5">
                            <path strokeLinecap="round" strokeLinejoin="round"
                                  d="M4.5 12.75l6 6 9-13.5"/>
                          </svg>
                        </button>

                        {/* Cancel (just closes) */}
                        <PopoverButton
                          aria-label="Cancel"
                          className="p-1 rounded hover:bg-neutral-700 text-neutral-300 hover:text-white"
                        >
                          {/* x icon */}
                          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"
                              fill="none" stroke="currentColor" strokeWidth="1.8"
                              className="size-5">
                            <path strokeLinecap="round" strokeLinejoin="round"
                                  d="M6 18L18 6M6 6l12 12"/>
                          </svg>
                        </PopoverButton>
                      </div>

                      {/* little caret */}
                      <div className="absolute -bottom-1 right-3 w-2 h-2 bg-neutral-900
                                      border-b border-r border-neutral-700 rotate-45"/>
                    </PopoverPanel>
                  </Transition>
                </>
              )}
            </Popover>


            </div>
          </div>
        ))}
      </div>

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
