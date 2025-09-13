import  { useState, useEffect, Fragment, useMemo } from 'react'
import { Switch, Popover, Transition, PopoverButton, PopoverPanel } from '@headlessui/react'
import {
  fetchIndicators,
  createIndicator,
  updateIndicator,
  deleteIndicator,
  fetchIndicatorOverlays,
} from '../adapters/indicator.adapter'
// import IndicatorModal from './IndicatorModal'
import IndicatorModalV2 from './IndicatorModal.v2.jsx'
const IndicatorModal = IndicatorModalV2; // for now, swap in new version under old name
import { useChartState } from '../contexts/ChartStateContext'
import IndicatorCard from './IndicatorCard.jsx';


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

const hexToRgba = (hex, a = 0.18) => {
  if (!hex || !hex.startsWith('#')) return `rgba(156,163,175,${a})`;
  const v = hex.slice(1);
  const n = v.length === 3
    ? v.split('').map(c => parseInt(c + c, 16))
    : [parseInt(v.slice(0,2),16), parseInt(v.slice(2,4),16), parseInt(v.slice(4,6),16)];
  return `rgba(${n[0]},${n[1]},${n[2]},${a})`;
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
    updateChart(chartId, { overlays: colored, overlayLoading: false });
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

      const boxes = Array.isArray(ov.payload.boxes)
        ? ov.payload.boxes.map(b => {
            if (!b) return b;
            return { ...b, color: hexToRgba(color, 0.1), border: { color: hexToRgba(color, 0.7), width: 1 } };
          })
        : ov.payload.boxes;

        const tintHex = hexToRgba(color, 0.7);

        if (Array.isArray(ov.payload.segments)) {
          ov.payload.segments = ov.payload.segments.map(s => ({ ...s, color: tintHex }));
        }
        if (Array.isArray(ov.payload.polylines)) {
          ov.payload.polylines = ov.payload.polylines.map(l => ({ ...l, color: tintHex }));
        }

      return { ...ov, payload: { ...ov.payload, price_lines, markers, boxes } };
    });

  const handleSelectColor = (indicatorId, color) => {
    setIndColors(prev => ({ ...prev, [indicatorId]: color }));
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
            <IndicatorCard
              key={indicator.id}
              indicator={indicator}
              color={indColors[indicator.id] || '#60a5fa'}
              onToggle={toggleEnable}
              onEdit={openEditModal}
              onDelete={handleDelete}
              onGenerateSignals={generateSignals}
              onSelectColor={handleSelectColor}
            />
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
