import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { createChart, CandlestickSeries, createSeriesMarkers} from 'lightweight-charts';
import { TimeframeSelect } from './TimeframeSelectComponent';
import { DateRangePickerComponent } from './DateTimePickerComponent';
import { options, seriesOptions } from './ChartOptions';
import { fetchCandleData } from '../../adapters/candle.adapter';
import { useChartState, useChartValue } from '../../contexts/ChartStateContext.jsx';
import { createLogger } from '../../utils/logger.js';
import { PaneViewManager } from '../../chart/paneViews/factory.js';
import { adaptPayload, getPaneViewsFor } from '../../chart/indicators/registry.js';
import LoadingOverlay from '../LoadingOverlay.jsx';
import SymbolPresets from './SymbolPresets.jsx';
import HotkeyHint from '../HotkeyHint.jsx';
import SymbolPalette from '../SymbolPalette.jsx';

// File-level namespace.
const LOG_NS = 'ChartComponent';

const deriveTimeScaleOptions = (rawInterval) => {
  const interval = (rawInterval || '').toString().toLowerCase();
  const base = { timeVisible: true, secondsVisible: false };

  if (!interval) return base;

  if (interval.endsWith('s')) {
    return { ...base, secondsVisible: true };
  }

  if (interval.endsWith('m')) {
    return base;
  }

  if (interval.endsWith('h')) {
    return base;
  }

  if (interval.endsWith('d')) {
    return { timeVisible: false, secondsVisible: false };
  }

  if (interval.endsWith('w') || interval.endsWith('mo') || interval.endsWith('y')) {
    return { timeVisible: false, secondsVisible: false };
  }

  return base;
};

export const ChartComponent = ({ chartId }) => {
  // Logger for this file.
  const logger = useMemo(() => createLogger(LOG_NS, { chartId }), [chartId]);
  const { debug, info, warn, error } = logger;

  // Context wiring.
  const { registerChart, updateChart, bumpRefresh } = useChartState();
  const chartState = useChartValue(chartId);

  // Local UI state.
  const [symbol, setSymbol] = useState('CL');
  const [interval, setInterval] = useState('15m');
  const [palOpen, setPalOpen] = useState(false);
  const [dateRange, setDateRange] = useState([
    new Date(Date.now() - 90 * 24 * 60 * 60 * 1000),
    new Date()
  ]);
  const [dataLoading, setDataLoading] = useState(false);
  const [rangeWarning, setRangeWarning] = useState(null);

  // Refs for chart and DOM.
  const chartContainerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const seededRef = useRef(false); // ensure we seed only once
  const pvMgrRef = useRef(null);
  const lastBarRef = useRef(null);
  const barSpacingRef = useRef(null);
  const timeframeWarningRef = useRef(null);

    // Overlay resource handles.
  const overlayHandlesRef = useRef({ priceLines: [] });

  // Derive ISO once per range change.
  const [startISO, endISO] = useMemo(() => {
    const [s, e] = dateRange || [];
    return [s?.toISOString(), e?.toISOString()];
  }, [dateRange?.[0]?.getTime(), dateRange?.[1]?.getTime()]);

  // Create chart once.
  useEffect(() => {
    const el = chartContainerRef.current;
    if (!el || chartRef.current) return;

    chartRef.current = createChart(el, {
      ...options,
      width: el.clientWidth,
      height: el.clientHeight || 400,
      timeScale: deriveTimeScaleOptions(interval),
    });

    const series = chartRef.current.addSeries(CandlestickSeries, {
      ...seriesOptions,
      priceScaleId: 'right',
    })
    seriesRef.current = series

    // Create pane view manager.
    pvMgrRef.current = new PaneViewManager(chartRef.current);


    registerChart?.(chartId, {
      get chart() { return chartRef.current; },
      get series() { return seriesRef.current; }
    });

    loadChartData();

    if (!seededRef.current) {
      updateChart?.(chartId, { symbol, interval, dateRange });
      bumpRefresh?.(chartId); // trigger initial indicator load
      seededRef.current = true;
    }

    info('chart_created');

    return () => {
      try {
        overlayHandlesRef.current?.priceLines?.forEach(h => {
          try { seriesRef.current?.removePriceLine(h); } catch {}
        });
        overlayHandlesRef.current?.markersApi?.setMarkers?.([]);
        pvMgrRef.current?.destroy();
        pvMgrRef.current = null;
        chartRef.current?.remove();
        chartRef.current = null;
        seriesRef.current = null;
        info('chart_removed');
      } catch (e) {
        error('cleanup failed', e);
      }
    };
  }, [chartId, registerChart, updateChart, bumpRefresh, info, error]);

  useEffect(() => {
    if (!chartRef.current) return;
    const scaleOpts = deriveTimeScaleOptions(interval);
    chartRef.current.applyOptions({ timeScale: scaleOpts });
    debug('time_scale_updated', {
      interval,
      timeVisible: scaleOpts.timeVisible,
      secondsVisible: scaleOpts.secondsVisible,
    });
  }, [interval, debug]);

  // Resize via ResizeObserver.
  useEffect(() => {
    const el = chartContainerRef.current;
    if (!el || !chartRef.current) return;

    const ro = new ResizeObserver(([entry]) => {
      const r = entry?.contentRect; if (!r) return;
      chartRef.current.applyOptions({ width: r.width, height: r.height });
      debug('chart_resize', { width: r.width, height: r.height });
    });

    ro.observe(el);
    return () => ro.disconnect();
  }, [debug]);

  useEffect(() => { 
    const onKey = (e) => {
      if (e.key === '/' && !e.metaKey && !e.ctrlKey && !e.altKey) {
        const el = e.target;
        const tag = (el?.tagName || '').toLowerCase();
        const editable = el?.isContentEditable || tag === 'input' || tag === 'textarea';
        if (!editable) { e.preventDefault(); setPalOpen(true); }
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  useEffect(() => {
    const openPalette = () => setPalOpen(true);
    window.addEventListener('qt-open-symbol-palette', openPalette);
    return () => window.removeEventListener('qt-open-symbol-palette', openPalette);
  }, []);

  useEffect(() => () => {
    if (timeframeWarningRef.current) {
      clearTimeout(timeframeWarningRef.current);
    }
  }, []);

  const applySymbol = (sym) => { setSymbol(sym); setPalOpen(false); handleApply(); };
  // Data loader.
  const loadChartData = useCallback(async () => {
    try {
      setDataLoading(true);
      if (!symbol || !interval || !startISO || !endISO) {
        warn('chart_load_missing_inputs', { symbol, interval, startISO, endISO });
        return;
      }

      info('candles_fetch_start', { symbol, interval, startISO, endISO });
      const resp = await fetchCandleData({
        symbol,
        timeframe: interval,
        start: startISO,
        end: endISO,
      });

      if (!Array.isArray(resp) || resp.length === 0) {
        warn('no data', { symbol, interval });
        return;
      }

      const data = resp
        .filter(c => c && typeof c.time === 'number')
        .map(c => ({
          time: c.time,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        }));

      if (!seriesRef.current) {
        warn('series missing');
        return;
      }

      // seriesRef.current.setData(data);
      // chartRef.current?.timeScale().fitContent();
      
      seriesRef.current.setData(data);

      // Remember last bar for real-time updates.
      lastBarRef.current = data.at(-1);

      if (data.length > 1) {
        let minStep = Infinity;
        for (let i = 1; i < data.length; i += 1) {
          const step = data[i].time - data[i - 1].time;
          if (Number.isFinite(step) && step > 0 && step < minStep) {
            minStep = step;
          }
        }
        barSpacingRef.current = Number.isFinite(minStep) && minStep > 0 ? minStep : null;
      } else {
        barSpacingRef.current = null;
      }

      pvMgrRef.current?.updateVABlockContext({
        lastSeriesTime: lastBarRef.current?.time,
        barSpacing: barSpacingRef.current,
      });
      // move view to the loaded window; add small padding for context
      const first = data[0]?.time;
      const last  = data.at(-1)?.time;
      if (chartRef.current && Number.isFinite(first) && Number.isFinite(last)) {
        const span = Math.max(1, last - first);
        const pad  = Math.max(1, Math.floor(span * 0.05));
        chartRef.current.timeScale().setVisibleRange({ from: first - pad, to: last + pad });
      } else {
        chartRef.current?.timeScale().scrollToRealTime(); // fallback to latest
      }

      info('candles_fetch_success', {
        points: data.length,
        first: data[0]?.time,
        last: data.at(-1)?.time,
      });
    } catch (e) {
      error('candles_fetch_failed', e);
    } finally {
      setDataLoading(false);
    }
  }, [symbol, interval, startISO, endISO, info, warn, error]);


  // Overlay refs and syncer.
  const syncOverlays = useCallback((overlays = []) => {
    setDataLoading(true);
    // Guard on required refs.
    if (!seriesRef.current || !chartRef.current) return;

    // Helper: normalize time to seconds.
    const toSec = (t) => {
      if (t == null) return t;
      if (typeof t !== 'number') return t;
      return t > 2e10 ? Math.floor(t / 1000) : t; 
    };

    // 1) Clear existing price lines.
    overlayHandlesRef.current.priceLines.forEach(h => {
      try { seriesRef.current.removePriceLine(h); } catch {}
    });
    overlayHandlesRef.current.priceLines = [];

    // Ensure markers plugin exists; clear existing markers.
    if (!overlayHandlesRef.current.markersApi) {
      overlayHandlesRef.current.markersApi = createSeriesMarkers(seriesRef.current, []);
    } else {
      overlayHandlesRef.current.markersApi.setMarkers([]);
    }

    pvMgrRef.current?.clearFrame();

    // 2) Build fresh markers and touch points.
    const markers = [];
    const touchPoints = [];
    const boxes = [];
    const signalBubbles = [];
    const allSegments = [];
    const allPolylines = [];

    // 3) Walk overlays and apply.
    for (const ov of overlays) {
      const { type, payload, color, ind_id: indicatorId } = ov || {};
      if (!payload) continue;

      const overlayLogger = logger.child({ indicatorId, indicatorType: type });
      overlayLogger.debug('overlay_payload_received', {
        priceLines: Array.isArray(payload.price_lines) ? payload.price_lines.length : 0,
        markers: Array.isArray(payload.markers) ? payload.markers.length : 0,
        boxes: Array.isArray(payload.boxes) ? payload.boxes.length : 0,
        segments: Array.isArray(payload.segments) ? payload.segments.length : 0,
        polylines: Array.isArray(payload.polylines) ? payload.polylines.length : 0,
      });

      const paneViews = getPaneViewsFor(type);
      const norm = adaptPayload(type, payload, color);
      overlayLogger.debug('overlay_adapted', {
        priceLines: Array.isArray(norm.priceLines) ? norm.priceLines.length : 0,
        markers: Array.isArray(norm.markers) ? norm.markers.length : 0,
        touchPoints: Array.isArray(norm.touchPoints) ? norm.touchPoints.length : 0,
        boxes: Array.isArray(norm.boxes) ? norm.boxes.length : 0,
        segments: Array.isArray(norm.segments) ? norm.segments.length : 0,
        polylines: Array.isArray(norm.polylines) ? norm.polylines.length : 0,
        bubbles: Array.isArray(norm.bubbles) ? norm.bubbles.length : 0,
      });

      // 3a) Price lines.
      if (Array.isArray(payload.price_lines)) {
        for (const pl of payload.price_lines) {
          const handle = seriesRef.current.createPriceLine({
            price: pl.price,
            color: pl.color ?? undefined,
            lineWidth: pl.lineWidth ?? 1,
            lineStyle: pl.lineStyle ?? 0,
            axisLabelVisible: pl.axisLabelVisible ?? true,
            title: pl.title ?? type ?? '',
          });
          overlayHandlesRef.current.priceLines.push(handle);
        }
      }

      // 3b) Markers.
      markers.push(...norm.markers);

      if (Array.isArray(norm.bubbles) && norm.bubbles.length) {
        signalBubbles.push(...norm.bubbles.map(b => ({
          ...b,
          accentColor: b.accentColor ?? color,
        })));
      }

      // 3c) Touch points.
      if (paneViews.includes('touch') && norm.touchPoints?.length) {
        touchPoints.push(...norm.touchPoints.map(p => ({
          ...p,
          time: toSec(p.time),
        })));
      }

      // 3d) VA Boxes.
      if (paneViews.includes('va_box') && norm.boxes?.length) {
        const lastCandleSec = toSec(lastBarRef.current?.time);
        const normalizedBoxes = norm.boxes.map((b, idxInGroup) => {
          const x1 = toSec(b.x1);
          const requestedX2 = toSec(b.x2);
          const x2 = Number.isFinite(lastCandleSec) ? lastCandleSec : requestedX2;

          if (Number.isFinite(lastCandleSec) && lastCandleSec !== requestedX2) {
            overlayLogger.debug('va_box_span_adjusted', {
              boxIndex: boxes.length + idxInGroup,
              x1,
              originalX2: requestedX2,
              forcedX2: x2,
              lastCandle: lastCandleSec,
            });
          }

          return {
            x1,
            x2,
            y1: Number(b.y1),
            y2: Number(b.y2),
            color: b.color,
            border: b.border,
          };
        });
        boxes.push(...normalizedBoxes);
        normalizedBoxes.forEach((b, idx) => {
          const width = Number.isFinite(b.x2) && Number.isFinite(b.x1)
            ? Number(b.x2) - Number(b.x1)
            : null;
          overlayLogger.debug('va_box_applied', {
            boxIndex: idx,
            x1: b.x1,
            x2: b.x2,
            y1: b.y1,
            y2: b.y2,
            width,
          });
        });
      }

      if (paneViews.includes('segment') && norm.segments?.length) {
        allSegments.push(...norm.segments);
      }
      if (paneViews.includes('polyline') && norm.polylines?.length) {
        allPolylines.push(...norm.polylines);
      }
    }

    // Group touch points by time, strictly 1 item per time.
    const grouped = new Map();
    for (const p of touchPoints) {
      if (p.time == null || Number.isNaN(p.time)) continue;
      if (!grouped.has(p.time)) grouped.set(p.time, []);
      grouped.get(p.time).push({
        price:  p.originalData?.price ?? p.price,
        color:  p.originalData?.color ?? p.color,
        size:   (p.originalData?.size ?? p.size ?? 3),
      });
    }
    
    // 4) Sort markers for deterministic rendering.
    markers.sort((a, b) => a.time - b.time);

    // 5) Apply markers to the main series.
    try {
      // seriesRef.current.setMarkers(markers);
      overlayHandlesRef.current.markersApi.setMarkers(markers);
      

      pvMgrRef.current?.setTouchPoints(touchPoints);
      pvMgrRef.current?.setVABlocks(boxes, {
        lastSeriesTime: lastBarRef.current?.time,
        barSpacing: barSpacingRef.current,
      });
      pvMgrRef.current?.setSegments(allSegments);
      pvMgrRef.current?.setPolylines(allPolylines);
      pvMgrRef.current?.setSignalBubbles(signalBubbles);

      // --- C: VWAP vs Candles coverage + coordinate check ---
      // seriesRef.current.setData(touch)
    } catch (e) {
      error('overlays_apply_failed', e);
    }

    // 6) Log summary for quick tracing.
    info('overlays_applied', {
      priceLines: overlayHandlesRef.current.priceLines.length,
      markers: markers.length,
      touchPoints: touchPoints.length,
      boxes: boxes.length,
      bubbles: signalBubbles.length,
      segments: allSegments.length,
      polylines: allPolylines.length,
    });

    setDataLoading(false);
  }, [info, error, logger]);

  // React to overlay changes.
  useEffect(() => {
    if (!chartState) return;
    syncOverlays(chartState.overlays || []);
  }, [chartState?.overlays, syncOverlays]);

  // Apply handler.
  const handleApply = useCallback(() => {
    const [start, end] = dateRange || [];
    const maxWindowMs = 90 * 24 * 60 * 60 * 1000;
    const windowMs = start && end ? Math.abs(end.getTime() - start.getTime()) : 0;
    if (windowMs > maxWindowMs) {
      warn('apply_blocked_range', { chartId, symbol, interval, windowMs });
      setRangeWarning('Please choose a window of 90 days or less before applying.');
      if (timeframeWarningRef.current) clearTimeout(timeframeWarningRef.current);
      timeframeWarningRef.current = setTimeout(() => setRangeWarning(null), 5000);
      return;
    }

    setRangeWarning(null);
    info('apply', { chartId, symbol, interval, dateRange });
    syncOverlays([]); // clear overlays on apply
    updateChart?.(chartId, { symbol, interval, dateRange });
    loadChartData();
    bumpRefresh?.(chartId);
  }, [info, loadChartData, updateChart, bumpRefresh, chartId, symbol, interval, dateRange, warn]);

  function useBusyDelay(busy, ms=250){
    const [show,setShow]=useState(false);
    useEffect(()=>{
      if(busy){ const t=setTimeout(()=>setShow(true), ms); return ()=>clearTimeout(t); }
      setShow(false);
    },[busy,ms]);
    return show;
  }

  const surfaceClass = 'rounded-3xl border border-neutral-800 bg-neutral-950/80 px-6 py-6 shadow-[0_24px_60px_-40px_rgba(0,0,0,0.9)] backdrop-blur';

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-col gap-2">
        <h2 className="text-2xl font-semibold text-neutral-100">Market snapshot</h2>
        <p className="max-w-2xl text-sm text-neutral-400">
          Adjust the timeframe, symbol, and window to plan your next move.
        </p>
      </div>

      {rangeWarning && (
        <div className="flex items-center gap-2 rounded-2xl border border-amber-500/40 bg-amber-500/15 px-4 py-3 text-sm text-amber-200">
          <span aria-hidden className="text-lg">⚠️</span>
          <span className="font-medium">{rangeWarning}</span>
        </div>
      )}

      <div className={surfaceClass}>
        <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-4 sm:flex-row sm:flex-wrap sm:items-end">
              <TimeframeSelect selected={interval} onChange={setInterval} />
              <div className="flex min-w-[10rem] flex-col gap-2">
                <span className="text-[11px] uppercase tracking-[0.24em] text-neutral-500">Symbol</span>
                <button
                  type="button"
                  onClick={() => setPalOpen(true)}
                  className="inline-flex w-full items-center justify-between rounded-lg border border-neutral-700 bg-neutral-900 px-3 py-2 text-sm font-semibold text-neutral-200 transition hover:border-neutral-500 hover:text-neutral-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-neutral-500"
                >
                  <span className="uppercase tracking-wide">{symbol}</span>
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth={1.5}
                    className="h-4 w-4 text-neutral-500"
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M16.862 4.487 19.5 7.125M4.5 19.5l2.569-.428a2 2 0 0 0 1.093-.554L19.5 7.125a1.875 1.875 0 1 0-2.652-2.652L5.51 16.366a2 2 0 0 0-.554 1.093L4.5 19.5Z" />
                  </svg>
                </button>
              </div>
              <DateRangePickerComponent dateRange={dateRange} setDateRange={setDateRange} />
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-left text-xs text-neutral-500">
              <div className="font-semibold uppercase tracking-[0.24em] text-neutral-400">Refresh</div>
              <div className="text-neutral-400">Apply your latest settings</div>
            </div>
            <button
              className="inline-flex h-11 w-11 items-center justify-center rounded-full border border-neutral-700 bg-neutral-900 text-neutral-300 shadow-sm transition hover:border-neutral-500 hover:text-neutral-100 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-neutral-500"
              onClick={handleApply}
              type="button"
              title="Fetch latest data"
              aria-label="Fetch latest data"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth={1.5}
                className="h-5 w-5"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  d="M4.5 12a7.5 7.5 0 0 1 12.618-5.303M19.5 12a7.5 7.5 0 0 1-12.618 5.303M8.25 8.25h-3v-3M15.75 15.75h3v3"
                />
              </svg>
            </button>
          </div>
        </div>
      </div>

      <div className={`${surfaceClass} relative h-[560px] overflow-hidden px-0 py-0`}>
        <div ref={chartContainerRef} className="h-full w-full rounded-[28px] bg-neutral-950" />
        <button
          type="button"
          onClick={() => setPalOpen(true)}
          className="group absolute left-6 top-6 inline-flex items-center gap-2 rounded-full border border-neutral-700 bg-neutral-900/80 px-4 py-2 text-xs font-semibold uppercase tracking-[0.24em] text-neutral-300 shadow-sm transition hover:border-neutral-500 hover:text-neutral-100"
          title="Open symbol presets (/)"
        >
          <span className="h-1.5 w-1.5 rounded-full bg-neutral-500 transition group-hover:bg-neutral-200" />
          Presets
        </button>

        <SymbolPalette open={palOpen} onClose={() => setPalOpen(false)} onPick={applySymbol} />
        <HotkeyHint />
        {/* overlay */}
        <LoadingOverlay
          show={useBusyDelay(chartState?.overlayLoading || chartState?.signalsLoading || dataLoading)}
          message={
            chartState?.signalsLoading ? 'Generating signals…'
            : chartState?.overlayLoading ? 'Loading overlays…'
            : 'Loading chart…'
          }
        />
      </div>
    </div>
  )
};
