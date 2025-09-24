import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { createChart, CandlestickSeries, createSeriesMarkers} from 'lightweight-charts';
import { TimeframeSelect, SymbolInput } from './TimeframeSelectComponent';
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
import { useConnectionMonitor } from '../../hooks/useConnectionMonitor.js';

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
  const [connectionNotice, setConnectionNotice] = useState(null);

  const connection = useConnectionMonitor({ name: 'QuantLab API' });
  const {
    status: connectionStatus,
    message: connectionMessage,
    lastHeartbeat,
    markAttempt,
    markSuccess,
    markError,
  } = connection;

  const statusDescriptor = useMemo(() => {
    const base = {
      label: 'Standby',
      tone: 'text-slate-300',
      badge: 'border-slate-800/80 bg-slate-900/60 text-slate-200',
    };

    if (connectionStatus === 'online') {
      return {
        label: 'Online',
        tone: 'text-emerald-200',
        badge: 'border-emerald-400/40 bg-emerald-500/10 text-emerald-200',
      };
    }

    if (connectionStatus === 'connecting' || connectionStatus === 'recovering') {
      return {
        label: 'Syncing',
        tone: 'text-amber-200',
        badge: 'border-amber-400/40 bg-amber-500/10 text-amber-200',
      };
    }

    if (connectionStatus === 'error') {
      return {
        label: 'Alert',
        tone: 'text-rose-200',
        badge: 'border-rose-500/40 bg-rose-500/10 text-rose-200',
      };
    }

    return base;
  }, [connectionStatus]);

  const heartbeatLabel = useMemo(() => {
    if (!lastHeartbeat) return 'Awaiting heartbeat';
    try {
      return `Last check ${new Intl.DateTimeFormat(undefined, {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      }).format(lastHeartbeat)}`;
    } catch {
      return 'Heartbeat received';
    }
  }, [lastHeartbeat]);

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

  const loadChartData = useCallback(async ({ targetSymbol, targetInterval, targetRange } = {}) => {
    const effectiveSymbol = targetSymbol ?? symbol;
    const effectiveInterval = targetInterval ?? interval;
    const effectiveRange = targetRange ?? dateRange;
    const [startDate, endDate] = effectiveRange || [];
    const startISO = startDate?.toISOString();
    const endISO = endDate?.toISOString();

    try {
      setDataLoading(true);
      if (!effectiveSymbol || !effectiveInterval || !startISO || !endISO) {
        warn('chart_load_missing_inputs', { symbol: effectiveSymbol, interval: effectiveInterval, startISO, endISO });
        return;
      }

      markAttempt();
      info('candles_fetch_start', { symbol: effectiveSymbol, interval: effectiveInterval, startISO, endISO });
      const resp = await fetchCandleData({
        symbol: effectiveSymbol,
        timeframe: effectiveInterval,
        start: startISO,
        end: endISO,
      });

      if (!Array.isArray(resp) || resp.length === 0) {
        warn('no data', { symbol: effectiveSymbol, interval: effectiveInterval });
        markSuccess();
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

      seriesRef.current.setData(data);

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
      const first = data[0]?.time;
      const last = data.at(-1)?.time;
      if (chartRef.current && Number.isFinite(first) && Number.isFinite(last)) {
        const span = Math.max(1, last - first);
        const pad = Math.max(1, Math.floor(span * 0.05));
        chartRef.current.timeScale().setVisibleRange({ from: first - pad, to: last + pad });
      } else {
        chartRef.current?.timeScale().scrollToRealTime();
      }

      info('candles_fetch_success', {
        points: data.length,
        first: data[0]?.time,
        last: data.at(-1)?.time,
      });

      markSuccess();
      updateChart?.(chartId, {
        symbol: effectiveSymbol,
        interval: effectiveInterval,
        dateRange: effectiveRange,
        lastUpdatedAt: new Date().toISOString(),
      });
    } catch (e) {
      markError(e);
      error('candles_fetch_failed', e);
    } finally {
      setDataLoading(false);
    }
  }, [symbol, interval, dateRange, info, warn, error, markAttempt, markSuccess, markError, updateChart, chartId]);

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
    });
    seriesRef.current = series;

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

    const overlayHandles = overlayHandlesRef.current;

    return () => {
      try {
        overlayHandles?.priceLines?.forEach(h => {
          try {
            seriesRef.current?.removePriceLine(h);
          } catch {
            // ignore failures when price line already removed
          }
        });
        overlayHandles?.markersApi?.setMarkers?.([]);
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
  }, [chartId, registerChart, updateChart, bumpRefresh, info, error, loadChartData, symbol, interval, dateRange]);

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
    if (connectionStatus === 'error') {
      setConnectionNotice(connectionMessage);
    } else {
      setConnectionNotice(null);
    }
  }, [connectionStatus, connectionMessage]);

  useEffect(() => {
    updateChart?.(chartId, {
      connectionStatus,
      connectionMessage,
    });
  }, [chartId, connectionStatus, connectionMessage, updateChart]);

  useEffect(() => () => {
    if (timeframeWarningRef.current) {
      clearTimeout(timeframeWarningRef.current);
    }
  }, []);

  const applySymbol = (sym) => {
    setSymbol(sym);
    setPalOpen(false);
    handleApply({ symbol: sym });
  };

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
      try {
        seriesRef.current.removePriceLine(h);
      } catch {
        // ignore if price line already cleared
      }
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
  }, [chartState, syncOverlays]);

  // Apply handler.
  const handleApply = useCallback((overrides = {}) => {
    const nextSymbol = overrides.symbol ?? symbol;
    const nextInterval = overrides.interval ?? interval;
    const nextRange = overrides.dateRange ?? dateRange;
    const [start, end] = nextRange || [];
    const maxWindowMs = 90 * 24 * 60 * 60 * 1000;
    const windowMs = start && end ? Math.abs(end.getTime() - start.getTime()) : 0;
    if (windowMs > maxWindowMs) {
      warn('apply_blocked_range', { chartId, symbol: nextSymbol, interval: nextInterval, windowMs });
      setRangeWarning('Please choose a window of 90 days or less before applying.');
      if (timeframeWarningRef.current) clearTimeout(timeframeWarningRef.current);
      timeframeWarningRef.current = setTimeout(() => setRangeWarning(null), 5000);
      return;
    }

    setRangeWarning(null);
    info('apply', { chartId, symbol: nextSymbol, interval: nextInterval, dateRange: nextRange });
    syncOverlays([]); // clear overlays on apply
    updateChart?.(chartId, { symbol: nextSymbol, interval: nextInterval, dateRange: nextRange });
    loadChartData({ targetSymbol: nextSymbol, targetInterval: nextInterval, targetRange: nextRange });
    bumpRefresh?.(chartId);
  }, [info, loadChartData, updateChart, bumpRefresh, chartId, symbol, interval, dateRange, warn, syncOverlays]);

  function useBusyDelay(busy, ms=250){
    const [show,setShow]=useState(false);
    useEffect(()=>{
      if(busy){ const t=setTimeout(()=>setShow(true), ms); return ()=>clearTimeout(t); }
      setShow(false);
    },[busy,ms]);
    return show;
  }

  return (
    <div className="space-y-5">
      {connectionNotice && (
        <div className="flex items-center gap-2 rounded-2xl border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-100 shadow-lg shadow-rose-900/40">
          <span className="text-lg">⚠️</span>
          <div>
            <p className="font-semibold">Connection issue</p>
            <p className="text-xs text-rose-100/80">{connectionNotice}</p>
          </div>
        </div>
      )}

      {rangeWarning && (
        <div className="flex items-center gap-2 rounded-2xl border border-amber-400/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100 shadow-lg shadow-amber-900/40">
          <span className="text-lg">⚠️</span>
          <span className="font-medium">{rangeWarning}</span>
        </div>
      )}

      <div className="rounded-2xl border border-white/10 bg-[#111114]/70 p-5 shadow-[0_40px_80px_-60px_rgba(0,0,0,0.9)]">
        <div className="flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between">
          <div className="flex flex-col gap-4 lg:flex-row lg:flex-wrap lg:items-end">
            <TimeframeSelect selected={interval} onChange={setInterval} />
            <SymbolInput value={symbol} onChange={setSymbol} />
            <DateRangePickerComponent dateRange={dateRange} setDateRange={setDateRange} />
          </div>

          <div className="flex flex-col items-start gap-3 sm:items-end">
            <div className={`inline-flex items-center gap-2 rounded-full border px-3 py-1 text-[11px] uppercase tracking-[0.35em] ${statusDescriptor.badge}`}>
              <span
                className={`h-2 w-2 rounded-full ${
                  connectionStatus === 'online'
                    ? 'bg-emerald-400 shadow-[0_0_10px] shadow-emerald-400/80'
                    : connectionStatus === 'error'
                      ? 'bg-rose-400 shadow-[0_0_10px] shadow-rose-500/70'
                      : connectionStatus === 'connecting' || connectionStatus === 'recovering'
                        ? 'bg-amber-300 shadow-[0_0_10px] shadow-amber-400/70'
                        : 'bg-slate-500'
                }`}
              />
              <span className={`${statusDescriptor.tone}`}>{statusDescriptor.label}</span>
            </div>
            <p className="text-xs text-slate-400">
              {connectionStatus === 'error' ? connectionMessage : heartbeatLabel}
            </p>
            <div className="flex flex-wrap items-center gap-2">
              <button
                className="inline-flex items-center gap-2 rounded-full border border-purple-500/40 bg-purple-500/10 px-4 py-2 text-xs font-medium uppercase tracking-[0.3em] text-purple-200 transition hover:bg-purple-500/20"
                onClick={() => setPalOpen(true)}
                type="button"
                title="Open symbol palette (/ shortcut)"
              >
                <span>Open Presets</span>
                <kbd className="rounded border border-purple-400/40 bg-purple-500/10 px-1 text-[10px] text-purple-200">/</kbd>
              </button>
              <button
                className="inline-flex items-center gap-2 rounded-full border border-purple-400/60 bg-purple-500/20 px-4 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-purple-100 transition hover:bg-purple-500/30 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-purple-400"
                onClick={() => handleApply()}
                type="button"
                title="Fetch latest data"
                aria-label="Fetch latest data"
              >
                Refresh
              </button>
            </div>
          </div>
        </div>

        <div className="mt-5 rounded-2xl border border-white/5 bg-black/30 px-4 py-4">
          <SymbolPresets selected={symbol} onPick={applySymbol} />
        </div>
      </div>

      <div className="relative h-[560px] overflow-hidden rounded-3xl border border-white/10 bg-[#050507]/80 shadow-[0_50px_90px_-65px_rgba(0,0,0,0.9)]">
        <div ref={chartContainerRef} className="h-full w-full bg-transparent" />

        <SymbolPalette open={palOpen} onClose={() => setPalOpen(false)} onPick={applySymbol} />
        <HotkeyHint />
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
