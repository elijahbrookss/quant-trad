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

// File-level namespace.
const LOG_NS = 'ChartComponent';

export const ChartComponent = ({ chartId }) => {
  // Logger for this file.
  const { debug, info, warn, error } = useMemo(() => createLogger(LOG_NS), []);

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

  // Refs for chart and DOM.
  const chartContainerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const seededRef = useRef(false); // ensure we seed only once
  const pvMgrRef = useRef(null);

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
    });

    const series = chartRef.current.addSeries(CandlestickSeries, { ...seriesOptions })
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

    info('chart created', { chartId });

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
        info('chart removed', { chartId });
      } catch (e) {
        error('cleanup failed', e);
      }
    };
  }, [chartId, registerChart, updateChart, bumpRefresh, info, error]);

  // Resize via ResizeObserver.
  useEffect(() => {
    const el = chartContainerRef.current;
    if (!el || !chartRef.current) return;

    const ro = new ResizeObserver(([entry]) => {
      const r = entry?.contentRect; if (!r) return;
      chartRef.current.applyOptions({ width: r.width, height: r.height });
      debug('resize', { w: r.width, h: r.height });
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

  const applySymbol = (sym) => { setSymbol(sym); handleApply(); };
  // Data loader.
  const loadChartData = useCallback(async () => {
    try {
      setDataLoading(true);
      if (!symbol || !interval || !startISO || !endISO) {
        warn('missing inputs', { symbol, interval, startISO, endISO });
        return;
      }

      info('fetch', { symbol, interval, startISO, endISO });
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

      info('data set', {
        points: data.length,
        first: data[0]?.time,
        last: data.at(-1)?.time,
      });
    } catch (e) {
      error('load failed', e);
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
    const allSegments = [];
    const allPolylines = [];

    // 3) Walk overlays and apply.
    for (const ov of overlays) {
      const { type, payload, color } = ov || {};
      if (!payload) continue;

      const paneViews = getPaneViewsFor(type);
      const norm = adaptPayload(type, payload, color);

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

      // 3c) Touch points.
      if (paneViews.includes('touch') && norm.touchPoints?.length) {
        touchPoints.push(...norm.touchPoints.map(p => ({
          ...p,
          time: toSec(p.time),
        })));
      }

      // 3d) VA Boxes.
      if (paneViews.includes('va_box') && norm.boxes?.length) {
        boxes.push(
          ...norm.boxes.map(b => ({
            x1: toSec(b.x1),
            x2: toSec(b.x2),
            y1: Number(b.y1),
            y2: Number(b.y2),
            color: b.color,
            border: b.border,
          }))
        );
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
      // Touch points via custom pane-view.
      console.log("Setting touch points", touchPoints);
      pvMgrRef.current?.setTouchPoints(touchPoints);
      pvMgrRef.current?.setVABlocks(boxes);
      pvMgrRef.current?.setSegments(allSegments);
      pvMgrRef.current?.setPolylines(allPolylines);

      // seriesRef.current.setData(touch)
    } catch (e) {
      error('setMarkers failed', e);
    }

    // 6) Log summary for quick tracing.
    info('overlays applied', {
      priceLines: overlayHandlesRef.current.priceLines.length,
      markers: markers.length,
      touchPoints: touchPoints.length,
      boxes: boxes.length,
    });

    setDataLoading(false);
  }, [info, error]);

  // React to overlay changes.
  useEffect(() => {
    if (!chartState) return;
    syncOverlays(chartState.overlays || []);
  }, [chartState?.overlays, syncOverlays]);

  // Apply handler.
  const handleApply = useCallback(() => {
    info('apply', { chartId, symbol, interval, dateRange });
    syncOverlays([]); // clear overlays on apply
    updateChart?.(chartId, { symbol, interval, dateRange });
    loadChartData();
    bumpRefresh?.(chartId);
  }, [info, loadChartData, updateChart, bumpRefresh, chartId, symbol, interval, dateRange]);

  function useBusyDelay(busy, ms=250){
    const [show,setShow]=useState(false);
    useEffect(()=>{
      if(busy){ const t=setTimeout(()=>setShow(true), ms); return ()=>clearTimeout(t); }
      setShow(false);
    },[busy,ms]);
    return show;
  }

  return (
    <>

      {/* 90-day note */}
      <div className="mb-2">
        <span className="inline-flex items-center rounded-md bg-amber-500/10 px-2 py-1 text-xs font-medium text-amber-400 ring-1 ring-inset ring-amber-500/20">
          ⚠️Timeframe max is 90 days
        </span>
      </div>

      <div className="flex items-end space-x-4">
        <TimeframeSelect selected={interval} onChange={setInterval} />
        <SymbolInput value={symbol} onChange={setSymbol} />
        <DateRangePickerComponent dateRange={dateRange} setDateRange={setDateRange} />
        <button
          className="mt-5.5 self-center border border-neutral-600 rounded-md p-2 hover:bg-neutral-700 transition-colors cursor-pointer"
          onClick={handleApply}
        >
          <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
            <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182m0-4.991v4.99" />
          </svg>
        </button>
      </div>
      <div className="flex space-x-4 mt-5">
        <div className="relative flex-1 rounded-lg overflow-hidden bg-gray-800 h-[550px]">
          <div ref={chartContainerRef} className="h-full w-full bg-transparent" />
          <button
            type="button"
            onClick={() => setPalOpen(true)}
            className="h-9 px-3 rounded-md border border-neutral-600 bg-neutral-800/70 text-neutral-200 hover:bg-neutral-700"
            title="Open symbol presets (/)"
          >
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
    </>
  )
};
