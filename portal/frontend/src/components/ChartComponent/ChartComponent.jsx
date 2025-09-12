import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { createChart, CandlestickSeries, createSeriesMarkers} from 'lightweight-charts';
import { TimeframeSelect, SymbolInput } from './TimeframeSelectComponent';
import { DateRangePickerComponent } from './DateTimePickerComponent';
import { options, seriesOptions } from './ChartOptions';
import { fetchCandleData } from '../../adapters/candle.adapter';
import { useChartState, useChartValue } from '../../contexts/ChartStateContext.jsx';
import { createLogger } from '../../utils/logger.js';

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
  const [interval, setInterval] = useState('30m');
  const [dateRange, setDateRange] = useState([
    new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
    new Date()
  ]);

  // Refs for chart and DOM.
  const chartContainerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
    // Overlay resource handles.
  const overlayHandlesRef = useRef({ priceLines: [] });
  const touchSeriesRef = useRef()
  const touchPaneViewRef = useRef()


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

    // Add touch points series and pane view.
    touchPaneViewRef.current = createTouchPaneView(chartRef.current.timeScale());
    touchSeriesRef.current = chartRef.current.addCustomSeries(touchPaneViewRef.current, {});

    registerChart?.(chartId, {
      get chart() { return chartRef.current; },
      get series() { return seriesRef.current; }
    });

    // Seed chart slice and trigger first indicator fetch
    updateChart?.(chartId, { symbol, interval, dateRange });
    bumpRefresh?.(chartId);

    info('chart created', { chartId });

    return () => {
      try {
        chartRef.current?.remove();
        chartRef.current = null;
        seriesRef.current = null;
        info('chart removed', { chartId });
      } catch (e) {
        error('cleanup failed', e);
      }
    };
  }, [chartId, info, error, registerChart]);

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

  // Data loader.
  const loadChartData = useCallback(async () => {
    try {
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

      seriesRef.current.setData(data);
      chartRef.current?.timeScale().fitContent();

      info('data set', {
        points: data.length,
        first: data[0]?.time,
        last: data.at(-1)?.time,
      });
    } catch (e) {
      error('load failed', e);
    }
  }, [symbol, interval, startISO, endISO, info, warn, error]);

  // Overlay refs and syncer.
  const syncOverlays = useCallback((overlays = []) => {
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

    // 2) Build fresh markers and touch points.
    const markers = [];
    const touchPoints = [];

    // 3) Walk overlays and apply.
    for (const ov of overlays) {
      const { type, payload } = ov || {};
      if (!payload) continue;

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

      // 3b) Touch and markers.
      if (Array.isArray(payload.markers)) {
        for (const m of payload.markers) {
          const t = toSec(m.time);
          if (t == null) continue;
          if (m.subtype === 'touch' && typeof m.price === 'number') {
            touchPoints.push({
              time: t,
              originalData: {price: m.price, color: m.color, size: 4},
            });
          } else {
            markers.push({
              time: t,
              position: m.position ?? 'inBar',   // 'aboveBar' | 'belowBar' | 'inBar'
              shape: m.shape ?? 'circle',        // 'circle' | 'square' | 'arrowUp' | 'arrowDown'
              color: m.color ?? '#60a5fa',
              text: m.text ?? '',
            });
          }
        }
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

    // materialize series data: one row per time
    const touchData = [...grouped.entries()]
    .map(([time, points]) => ({
      time,
      originalData: { points },        // <-- multiple dots per bar
    }))
    .sort((a, b) => a.time - b.time);
    
    // 4) Sort markers for deterministic rendering.
    markers.sort((a, b) => a.time - b.time);

    // 5) Apply markers to the main series.
    try {
      // seriesRef.current.setMarkers(markers);
      overlayHandlesRef.current.markersApi.setMarkers(markers);
     
      // Apply touch points to the touch series via its pane view.
      touchPaneViewRef.current?.setExternalData(touchData);
      touchSeriesRef.current.setData(touchData);

      // seriesRef.current.setData(touch)
    } catch (e) {
      error('setMarkers failed', e);
    }

    // 6) Log summary for quick tracing.
    info('overlays applied', {
      priceLines: overlayHandlesRef.current.priceLines.length,
      markers: markers.length,
    });
  }, [info, error]);

  // React to overlay changes.
  useEffect(() => {
    if (!chartState) return;
    syncOverlays(chartState.overlays || []);
  }, [chartState?.overlays, syncOverlays]);

  // Apply handler.
  const handleApply = useCallback(() => {
    info('apply', { chartId, symbol, interval, dateRange });
    loadChartData();
    updateChart?.(chartId, { symbol, interval, dateRange });
    bumpRefresh?.(chartId);
  }, [info, loadChartData, updateChart, bumpRefresh, chartId, symbol, interval, dateRange]);

  // Initial load.
  useEffect(() => {
    if (chartRef.current && seriesRef.current) {
      info('initial load', { chartId });
      loadChartData();
    }
  }, [loadChartData, info, chartId]);

  function createTouchPaneView(tsApi) {
    let external = [];

    const renderer = {
      draw: (target, priceToCoordinate) => {
        const ctx = target.useMediaCoordinateSpace(({ context }) => context);
        ctx.save();

        const toSec = t => (typeof t === 'number' && t > 2e10 ? Math.floor(t / 1000) : t);

        for (const row of external) {
          const x = tsApi.timeToCoordinate(toSec(row.time)); // ⬅ use chart’s timeScale
          if (x == null) continue;

          const pts = row.originalData?.points || [];
          for (const pt of pts) {
            const y = priceToCoordinate(pt.price);
            if (y == null) continue;

            ctx.beginPath();
            ctx.arc(x, y, (pt.size ?? 5), 0, Math.PI * 2);
            ctx.fillStyle = pt.color ?? '#60a5fa';
            ctx.fill();
          }
        }

        ctx.restore();
      },
      drawBackground: () => {},
      hitTest: () => null,
    };

    return {
      renderer: () => renderer,
      update: () => {},                      // we don't rely on paneData anymore
      setExternalData: rows => { external = rows || []; },  // <-- we’ll call this
      priceValueBuilder: item => {
        const p = item.originalData?.points?.[0]?.price ?? 0;
        return [p, p, p];
      },
      isWhitespace: item => !(item.originalData?.points?.length),
      defaultOptions() {
        return { priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false };
      },
      destroy: () => {},
    };
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
        <div className="flex-1 rounded-lg overflow-hidden bg-gray-800 h-[400px]">
          <div ref={chartContainerRef} className="h-full w-full bg-transparent" />
        </div>
      </div>
    </>
  )
};
