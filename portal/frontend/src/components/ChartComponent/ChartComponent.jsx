import { useState, useEffect, useRef, useLayoutEffect } from 'react'
import { createChart, CandlestickSeries, createSeriesMarkers } from 'lightweight-charts'
import { TimeframeSelect, SymbolInput } from './TimeframeSelectComponent'
import { DateRangePickerComponent } from './DateTimePickerComponent'
import { options, seriesOptions } from './ChartOptions'
import { fetchCandleData } from '../../adapters/candle.adapter'
import { useChartState, useChartValue } from '../../contexts/ChartStateContext.jsx'

export const ChartComponent = ({ chartId }) => {
  const { registerChart, updateChart, getChart, bumpRefresh } = useChartState()
  const chartState = useChartValue(chartId)

  console.log("[ChartComponent] chartState from context:", chartState)

  // Local state for inputs
  const [symbol, setSymbol] = useState('CL')
  const [interval, setInterval] = useState('15m')
  const [dateRange, setDateRange] = useState([
    (() => { const d = new Date(); d.setDate(d.getDate() - 45); return d })(),
    (() => { const d = new Date(); d.setMinutes(d.getMinutes() - 5); return d })(),
  ])

  // Chart refs
  const chartContainerRef = useRef()
  const chartRef = useRef()
  const paneViewRef = useRef()
  const seriesRef = useRef()
  const overlayHandlesRef = useRef({ price_lines: [], markersApi: null }) // To hold multiple overlay series
  const touchSeriesRef = useRef()

  // Initialize chart and register context
  useLayoutEffect(() => {
    if (!chartId) {
      console.error('[ChartComponent] chartId prop is missing')
      return
    }
    registerChart(chartId, { symbol, interval, overlays: [], start: dateRange[0].toISOString(), end: dateRange[1].toISOString() })
    const chartState = getChart(chartId, "chartComponent-useLayoutEffect")
    console.log("[ChartComponent] Registered chart state:", chartState)
  }, [chartId])

  useEffect(() => {
    console.log("[ChartComponent] Mounting chart for", symbol, interval, dateRange)
    const chart = createChart(chartContainerRef.current, { ...options })
    chartRef.current = chart

    const series = chart.addSeries(CandlestickSeries, { ...seriesOptions })
    seriesRef.current = series

    const paneView = createTouchPaneView(chart.timeScale());
    paneViewRef.current = paneView;

    const touchSeries = chart.addCustomSeries(paneView, {});
    touchSeriesRef.current = touchSeries;

    loadChartData()
    chart.timeScale().fitContent()

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current.clientWidth })
    }
    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.remove()
      console.log("[ChartComponent] Chart unmounted")
    }
  }, [])

  useEffect(() => {
    if (chartState?.overlays) {
      console.log("[ChartComponent] overlays changed:", chartState.overlays)
      syncOverlays(chartState.overlays)
    }
  }, [chartState?.overlays])

  const loadChartData = async () => {
    console.log("[ChartComponent] Loading chart data for", symbol, interval, dateRange)
    try {
      const response = await fetchCandleData({
        symbol,
        timeframe: interval,
        start: dateRange[0].toISOString(),
        end: dateRange[1].toISOString(),
      })
      console.log("[ChartComponent] Fetched", response.length, "candles")

      const formatted = response
        .filter(c => c && typeof c.time === 'number')
        .map(c => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close }))

      if (seriesRef.current && formatted.length > 0) {
        seriesRef.current.setData(formatted)
        chartRef.current.timeScale().fitContent()
        console.log("[ChartComponent] Chart data set with", formatted.length, "points")
      } else {
        console.warn("[ChartComponent] No valid data to set on chart")
      }
    } catch (err) {
      console.error("[ChartComponent] Error loading chart data:", err)
    }
  }
  
  const syncOverlays = (overlays = []) => {
    console.log("[ChartComponent] Syncing overlays:", overlays)
    if (!seriesRef.current || !chartRef.current) return;
    overlayHandlesRef.current.price_lines.forEach(h => {
      try { seriesRef.current.removePriceLine(h) } catch {}
    })
    overlayHandlesRef.current.price_lines = []

    // 2) ensure markers plugin exists; clear old markers
    if (!overlayHandlesRef.current.markersApi) {
      overlayHandlesRef.current.markersApi = createSeriesMarkers(seriesRef.current, [])
    } else {
      overlayHandlesRef.current.markersApi.setMarkers([])
    }

    // 3) draw new overlays
    const touchPoints = [] // for touch series
    const allBarMarkers = [] // for standard markers
    overlays.forEach(({ type, payload }) => {
      console.log("[ChartComponent] Processing overlay:", type, payload)
      if (!payload) return

      if (Array.isArray(payload.price_lines)) {
        console.log("[ChartComponent] Creating price lines for overlay:", type, payload.price_lines)
        payload.price_lines.forEach(pl => {
          const handle = seriesRef.current.createPriceLine({
            price: pl.price,
            color: pl.color || undefined,
            lineWidth: pl.lineWidth ?? 1,
            lineStyle: pl.lineStyle ?? 0,
            axisLabelVisible: true,
            title: pl.title || type,
          })
          overlayHandlesRef.current.price_lines.push(handle)
        })
      }

      if (Array.isArray(payload.markers)) {
        for (const m of payload.markers) {
          if (m.subtype === 'touch' && typeof m.price === 'number') {
            // plot exact dots on TouchSeries only
            touchPoints.push({
              time: m.time,
              originalData: { price: m.price, color: m.color, size: 4 },
            });
          } else {
            // everything else goes to standard markers
            allBarMarkers.push({
              time: m.time,
              position: m.position ?? 'aboveBar',
              shape: m.shape ?? 'circle',
              color: m.color,
              text: m.text ?? '',
            });
          }
        }
      }
    });

    // Sort touch points by time to ensure correct rendering
    // ---- normalize, group, sort ----
    const toSec = v => (typeof v === 'number' ? (v > 2e10 ? Math.floor(v / 1000) : v) : v);

    // normalize times
    allBarMarkers.forEach(m => { m.time = toSec(m.time); });
    touchPoints.forEach(p => { p.time = toSec(p.time); });

    // group touch points by time (STRICTLY 1 item per time)
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

    // sort markers too (not strictly required, but tidy)
    allBarMarkers.sort((a, b) => a.time - b.time);


    console.log('[Touch] count:', touchPoints.length,
            'first:', touchPoints[0],
            'last:', touchPoints[touchPoints.length - 1]);
    console.log('[Touch] grouped entries:', grouped.size, 'touchData points:', touchData.length);
    console.log('[Markers] count:', allBarMarkers.length,
            'first:', allBarMarkers[0],
            'last:', allBarMarkers[allBarMarkers.length - 1]);


    const lastCandleTime = seriesRef.current?._data?._items?.at?.(-1)?.time ?? null;
    const lastClose = seriesRef.current?._data?._items?.at?.(-1)?.close ?? null;

    if (lastCandleTime != null && lastClose != null) {
      touchData.push({
        time: lastCandleTime,
        originalData: { points: [{ price: lastClose, color: '#ff00ff', size: 8 }] },
      });
      console.log('[TouchSeries] injected test dot at', lastCandleTime, 'price', lastClose);
    }

    // update series
    overlayHandlesRef.current.markersApi.setMarkers(allBarMarkers);
    if (touchSeriesRef.current) {
      console.log('[TouchSeries] calling setData len:', touchData.length, 'first:', touchData[0], 'last:', touchData.at(-1));

      const last = seriesRef.current?._data?._items?.at?.(-1);
      if (last) {
        touchData.push({
          time: last.time,
          originalData: { points: [{ price: last.close, color: '#ff00ff', size: 10 }] },
        });
      }

      paneViewRef.current?.setExternalData(touchData);
      touchSeriesRef.current.setData(touchData);
    }
  }

  const handlePrintState = () => {
    console.log("[ChartComponent] Print state button clicked")
    loadChartData()
    updateChart(chartId, { symbol, interval, dateRange })
    bumpRefresh(chartId)
    
    const chartState = getChart(chartId, "chartComponent-handlePrintState")
    console.log("[ChartComponent] Current chart state:", chartState)
  }
  
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
      <div className="flex items-end space-x-4">
        <TimeframeSelect selected={interval} onChange={setInterval} />
        <SymbolInput value={symbol} onChange={setSymbol} />
        <DateRangePickerComponent dateRange={dateRange} setDateRange={setDateRange} />
        <button
          className="mt-5.5 self-center border border-neutral-600 rounded-md p-2 hover:bg-neutral-700 transition-colors cursor-pointer"
          onClick={handlePrintState}
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
}
