import { useState, useEffect, useRef, useLayoutEffect } from 'react'
import { createChart, CandlestickSeries, LineSeries } from 'lightweight-charts'
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
  const seriesRef = useRef()
  const overlayHandlesRef = useRef({ price_lines: [], hasMarkers: false }) // To hold multiple overlay series

  const syncOverlays = (overlays = []) => {
    console.log("[ChartComponent] Syncing overlays:", overlays)
    if (!seriesRef.current || !chartRef.current) return;
    overlayHandlesRef.current.price_lines.forEach(h => {
      try { seriesRef.current.removePriceLine(h) } catch {}
    })
    overlayHandlesRef.current.price_lines = []
    if (overlayHandlesRef.current.hasMarkers) {
      try { seriesRef.current.setMarkers([]) } catch {}
      overlayHandlesRef.current.hasMarkers = false
    }

    // 3) draw new overlays
    const markers = []
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

      // Example: markers
      if (Array.isArray(payload.markers)) {
        payload.markers.forEach(m => {
          markers.push({
            time: m.time,                // unix seconds expected by lightweight-charts
            position: m.position,        // 'aboveBar' | 'belowBar' | ...
            shape: m.shape,              // 'circle' | 'arrowUp' | ...
            color: m.color,
            text: m.text || type,
          })
        })
      }
    })
    if (markers.length) {
      seriesRef.current.setMarkers(markers)
      overlayHandlesRef.current.hasMarkers = true
    }
  }

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

  const handlePrintState = () => {
    console.log("[ChartComponent] Print state button clicked")
    loadChartData()
    updateChart(chartId, { symbol, interval, dateRange })
    bumpRefresh(chartId)
    
    const chartState = getChart(chartId, "chartComponent-handlePrintState")
    console.log("[ChartComponent] Current chart state:", chartState)
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
