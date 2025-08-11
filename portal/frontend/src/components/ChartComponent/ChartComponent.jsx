import { useState, useEffect, useRef, useLayoutEffect } from 'react'
import { createChart, CandlestickSeries, LineSeries } from 'lightweight-charts'
import { TimeframeSelect, SymbolInput } from './TimeframeSelectComponent'
import { DateRangePickerComponent } from './DateTimePickerComponent'
import { options, seriesOptions } from './ChartOptions'
import { fetchCandleData } from '../../adapters/candle.adapter'
import { useChartState } from '../../contexts/ChartStateContext.jsx'

export const ChartComponent = ({ chartId }) => {
  const { registerChart, updateChart, getChart, bumpRefresh } = useChartState()

  // Local state for inputs
  const [symbol, setSymbol] = useState('AAPL')
  const [interval, setInterval] = useState('1h')
  const [dateRange, setDateRange] = useState([
    (() => { const d = new Date(); d.setDate(d.getDate() - 45); return d })(),
    (() => { const d = new Date(); d.setMinutes(d.getMinutes() - 5); return d })(),
  ])
  // Local overlays state, synced from context
  const [overlays, setOverlays] = useState([])

  // Chart refs
  const chartContainerRef = useRef()
  const chartRef = useRef()
  const seriesRef = useRef()
  const overlaySeriesRefs = useRef({})

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
      console.log("[ChartComponent] Chart resized to", chartContainerRef.current.clientWidth)
    }
    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.remove()
      console.log("[ChartComponent] Chart unmounted")
    }
  }, [])

  // // Render overlays on chart
  // useEffect(() => {
  //   const chart = chartRef.current
  //   if (!chart) return
  //   // Remove series for overlays no longer present
  //   Object.keys(overlaySeriesRefs.current).forEach(id => {
  //     if (!overlays.find(o => o.id === id)) {
  //       overlaySeriesRefs.current[id].remove()
  //       delete overlaySeriesRefs.current[id]
  //       console.log(`[ChartComponent] Removed overlay series: ${id}`)
  //     }
  //   })
  //   // Add or update overlay series
  //   overlays.forEach(o => {
  //     let lineSeries = overlaySeriesRefs.current[o.id]
  //     if (!lineSeries) {
  //       lineSeries = chart.addSeries(LineSeries, { title: o.type })
  //       overlaySeriesRefs.current[o.id] = lineSeries
  //       console.log(`[ChartComponent] Added overlay series: ${o.id}`)
  //     }
  //     lineSeries.setData(o.data || [])
  //     console.log(`[ChartComponent] Updated overlay series: ${o.id} with ${o.data?.length || 0} points`)
  //   })
  // }, [overlays])

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
    updateChart(chartId, { symbol, interval, dateRange, overlays })
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
