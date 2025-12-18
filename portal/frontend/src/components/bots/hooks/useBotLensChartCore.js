import { useEffect, useRef } from 'react'
import { createChart, CandlestickSeries, LineSeries } from 'lightweight-charts'
import { PaneViewManager } from '../../../chart/paneViews/factory.js'

export const useBotLensChartCore = ({
  chartId,
  containerRef,
  chartOptions,
  seriesOptions,
  registerChart,
  candleLookup,
  focusAtTime,
  pulseTrade,
  clearPulse,
  recenter,
  attachRangeGuards,
  markerCacheRef,
  markerDetailsRef,
  markerManager,
  chartRef: extChartRef,
  seriesRef: extSeriesRef,
  levelSeriesRef: extLevelSeriesRef,
  paneMgrRef: extPaneMgrRef,
  markersApiRef: extMarkersApiRef,
  overlayHandlesRef: extOverlayHandlesRef,
  barSpacingRef: extBarSpacingRef,
}) => {
  const chartRef = extChartRef ?? useRef(null)
  const seriesRef = extSeriesRef ?? useRef(null)
  const levelSeriesRef = extLevelSeriesRef ?? useRef(null)
  const paneMgrRef = extPaneMgrRef ?? useRef(null)
  const markersApiRef = extMarkersApiRef ?? useRef(null)
  const overlayHandlesRef = extOverlayHandlesRef ?? useRef({ priceLines: [] })
  const barSpacingRef = extBarSpacingRef ?? useRef(null)
  const resizeObserverRef = useRef(null)
  const focusTimeoutRef = useRef(null)

  useEffect(() => {
    const el = containerRef.current
    if (!el || chartRef.current) return undefined

    const chart = createChart(el, {
      ...chartOptions,
      width: el.clientWidth,
      height: el.clientHeight || 360,
    })
    const series = chart.addSeries(CandlestickSeries, seriesOptions)
    const levelSeries = chart.addSeries(LineSeries, {
      color: 'rgba(0,0,0,0)',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    })

    chartRef.current = chart
    seriesRef.current = series
    levelSeriesRef.current = levelSeries
    paneMgrRef.current = new PaneViewManager(chart)

    registerChart?.(chartId, {
      get chart() {
        return chartRef.current
      },
      get series() {
        return seriesRef.current
      },
      focusAtTime: (time, priceHint) => focusAtTime(time, priceHint, candleLookup),
      pulseTrade,
      clearPulse,
      zoomIn: () => chartRef.current?.timeScale?.().zoomIn?.(),
      zoomOut: () => chartRef.current?.timeScale?.().zoomOut?.(),
      centerView: recenter,
    })

    const cleanupGuards = attachRangeGuards(el)

    resizeObserverRef.current = new ResizeObserver(([entry]) => {
      const rect = entry?.contentRect
      if (!rect || !chartRef.current) return
      chartRef.current.applyOptions({ width: rect.width, height: rect.height })
    })
    resizeObserverRef.current.observe(el)

    return () => {
      cleanupGuards?.()
      resizeObserverRef.current?.disconnect()
      resizeObserverRef.current = null

      if (focusTimeoutRef.current) {
        clearTimeout(focusTimeoutRef.current)
        focusTimeoutRef.current = null
      }

      clearPulse?.()
      markerManager?.clearLayer('base')
      markerManager?.clearLayer('focus')
      markerManager?.clearLayer('pulse')
      markerManager?.flush?.()
      markersApiRef.current = null

      paneMgrRef.current?.destroy()
      paneMgrRef.current = null

      overlayHandlesRef.current.priceLines = []
      markerCacheRef.current = []
      markerDetailsRef.current = []

      if (levelSeriesRef.current) {
        try {
          chart.removeSeries(levelSeriesRef.current)
        } catch {
          /* ignore */
        }
      }
      levelSeriesRef.current = null
      seriesRef.current = null
      chartRef.current?.remove()
      chartRef.current = null
    }
  }, [
    attachRangeGuards,
    candleLookup,
    chartId,
    chartOptions,
    clearPulse,
    containerRef,
    focusAtTime,
    markerCacheRef,
    markerDetailsRef,
    recenter,
    registerChart,
    seriesOptions,
  ])

  return {
    chartRef,
    seriesRef,
    levelSeriesRef,
    paneMgrRef,
    markersApiRef,
    overlayHandlesRef,
    barSpacingRef,
  }
}

