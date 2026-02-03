import { useEffect, useRef } from 'react'
import { createChart, CandlestickSeries, LineSeries } from 'lightweight-charts'
import { PaneViewManager } from '../../../chart/paneViews/factory.js'
import { BOTLENS_DEBUG } from '../chartDataUtils.js'

export const useBotLensChartCore = ({
  chartId,
  containerRef,
  chartOptions,
  seriesOptions,
  registerChart,
  candleLookupRef,
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
  const creationSeqRef = useRef(0)
  const effectRunSeqRef = useRef(0)
  const lastDepsRef = useRef(null)

  useEffect(() => {
    effectRunSeqRef.current += 1
    const depSnapshot = {
      chartId,
      chartOptions,
      seriesOptions,
      registerChart,
      attachRangeGuards,
      recenter,
      clearPulse,
      containerRef,
      hasContainer: Boolean(containerRef?.current),
    }

    const prevSnapshot = lastDepsRef.current
    const changed = prevSnapshot
      ? Object.entries(depSnapshot)
          .filter(([key, value]) => prevSnapshot[key] !== value)
          .map(([key]) => key)
      : []
    const containerChanged = prevSnapshot?.hasContainer !== depSnapshot.hasContainer
    const reason = !prevSnapshot
      ? 'initial'
      : changed.length
        ? containerChanged && changed.length === 1
          ? 'container-changed'
          : 'deps-changed'
        : 'strict-reinvoke'

    if (BOTLENS_DEBUG) {
      console.info('[BotLensChartCore] effect run', {
        chartId,
        runId: effectRunSeqRef.current,
        reason,
        changed,
        hasContainer: depSnapshot.hasContainer,
        hasChart: Boolean(chartRef.current),
      })
    }

    lastDepsRef.current = depSnapshot

    const el = containerRef.current
    if (!el) {
      console.warn('[BotLensChartCore] no container, skipping chart creation', {
        chartId,
        runId: effectRunSeqRef.current,
      })
      return undefined
    }
    if (chartRef.current) return undefined

    creationSeqRef.current += 1
    
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
      focusAtTime: (time, priceHint) => focusAtTime(time, priceHint, candleLookupRef?.current),
      pulseTrade,
      clearPulse,
      zoomIn: () => chartRef.current?.timeScale?.().zoomIn?.(),
      zoomOut: () => chartRef.current?.timeScale?.().zoomOut?.(),
      centerView: recenter,
    }, {
      caller: 'useBotLensChartCore',
      lifecycleSeq: creationSeqRef.current,
      mountId: effectRunSeqRef.current,
    })

    if (BOTLENS_DEBUG) {
      console.info('[BotLensChartCore] chart created', {
        chartId,
        seq: creationSeqRef.current,
        runId: effectRunSeqRef.current,
      })
    }

    const cleanupGuards = attachRangeGuards(el)

    resizeObserverRef.current = new ResizeObserver(([entry]) => {
      const rect = entry?.contentRect
      if (!rect || !chartRef.current) return
      chartRef.current.applyOptions({ width: rect.width, height: rect.height })
    })
    resizeObserverRef.current.observe(el)

    return () => {
      if (BOTLENS_DEBUG) {
        console.info('[BotLensChartCore] chart cleanup', {
          chartId,
          seq: creationSeqRef.current,
          runId: effectRunSeqRef.current,
          hasContainer: Boolean(containerRef?.current),
          lastDepHasContainer: depSnapshot.hasContainer,
          lastRunReason: reason,
          changed,
        })
      }
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
  }, [chartId])

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
