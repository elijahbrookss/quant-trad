import { useCallback, useEffect, useState } from 'react'

import { generateStrategySignals } from '../../adapters/strategy.adapter.js'

const useSignalGeneration = ({
  chartId,
  chartSnapshot,
  getChart,
  updateChart,
  selectedStrategy,
  selectedInstrumentIds,
  logger,
  onError,
} = {}) => {
  const [signalsLoading, setSignalsLoading] = useState(false)
  const [signalResult, setSignalResult] = useState(null)
  const [signalInstrumentId, setSignalInstrumentId] = useState(null)
  const [signalWindow, setSignalWindow] = useState(() => {
    const end = new Date()
    const start = new Date(end.getTime() - 7 * 24 * 60 * 60 * 1000)
    return { dateRange: [start, end] }
  })

  useEffect(() => {
    const chartRange = Array.isArray(chartSnapshot?.dateRange) ? chartSnapshot.dateRange : null
    setSignalWindow((prev) => {
      const hasValidRange = Array.isArray(prev.dateRange)
        && prev.dateRange[0] instanceof Date
        && !Number.isNaN(prev.dateRange[0]?.valueOf())
        && prev.dateRange[1] instanceof Date
        && !Number.isNaN(prev.dateRange[1]?.valueOf())

      if (!hasValidRange && Array.isArray(chartRange) && chartRange[0] instanceof Date && chartRange[1] instanceof Date) {
        return { ...prev, dateRange: chartRange }
      }
      return prev
    })
  }, [chartSnapshot?.dateRange])

  useEffect(() => {
    if (!selectedInstrumentIds.length) {
      setSignalInstrumentId(null)
      return
    }
    setSignalInstrumentId((prev) =>
      prev && selectedInstrumentIds.includes(prev) ? prev : selectedInstrumentIds[0],
    )
  }, [selectedInstrumentIds])

  useEffect(() => {
    setSignalResult(null)
  }, [selectedStrategy?.id])

  const runSignals = useCallback(
    async (window) => {
      if (!selectedStrategy) return
      const [startDate, endDate] = window.dateRange || []
      if (!(startDate instanceof Date) || Number.isNaN(startDate.valueOf()) || !(endDate instanceof Date) || Number.isNaN(endDate.valueOf())) {
        onError?.('A valid start and end date are required to generate signals.')
        return
      }
      if (!signalInstrumentId) {
        onError?.('Select an instrument to focus the preview.')
        return
      }
      if (!selectedInstrumentIds.includes(signalInstrumentId)) {
        onError?.('Selected instrument is not attached to this strategy.')
        return
      }
      setSignalsLoading(true)
      setSignalResult(null)
      onError?.(null)
      try {
        const interval = selectedStrategy.timeframe

        const result = await generateStrategySignals(selectedStrategy.id, {
          start: startDate.toISOString(),
          end: endDate.toISOString(),
          interval,
          instrument_ids: [signalInstrumentId],
        })
        setSignalResult(result)
        logger?.info?.('strategy_signals_generated', { strategyId: selectedStrategy.id })

        const instrumentResult = result?.instruments?.[signalInstrumentId]
        if (!instrumentResult?.window) {
          onError?.('Signal preview response is missing the window payload for the selected instrument.')
          return
        }

        const {
          instrument_id: resolvedInstrumentId,
          symbol: resolvedSymbol,
          interval: resolvedInterval,
          datasource: resolvedDatasource,
          exchange: resolvedExchange,
        } = instrumentResult.window

        if (!resolvedInstrumentId || resolvedInstrumentId !== signalInstrumentId) {
          onError?.('Signal preview response does not match the selected instrument.')
          return
        }
        if (!resolvedSymbol || !resolvedInterval || !resolvedDatasource) {
          onError?.('Signal preview response is missing symbol, interval, or datasource.')
          return
        }

        const buyMarkers = Array.isArray(instrumentResult?.chart_markers?.buy) ? instrumentResult.chart_markers.buy : []
        const sellMarkers = Array.isArray(instrumentResult?.chart_markers?.sell) ? instrumentResult.chart_markers.sell : []
        const combinedMarkers = [...buyMarkers, ...sellMarkers]

        const existing = (getChart(chartId)?.overlays || []).filter(Boolean)
        const overlays = existing
          .filter((overlay) => !(overlay && overlay.source === 'strategy'))
          .filter(Boolean)

        if (combinedMarkers.length) {
          overlays.push({
            id: `strategy-${selectedStrategy.id}-signals`,
            source: 'strategy',
            strategyId: selectedStrategy.id,
            type: 'strategy',
            payload: { markers: combinedMarkers },
          })
        }

        const appliedDateRange = Array.isArray(window.dateRange)
          && window.dateRange[0] instanceof Date
          && window.dateRange[1] instanceof Date
            ? window.dateRange
            : undefined

        updateChart(chartId, {
          overlays,
          symbol: resolvedSymbol,
          interval: resolvedInterval,
          datasource: resolvedDatasource,
          exchange: resolvedExchange,
          dateRange: appliedDateRange,
        })
      } catch (err) {
        onError?.(err?.message || 'Failed to generate signals')
        logger?.error?.('strategy_signals_failed', err)
      } finally {
        setSignalsLoading(false)
      }
    },
    [chartId, getChart, onError, selectedInstrumentIds, selectedStrategy, signalInstrumentId, updateChart, logger],
  )

  return {
    signalsLoading,
    signalResult,
    signalInstrumentId,
    setSignalInstrumentId,
    signalWindow,
    setSignalWindow,
    runSignals,
  }
}

export default useSignalGeneration
