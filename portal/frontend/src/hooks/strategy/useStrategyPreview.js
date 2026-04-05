import { useCallback, useEffect, useState } from 'react'

import { runStrategyPreview } from '../../adapters/strategy.adapter.js'

const useStrategyPreview = ({
  chartId,
  chartSnapshot,
  updateChart,
  selectedStrategy,
  selectedInstrumentIds,
  logger,
  onError,
} = {}) => {
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewResult, setPreviewResult] = useState(null)
  const [previewInstrumentId, setPreviewInstrumentId] = useState(null)
  const [previewWindow, setPreviewWindow] = useState(() => {
    const end = new Date()
    const start = new Date(end.getTime() - 7 * 24 * 60 * 60 * 1000)
    return { dateRange: [start, end] }
  })

  useEffect(() => {
    const chartRange = Array.isArray(chartSnapshot?.dateRange) ? chartSnapshot.dateRange : null
    setPreviewWindow((prev) => {
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
      setPreviewInstrumentId(null)
      return
    }
    setPreviewInstrumentId((prev) =>
      prev && selectedInstrumentIds.includes(prev) ? prev : selectedInstrumentIds[0],
    )
  }, [selectedInstrumentIds])

  useEffect(() => {
    setPreviewResult(null)
  }, [selectedStrategy?.id])

  const runPreview = useCallback(
    async (window) => {
      if (!selectedStrategy) return
      const [startDate, endDate] = window.dateRange || []
      if (!(startDate instanceof Date) || Number.isNaN(startDate.valueOf()) || !(endDate instanceof Date) || Number.isNaN(endDate.valueOf())) {
        onError?.('A valid start and end date are required to run preview.')
        return
      }
      if (!previewInstrumentId) {
        onError?.('Select an instrument to focus the preview.')
        return
      }
      if (!selectedInstrumentIds.includes(previewInstrumentId)) {
        onError?.('Selected instrument is not attached to this strategy.')
        return
      }
      setPreviewLoading(true)
      setPreviewResult(null)
      onError?.(null)
      try {
        const interval = selectedStrategy.timeframe

        const result = await runStrategyPreview(selectedStrategy.id, {
          start: startDate.toISOString(),
          end: endDate.toISOString(),
          interval,
          instrument_ids: [previewInstrumentId],
        })
        setPreviewResult(result)
        logger?.info?.('strategy_preview_generated', { strategyId: selectedStrategy.id })

        const instrumentResult = result?.instruments?.[previewInstrumentId]
        if (!instrumentResult?.window) {
          onError?.('Strategy preview response is missing the window payload for the selected instrument.')
          return
        }

        const {
          instrument_id: resolvedInstrumentId,
          symbol: resolvedSymbol,
          interval: resolvedInterval,
          datasource: resolvedDatasource,
          exchange: resolvedExchange,
        } = instrumentResult.window

        if (!resolvedInstrumentId || resolvedInstrumentId !== previewInstrumentId) {
          onError?.('Strategy preview response does not match the selected instrument.')
          return
        }
        if (!resolvedSymbol || !resolvedInterval || !resolvedDatasource) {
          onError?.('Strategy preview response is missing symbol, interval, or datasource.')
          return
        }

        const overlays = Array.isArray(instrumentResult?.ui?.overlays)
          ? instrumentResult.ui.overlays
          : Array.isArray(instrumentResult?.overlays)
            ? instrumentResult.overlays
            : []

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
        onError?.(err?.message || 'Failed to run strategy preview')
        logger?.error?.('strategy_preview_failed', err)
      } finally {
        setPreviewLoading(false)
      }
    },
    [chartId, onError, selectedInstrumentIds, selectedStrategy, previewInstrumentId, updateChart, logger],
  )

  return {
    previewLoading,
    previewResult,
    previewInstrumentId,
    setPreviewInstrumentId,
    previewWindow,
    setPreviewWindow,
    runPreview,
  }
}

export default useStrategyPreview
