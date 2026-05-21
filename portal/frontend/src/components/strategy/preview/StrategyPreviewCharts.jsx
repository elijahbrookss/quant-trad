import { Suspense, lazy, useEffect, useMemo, useState } from 'react'

import { fetchInstrumentCandles } from '../../../hooks/useInstrumentCandles.js'
import { createLogger } from '../../../utils/logger.js'
import { toSec } from '../../bots/chartDataUtils.js'
import { useChartState } from '../../../contexts/ChartStateContext.jsx'

const BotLensChart = lazy(() =>
  import('../../bots/BotLensChart.jsx').then((module) => ({ default: module.BotLensChart })),
)

export const StrategyPreviewCharts = ({
  strategy,
  instruments,
  previewInstrumentId = null,
  previewResult,
  focusRequest = null,
}) => {
  const [previewState, setPreviewState] = useState({})
  const logger = useMemo(() => createLogger('StrategyPreviewCharts'), [])
  const { getChart } = useChartState()
  const hasInstrumentsPayload = Boolean(previewResult?.instruments)

  const instrumentResults = useMemo(() => {
    if (!previewResult?.instruments || !instruments?.length || !previewInstrumentId) return []
    return instruments
      .filter((instrument) => instrument?.id)
      .filter((instrument) => instrument.id === previewInstrumentId)
      .map((instrument) => ({
        instrumentId: instrument.id,
        symbol: instrument.symbol,
        result: previewResult.instruments[instrument.id],
      }))
  }, [previewResult, instruments, previewInstrumentId])

  useEffect(() => {
    if (!instrumentResults.length || !strategy) return

    instrumentResults.forEach(({ instrumentId }) => {
      setPreviewState((prev) => ({
        ...prev,
        [instrumentId]: { loading: true, error: null, candles: [], overlays: [] },
      }))
    })

    const run = async () => {
      for (const entry of instrumentResults) {
        const { instrumentId, symbol, result } = entry
        if (!result?.window) {
          setPreviewState((prev) => ({
            ...prev,
            [instrumentId]: { loading: false, error: 'Preview window missing.', candles: [], overlays: [] },
          }))
          continue
        }
        const { start, end, interval, symbol: windowSymbol, datasource, exchange, instrument_id } = result.window

        try {
          if (!start || !end || !interval) {
            throw new Error('Preview window is incomplete.')
          }
          if (!instrument_id || instrument_id !== instrumentId) {
            throw new Error('Instrument mismatch for preview.')
          }
          if (symbol && windowSymbol && symbol !== windowSymbol) {
            throw new Error('Symbol mismatch for preview.')
          }
          const candleResult = await fetchInstrumentCandles({
            instrumentId: instrument_id,
            symbol: windowSymbol,
            timeframe: interval,
            start,
            end,
            datasource,
            exchange,
          })
          const candles = candleResult.candles
          logger.info('preview_candles_loaded', {
            instrumentId,
            symbol: windowSymbol,
            interval,
            start,
            end,
            datasource,
            exchange,
            candles: candles.length,
          })
          if (candles.length) {
            const sample = candles.slice(0, 3).map((item) => ({
              time: item?.time,
              type: typeof item?.time,
            }))
            const parsedTimes = candles
              .map((item) => toSec(item?.time))
              .filter((value) => Number.isFinite(value))
            logger.info('preview_candles_timecheck', {
              instrumentId,
              symbol: windowSymbol,
              interval,
              rawCount: candles.length,
              parsedCount: parsedTimes.length,
              uniqueTimes: new Set(parsedTimes).size,
              first: parsedTimes[0],
              last: parsedTimes[parsedTimes.length - 1],
              sample,
            })
          }
          if (!candles.length) {
            throw new Error('No candles returned for preview.')
          }
          if (!Array.isArray(result?.ui?.overlays)) {
            throw new Error('Preview overlays are missing.')
          }

          setPreviewState((prev) => ({
            ...prev,
            [instrumentId]: {
              loading: false,
              error: null,
              candles,
              overlays: result.ui.overlays,
            },
          }))
        } catch (err) {
          setPreviewState((prev) => ({
            ...prev,
            [instrumentId]: {
              loading: false,
              error: err?.message || 'Failed to load preview data.',
              candles: [],
              overlays: [],
            },
          }))
        }
      }
    }

    run()
  }, [instrumentResults, strategy, logger])

  useEffect(() => {
    if (!focusRequest || !Number.isFinite(focusRequest?.epoch)) return
    const targetInstrumentId = focusRequest.instrumentId || previewInstrumentId
    if (!targetInstrumentId || !strategy?.id) return
    const chartId = `strategy-preview-${strategy.id}-${targetInstrumentId}`
    const chart = getChart?.(chartId)
    const handles = chart?.handles
    const focusFn = handles?.focusAtTime
    if (typeof focusFn !== 'function') return
    focusFn(Number(focusRequest.epoch))
  }, [focusRequest, getChart, previewInstrumentId, strategy?.id])

  if (!previewResult) return null

  if (!hasInstrumentsPayload) {
    return (
      <div className="rounded-2xl border border-rose-500/30 bg-rose-500/10 p-4 text-sm text-rose-200">
        Strategy preview response is missing the multi-instrument payload. Update the backend to return
        <span className="mx-1 font-mono text-[11px] uppercase tracking-[0.2em] text-rose-100">
          instruments[&lt;instrument_id&gt;]
        </span>
        with <span className="mx-1 font-mono text-[11px] uppercase tracking-[0.2em] text-rose-100">window</span>
        and <span className="mx-1 font-mono text-[11px] uppercase tracking-[0.2em] text-rose-100">ui.overlays</span>
        for each instrument.
      </div>
    )
  }

  if (!instrumentResults.length) return null

  return (
    <div className="space-y-4">
      <div>
        <p className="text-xs uppercase tracking-[0.3em] text-slate-500">Preview charts</p>
        <p className="mt-1 text-sm text-slate-400">
          All preview visuals come through canonical overlays from the preview runtime.
        </p>
      </div>
      <div className="space-y-4">
        {instrumentResults.map(({ instrumentId, symbol }) => {
          const state = previewState[instrumentId] || {}
          return (
            <div key={`strategy-preview-${instrumentId}`} className="rounded-xl border border-white/10 bg-[#0f1524]/70 p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">Instrument</p>
                  <p className="text-base font-semibold text-white">{symbol || instrumentId}</p>
                </div>
                {state.loading ? <span className="text-xs text-slate-400">Loading…</span> : null}
              </div>
              {state.error ? (
                <div className="mt-3 rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-xs text-rose-200">
                  {state.error}
                </div>
              ) : state.candles?.length ? (
                <div className="mt-3">
                  <Suspense
                    fallback={
                      <div className="flex h-[320px] items-center justify-center rounded-xl border border-white/10 bg-black/20 text-xs text-slate-400">
                        Loading preview chart…
                      </div>
                    }
                  >
                    <BotLensChart
                      chartId={`strategy-preview-${strategy.id}-${instrumentId}`}
                      candles={state.candles}
                      trades={[]}
                      overlays={state.overlays || []}
                      playbackSpeed={0}
                      debugRanges
                      className="h-[320px] w-full"
                    />
                  </Suspense>
                </div>
              ) : null}
            </div>
          )
        })}
      </div>
    </div>
  )
}
