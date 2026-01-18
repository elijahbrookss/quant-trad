import { useEffect, useMemo, useState } from 'react'
import { BotLensChart } from '../../bots/BotLensChart.jsx'
import { fetchInstrumentCandles } from '../../../hooks/useInstrumentCandles.js'
import { fetchIndicatorOverlays } from '../../../adapters/indicator.adapter.js'
import { createLogger } from '../../../utils/logger.js'
import { toSec } from '../../bots/chartDataUtils.js'

const buildSignalOverlay = ({ strategyId, instrumentId, markers }) => ({
  id: `strategy-${strategyId}-${instrumentId}-signals`,
  source: 'strategy',
  type: 'strategy',
  payload: { markers },
})

const buildIndicatorOverlay = ({ indicator, instrumentId, payload }) => ({
  id: `indicator-${indicator.id}-${instrumentId}`,
  source: 'indicator',
  type: indicator.type || 'indicator',
  color: indicator.color,
  payload,
})

export const SignalPreviewCharts = ({
  strategy,
  instruments,
  previewInstrumentId = null,
  signalResult,
  attachedIndicators,
}) => {
  const [previewState, setPreviewState] = useState({})
  const logger = useMemo(() => createLogger('SignalPreviewCharts'), [])
  const hasInstrumentsPayload = Boolean(signalResult?.instruments)

  const instrumentResults = useMemo(() => {
    if (!signalResult?.instruments || !instruments?.length || !previewInstrumentId) return []
    return instruments
      .filter((instrument) => instrument?.id)
      .filter((instrument) => instrument.id === previewInstrumentId)
      .map((instrument) => ({
        instrumentId: instrument.id,
        symbol: instrument.symbol,
        result: signalResult.instruments[instrument.id],
      }))
  }, [signalResult, instruments, previewInstrumentId])

  useEffect(() => {
    if (!instrumentResults.length || !strategy) return
    if (!Array.isArray(attachedIndicators)) {
      instrumentResults.forEach(({ instrumentId }) => {
        setPreviewState((prev) => ({
          ...prev,
          [instrumentId]: { loading: false, error: 'Indicator list missing for preview.', candles: [], overlays: [] },
        }))
      })
      return
    }

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
            [instrumentId]: { loading: false, error: 'Signal window missing.', candles: [], overlays: [] },
          }))
          continue
        }
        const { start, end, interval, symbol: windowSymbol, datasource, exchange, instrument_id } = result.window

        try {
          if (!start || !end || !interval) {
            throw new Error('Signal window is incomplete.')
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
            const sample = candles.slice(0, 3).map((entry) => ({
              time: entry?.time,
              type: typeof entry?.time,
            }))
            const parsedTimes = candles
              .map((entry) => toSec(entry?.time))
              .filter((value) => Number.isFinite(value))
            const uniqueTimes = new Set(parsedTimes)
            logger.info('preview_candles_timecheck', {
              instrumentId,
              symbol: windowSymbol,
              interval,
              rawCount: candles.length,
              parsedCount: parsedTimes.length,
              uniqueTimes: uniqueTimes.size,
              first: parsedTimes[0],
              last: parsedTimes[parsedTimes.length - 1],
              sample,
            })
          }
          if (!candles.length) {
            throw new Error('No candles returned for preview.')
          }

          const indicatorOverlays = []
          for (const indicator of attachedIndicators) {
            const overlayPayload = await fetchIndicatorOverlays(indicator.id, {
              start,
              end,
              interval,
              symbol: windowSymbol,
              datasource,
              exchange,
              instrument_id: instrumentId,
            })
            indicatorOverlays.push(buildIndicatorOverlay({ indicator, instrumentId, payload: overlayPayload }))
          }

          const buyMarkers = Array.isArray(result?.chart_markers?.buy) ? result.chart_markers.buy : []
          const sellMarkers = Array.isArray(result?.chart_markers?.sell) ? result.chart_markers.sell : []
          const signalOverlay = buildSignalOverlay({
            strategyId: strategy.id,
            instrumentId,
            markers: [...buyMarkers, ...sellMarkers],
          })

          setPreviewState((prev) => ({
            ...prev,
            [instrumentId]: {
              loading: false,
              error: null,
              candles,
              overlays: [...indicatorOverlays, signalOverlay],
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
  }, [attachedIndicators, instrumentResults, strategy])

  if (!signalResult) return null

  if (!hasInstrumentsPayload) {
    return (
      <div className="rounded-2xl border border-rose-500/30 bg-rose-500/10 p-4 text-sm text-rose-200">
        Signal preview response is missing the multi-instrument payload. Update the backend to return
        <span className="mx-1 font-mono text-[11px] uppercase tracking-[0.2em] text-rose-100">
          instruments[&lt;instrument_id&gt;]
        </span>
        with <span className="mx-1 font-mono text-[11px] uppercase tracking-[0.2em] text-rose-100">window</span>,
        <span className="mx-1 font-mono text-[11px] uppercase tracking-[0.2em] text-rose-100">chart_markers</span>,
        and <span className="mx-1 font-mono text-[11px] uppercase tracking-[0.2em] text-rose-100">applied_inputs</span>
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
          Indicators and signal markers for the evaluation window.
        </p>
      </div>
      <div className="space-y-4">
        {instrumentResults.map(({ instrumentId, symbol }) => {
          const state = previewState[instrumentId] || {}
          return (
            <div key={`signal-preview-${instrumentId}`} className="rounded-xl border border-white/10 bg-[#0f1524]/70 p-4">
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
                  <BotLensChart
                    chartId={`signal-preview-${strategy.id}-${instrumentId}`}
                    candles={state.candles}
                    trades={[]}
                    overlays={state.overlays || []}
                    playbackSpeed={0}
                    debugRanges
                    className="h-[320px] w-full"
                  />
                </div>
              ) : null}
            </div>
          )
        })}
      </div>
    </div>
  )
}
