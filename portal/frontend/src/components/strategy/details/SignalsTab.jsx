import { Button } from '../../ui'
import { SignalSummary } from '../signals'
import { SignalPreviewCharts } from '../signals/SignalPreviewCharts.jsx'

/**
 * Get display name for an instrument - prefers base_currency from metadata
 */
const getInstrumentDisplay = (instrument) => {
  if (!instrument) return ''

  // Try to get base_currency from instrument metadata
  const baseCurrency = instrument?.metadata?.instrument_fields?.base_currency || instrument?.base_currency
  if (baseCurrency) return baseCurrency

  // Fallback to symbol
  const symbol = instrument?.symbol || ''
  if (symbol.length <= 8) return symbol
  return symbol.slice(0, 6) + '…'
}

/**
 * Date range preset buttons
 */
const DATE_PRESETS = [
  { label: '7d', days: 7 },
  { label: '30d', days: 30 },
  { label: '90d', days: 90 },
  { label: '6mo', days: 180 },
  { label: '1y', days: 365 },
]

/**
 * Signals tab for generating and previewing strategy signals.
 */
export const SignalsTab = ({
  strategy,
  instruments = [],
  attachedIndicators,
  signalWindow,
  signalsLoading,
  signalResult,
  signalInstrumentId,
  onInstrumentChange,
  onSubmit,
  onDateRangeChange,
  DateRangePickerComponent,
}) => {
  const activeInstrument = instruments.find((instrument) => instrument?.id === signalInstrumentId) || null
  const symbol = activeInstrument?.symbol || '—'
  const interval = strategy.timeframe || '—'
  const datasource = activeInstrument?.datasource || '—'
  const exchange = activeInstrument?.exchange || '—'

  // Handle preset date range selection
  const handlePresetClick = (days) => {
    const end = new Date()
    const start = new Date()
    start.setDate(start.getDate() - days)
    // DateRangePickerComponent expects [startDate, endDate] array format
    onDateRangeChange([start, end])
  }

  // Get signal counts for mini summary
  const buyCount = signalResult?.buy_signals?.length || signalResult?.summary?.buy_count || 0
  const sellCount = signalResult?.sell_signals?.length || signalResult?.summary?.sell_count || 0

  return (
    <div className="space-y-4">
      {/* Mini signal summary - shown if we have results */}
      {signalResult && (
        <div className="flex items-center gap-4 rounded-lg border border-white/[0.06] bg-white/[0.02] px-4 py-2">
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-emerald-500" />
            <span className="text-sm font-medium text-white">{buyCount}</span>
            <span className="text-xs text-slate-500">buy signals</span>
          </div>
          <div className="h-4 w-px bg-white/10" />
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-rose-500" />
            <span className="text-sm font-medium text-white">{sellCount}</span>
            <span className="text-xs text-slate-500">sell signals</span>
          </div>
          {signalResult?.summary?.rules_matched !== undefined && (
            <>
              <div className="h-4 w-px bg-white/10" />
              <span className="text-xs text-slate-400">
                {signalResult.summary.rules_matched}/{strategy.rules?.length || 0} rules matched
              </span>
            </>
          )}
        </div>
      )}

      <form onSubmit={onSubmit} className="space-y-3">
        {/* Date range with presets */}
        <div className="rounded-lg border border-white/[0.06] bg-white/[0.02] p-3">
          <div className="mb-2 flex items-center justify-between">
            <span className="text-[10px] uppercase tracking-wider text-slate-500">Date Range</span>
            <div className="flex gap-1">
              {DATE_PRESETS.map((preset) => (
                <button
                  key={preset.label}
                  type="button"
                  onClick={() => handlePresetClick(preset.days)}
                  className="rounded px-2 py-0.5 text-[10px] font-medium text-slate-400 transition hover:bg-white/5 hover:text-white"
                >
                  {preset.label}
                </button>
              ))}
            </div>
          </div>
          <DateRangePickerComponent
            dateRange={signalWindow.dateRange}
            setDateRange={onDateRangeChange}
          />
        </div>

        {/* Instrument selector - compact */}
        <div className="rounded-lg border border-white/[0.06] bg-white/[0.02] p-3">
          <span className="mb-2 block text-[10px] uppercase tracking-wider text-slate-500">Instrument</span>
          <div className="flex flex-wrap gap-1.5">
            {instruments.length ? (
              instruments.map((instrument) => (
                <button
                  key={`signal-instrument-${instrument?.id || instrument?.symbol}`}
                  type="button"
                  onClick={() => onInstrumentChange(instrument?.id)}
                  className={`rounded px-2.5 py-1 text-xs transition ${
                    instrument?.id === signalInstrumentId
                      ? 'bg-[color:var(--accent-alpha-20)] text-white'
                      : 'bg-white/[0.04] text-slate-400 hover:bg-white/[0.08] hover:text-white'
                  }`}
                  title={instrument?.symbol}
                >
                  {getInstrumentDisplay(instrument) || 'Instrument'}
                </button>
              ))
            ) : (
              <p className="text-xs text-slate-500">No instruments configured</p>
            )}
          </div>
        </div>

        {/* Compact config summary */}
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 rounded-lg border border-white/[0.06] bg-white/[0.02] px-3 py-2 text-xs">
          <div className="flex items-center gap-1.5">
            <span className="text-slate-500">Symbol:</span>
            <span className="text-white" title={activeInstrument?.symbol}>{getInstrumentDisplay(activeInstrument) || symbol}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="text-slate-500">Interval:</span>
            <span className="text-white">{interval}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="text-slate-500">Source:</span>
            <span className="text-white">{datasource}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="text-slate-500">Exchange:</span>
            <span className="text-white">{exchange}</span>
          </div>
        </div>

        {/* Generate button */}
        <div className="flex justify-end">
          <Button
            type="submit"
            disabled={signalsLoading || !signalInstrumentId}
            loading={signalsLoading}
          >
            {signalsLoading ? 'Generating...' : 'Generate Signals'}
          </Button>
        </div>
      </form>

      {/* Signal results */}
      {signalResult && (
        <div className="space-y-4">
          <SignalSummary result={signalResult} instrumentId={signalInstrumentId} />

          {!signalResult?.instruments && (
            <div className="rounded-lg border border-rose-500/20 bg-rose-500/10 px-3 py-2 text-xs text-rose-200">
              Signal preview payload is missing instrument data for multi-instrument previews.
            </div>
          )}

          <SignalPreviewCharts
            strategy={strategy}
            instruments={instruments}
            previewInstrumentId={signalInstrumentId}
            signalResult={signalResult}
            attachedIndicators={attachedIndicators}
          />
        </div>
      )}
    </div>
  )
}
