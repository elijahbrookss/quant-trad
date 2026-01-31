import { Button } from '../../ui'
import { SignalSummary } from '../signals'
import { SignalPreviewCharts } from '../signals/SignalPreviewCharts.jsx'
import { buildTriggerRows } from '../utils/orderTriggers.js'

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
 * Order Triggers tab for previewing when a strategy would attempt orders.
 */
export const OrderTriggersTab = ({
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
  onNavigateToRules,
  onNavigateToExecution,
}) => {
  const activeInstrument = instruments.find((instrument) => instrument?.id === signalInstrumentId) || null
  const symbol = activeInstrument?.symbol || '—'
  const interval = strategy.timeframe || '—'
  const datasource = activeInstrument?.datasource || '—'
  const exchange = activeInstrument?.exchange || '—'
  const instrumentResult = signalResult?.instruments?.[signalInstrumentId] || null

  // Handle preset date range selection
  const handlePresetClick = (days) => {
    const end = new Date()
    const start = new Date()
    start.setDate(start.getDate() - days)
    // DateRangePickerComponent expects [startDate, endDate] array format
    onDateRangeChange([start, end])
  }

  // Get signal counts for mini summary
  const countSignals = (entries = []) => entries.reduce((total, entry) => {
    if (!entry) return total
    if (Array.isArray(entry.signals) && entry.signals.length) {
      return total + entry.signals.length
    }
    return total + (entry.matched ? 1 : 0)
  }, 0)
  const buyCount = instrumentResult
    ? countSignals(instrumentResult.buy_signals || [])
    : signalResult?.summary?.buy_count || 0
  const sellCount = instrumentResult
    ? countSignals(instrumentResult.sell_signals || [])
    : signalResult?.summary?.sell_count || 0
  const rulesMatched = instrumentResult
    ? (instrumentResult.rule_results || []).filter((entry) => entry?.matched).length
    : signalResult?.summary?.rules_matched

  const triggerRows = buildTriggerRows({ instrumentResult, rules: strategy.rules, symbol })

  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-white/[0.06] bg-white/[0.02] p-3">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-[10px] uppercase tracking-wider text-slate-500">Order triggers</p>
            <p className="text-xs text-slate-400">Read-only preview of when this strategy would place orders based on your rules.</p>
          </div>
          <div className="flex gap-2 text-[11px] text-slate-400">
            <button type="button" onClick={onNavigateToRules} className="rounded border border-white/10 bg-white/5 px-2 py-1 text-xs text-slate-200 hover:border-white/20 hover:text-white">
              View rules
            </button>
            <button type="button" onClick={onNavigateToExecution} className="rounded border border-white/10 bg-white/5 px-2 py-1 text-xs text-slate-200 hover:border-white/20 hover:text-white">
              Execution config
            </button>
          </div>
        </div>
      </div>

      {/* Mini trigger summary - shown if we have results */}
      {signalResult && (
        <div className="flex flex-wrap items-center gap-4 rounded-lg border border-white/[0.06] bg-white/[0.02] px-4 py-2">
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-emerald-500" />
            <span className="text-sm font-medium text-white">{buyCount}</span>
            <span className="text-xs text-slate-500">buys</span>
          </div>
          <div className="h-4 w-px bg-white/10" />
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-rose-500" />
            <span className="text-sm font-medium text-white">{sellCount}</span>
            <span className="text-xs text-slate-500">sells</span>
          </div>
          {rulesMatched !== undefined && (
            <>
              <div className="h-4 w-px bg-white/10" />
              <span className="text-xs text-slate-400">
                {rulesMatched}/{strategy.rules?.length || 0} rules matched
              </span>
            </>
          )}
        </div>
      )}

      <form onSubmit={onSubmit} className="space-y-3" aria-label="Order trigger preview controls">
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

        {/* Preview button */}
        <div className="flex justify-end">
          <Button
            type="submit"
            disabled={signalsLoading || !signalInstrumentId}
            loading={signalsLoading}
          >
            {signalsLoading ? 'Previewing…' : 'Preview Order Triggers'}
          </Button>
        </div>
      </form>

      {/* Trigger results */}
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

          <div className="rounded-xl border border-white/10 bg-white/[0.02] p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-sm font-semibold text-white">Recent order triggers</p>
                <p className="text-xs text-slate-400">Derived from evaluated rules—no indicator edits or recompute actions here.</p>
              </div>
              <span className="rounded-full border border-white/10 bg-black/50 px-3 py-1 text-[10px] uppercase tracking-[0.25em] text-slate-300">
                {triggerRows.length || 0} events
              </span>
            </div>

            {triggerRows.length === 0 ? (
              <p className="mt-3 text-sm text-slate-500">Run a preview to see when this strategy would attempt orders.</p>
            ) : (
              <div className="mt-3 divide-y divide-white/5 border-t border-white/10">
                {triggerRows.map((row) => (
                  <div key={row.id} className="flex flex-wrap items-center gap-3 py-3">
                    <span className={`inline-flex items-center rounded-full px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.24em] ${row.direction === 'BUY' ? 'bg-emerald-500/15 text-emerald-200 border border-emerald-500/30' : 'bg-rose-500/15 text-rose-200 border border-rose-500/30'}`}>
                      {row.direction}
                    </span>
                    <div className="flex min-w-[180px] flex-col">
                      <span className="text-sm font-medium text-white">{row.ruleName}</span>
                      <span className="text-[11px] text-slate-400">{row.instrument} • {row.triggerType || 'entry'}{row.matched === false ? ' (not matched)' : ''}</span>
                    </div>
                    <div className="flex flex-1 flex-wrap items-center gap-2 text-[11px] text-slate-400">
                      <span className="rounded border border-white/10 bg-white/5 px-2 py-1 text-[11px] text-slate-200">
                        {row.timestamp || 'timestamp —'}
                      </span>
                      {Array.isArray(row.reasons) && row.reasons.length
                        ? row.reasons.slice(0, 4).map((reason, idx) => (
                          <span
                            key={`${row.id}-reason-${idx}`}
                            className="rounded-full border border-white/10 bg-black/40 px-2 py-0.5 text-[10px] uppercase tracking-[0.24em] text-slate-300"
                          >
                            {typeof reason === 'string' ? reason : reason?.label || 'reason'}
                          </span>
                        ))
                        : null}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
