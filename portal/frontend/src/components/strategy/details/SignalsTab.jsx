import React from 'react'
import { Button } from '../../ui'
import { SignalSummary } from '../signals'
import { SignalPreviewCharts } from '../signals/SignalPreviewCharts.jsx'

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
  // These components need to be passed in
  DateRangePickerComponent,
}) => {
  const activeInstrument = instruments.find((instrument) => instrument?.id === signalInstrumentId) || null
  const symbol = activeInstrument?.symbol || '—'
  const interval = strategy.timeframe || '—'
  const datasource = activeInstrument?.datasource || '—'
  const exchange = activeInstrument?.exchange || '—'

  return (
    <>
      <p className="mb-4 text-sm text-slate-400">
        Preview raw BUY/SELL signals from your rules. No PnL or positions.
      </p>

      <form onSubmit={onSubmit} className="space-y-4 rounded-xl border border-white/10 bg-white/5 p-4 text-sm">
        <DateRangePickerComponent
          dateRange={signalWindow.dateRange}
          setDateRange={onDateRangeChange}
        />

        <div className="space-y-2 rounded-lg border border-white/10 bg-white/5 p-3 text-xs text-slate-200">
          <span className="uppercase tracking-[0.25em] text-[10px] text-slate-400">Instruments</span>
          <div className="flex flex-wrap gap-2">
            {instruments.length ? (
              instruments.map((instrument) => (
                <button
                  key={`signal-instrument-${instrument?.id || instrument?.symbol}`}
                  type="button"
                  onClick={() => onInstrumentChange(instrument?.id)}
                  className={`rounded-full border px-3 py-1 text-[11px] uppercase tracking-[0.2em] ${
                    instrument?.id === signalInstrumentId
                      ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-20)] text-white'
                      : 'border-white/10 bg-black/20 text-slate-300 hover:border-white/30 hover:text-white'
                  }`}
                >
                  {instrument?.symbol || 'Instrument'}
                </button>
              ))
            ) : (
              <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">No instruments configured</p>
            )}
          </div>
        </div>

        <div className="space-y-2 rounded-lg border border-white/10 bg-white/5 p-3 text-xs text-slate-200">
          <div className="flex items-center justify-between">
            <span className="uppercase tracking-[0.25em] text-[10px] text-slate-400">Symbol</span>
            <span className="font-semibold text-white">{symbol}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="uppercase tracking-[0.25em] text-[10px] text-slate-400">Interval</span>
            <span className="font-semibold text-white">{interval}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="uppercase tracking-[0.25em] text-[10px] text-slate-400">Data source</span>
            <span className="font-semibold text-white">{datasource}</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="uppercase tracking-[0.25em] text-[10px] text-slate-400">Broker / Exchange</span>
            <span className="font-semibold text-white">{exchange}</span>
          </div>
          <p className="pt-1 text-[11px] text-slate-400">
            We will automatically reuse your strategy inputs for the preview; pick a date range to generate signals.
          </p>
        </div>

        <div className="flex items-end justify-end">
          <Button
            type="submit"
            disabled={signalsLoading || !signalInstrumentId}
            loading={signalsLoading}
            className="w-full justify-center md:w-auto"
          >
            {signalsLoading ? 'Running…' : 'Generate signals'}
          </Button>
        </div>
      </form>

      {signalResult && (
        <div className="mt-4">
          <SignalSummary result={signalResult} instrumentId={signalInstrumentId} />
        </div>
      )}

      {signalResult && !signalResult?.instruments && (
        <div className="mt-4 rounded-2xl border border-rose-500/30 bg-rose-500/10 p-4 text-sm text-rose-200">
          Signal preview payload is missing `instruments`. The UI expects a per-instrument response map for multi-instrument previews.
        </div>
      )}

      {signalResult && (
        <div className="mt-6">
          <SignalPreviewCharts
            strategy={strategy}
            instruments={instruments}
            previewInstrumentId={signalInstrumentId}
            signalResult={signalResult}
            attachedIndicators={attachedIndicators}
          />
        </div>
      )}
    </>
  )
}
