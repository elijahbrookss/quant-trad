import React from 'react'
import { Button } from '../../ui'
import { DEFAULT_DATASOURCE } from '../../../utils/constants'
import { SignalSummary } from '../signals'

/**
 * Signals tab for generating and previewing strategy signals.
 */
export const SignalsTab = ({
  strategy,
  signalWindow,
  signalsLoading,
  signalResult,
  onSubmit,
  onDateRangeChange,
  // These components need to be passed in
  DateRangePickerComponent,
}) => {
  const symbol = strategy.symbols?.[0] || '—'
  const interval = strategy.timeframe || '—'
  const datasource = strategy.datasource || DEFAULT_DATASOURCE
  const exchange = strategy.exchange || '—'

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
            <span className="font-semibold text-white">{datasource || DEFAULT_DATASOURCE}</span>
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
            disabled={signalsLoading}
            loading={signalsLoading}
            className="w-full justify-center md:w-auto"
          >
            {signalsLoading ? 'Running…' : 'Generate signals'}
          </Button>
        </div>
      </form>

      {signalResult && (
        <div className="mt-4">
          <SignalSummary result={signalResult} />
        </div>
      )}
    </>
  )
}
