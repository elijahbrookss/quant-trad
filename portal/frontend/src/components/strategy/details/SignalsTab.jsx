import React from 'react'
import { Input, Select, Button } from '../../ui'
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
  onWindowChange,
  // These components need to be passed in
  DateRangePickerComponent,
  DropdownSelect
}) => {
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

        <div className="grid gap-4 md:grid-cols-2">
          <Input
            label="Interval"
            value={signalWindow.interval}
            onChange={onWindowChange('interval')}
            placeholder={strategy.timeframe || '15m'}
          />

          <Input
            label="Symbol"
            value={strategy.symbols?.[0] || signalWindow.symbol}
            readOnly
          />
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <div>
            <DropdownSelect
              label="Data source (market data)"
              value={signalWindow.datasource || strategy.datasource || ''}
              onChange={onWindowChange('datasource')}
              options={[
                {
                  value: '',
                  label: `Use strategy data source (${strategy.datasource || DEFAULT_DATASOURCE})`,
                  description: 'Follow the strategy default',
                },
                { value: 'ALPACA', label: 'Market data • ALPACA' },
                { value: 'IBKR', label: 'Interactive Brokers • IBKR' },
                { value: 'CCXT', label: 'Crypto data • CCXT' },
              ]}
              className="mt-1 w-full"
            />
            <p className="mt-1 text-[11px] text-slate-500">
              Choose the provider used to load candles when checking these rules.
            </p>
          </div>

          <div>
            <Input
              label="Broker / Exchange"
              value={signalWindow.exchange || strategy.exchange || ''}
              onChange={onWindowChange('exchange')}
              placeholder="e.g. ALPACA, BINANCE"
              hint="Specify where trades would be routed in the future."
            />
          </div>
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
