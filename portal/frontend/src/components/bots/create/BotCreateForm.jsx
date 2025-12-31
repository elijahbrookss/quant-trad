import { PlusCircle } from 'lucide-react'
import DropdownSelect from '../../ChartComponent/DropdownSelect.jsx'
import { BacktestRangeField } from './BacktestRangeField.jsx'
import { StrategySelector } from './StrategySelector.jsx'
import { WalletBalancesSection } from './WalletBalancesSection.jsx'

function RunTypeField({ value, onChange }) {
  const options = [
    { value: 'backtest', label: 'Backtest', description: 'Replay historical data in Bot Lens' },
    { value: 'sim', label: 'Sim', description: 'Paper trading (coming soon)', disabled: true },
    { value: 'live', label: 'Live', description: 'Exchange-connected (coming soon)', disabled: true },
  ]

  return (
    <DropdownSelect
      label="Run type"
      value={value}
      onChange={onChange}
      options={options}
      placeholder="Select run type"
    />
  )
}

export function BotCreateForm({
  form,
  strategies,
  strategiesLoading,
  strategyError,
  walletError,
  onSubmit,
  onChange,
  onBacktestRangeChange,
  onStrategyToggle,
  onWalletBalanceChange,
  onWalletBalanceAdd,
  onWalletBalanceRemove,
  submitDisabled,
  error,
}) {
  return (
    <form onSubmit={onSubmit} className="space-y-6">
      <div className="space-y-4 rounded-2xl border border-white/10 bg-black/30 p-4">
        <div className="space-y-2">
          <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Name</label>
          <input
            type="text"
            name="name"
            value={form.name}
            onChange={onChange}
            className="w-full rounded-xl border border-white/10 bg-[#0f1524] px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
            placeholder="My walk-forward bot"
          />
        </div>
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          <RunTypeField value={form.run_type} onChange={(value) => onChange({ target: { name: 'run_type', value } })} />
          {form.run_type === 'backtest' ? (
            <BacktestRangeField
              start={form.backtest_start}
              end={form.backtest_end}
              onChange={onBacktestRangeChange}
            />
          ) : null}
        </div>
        <StrategySelector
          strategies={strategies}
          selectedIds={form.strategy_ids}
          onToggle={onStrategyToggle}
          loading={strategiesLoading}
          error={strategyError}
        />
        <WalletBalancesSection
          walletBalances={form.wallet_balances}
          onWalletBalanceChange={onWalletBalanceChange}
          onWalletBalanceAdd={onWalletBalanceAdd}
          onWalletBalanceRemove={onWalletBalanceRemove}
          walletError={walletError}
        />
      </div>

      <div className="flex justify-end">
        <button
          type="submit"
          className="inline-flex items-center justify-center gap-2 rounded-xl bg-[color:var(--accent-alpha-40)] px-4 py-2 text-sm font-semibold text-white transition hover:bg-[color:var(--accent-alpha-50)] disabled:opacity-40"
          disabled={submitDisabled}
        >
          <PlusCircle className="size-4" /> Create bot
        </button>
      </div>
      {error ? <p className="text-sm text-rose-300">{error}</p> : null}
    </form>
  )
}
