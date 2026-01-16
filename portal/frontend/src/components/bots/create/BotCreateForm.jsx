import { PlusCircle, Zap, Clock, Play } from 'lucide-react'
import { useMemo } from 'react'
import { BacktestRangeField } from './BacktestRangeField.jsx'
import { StrategySelector } from './StrategySelector.jsx'
import { WalletBalancesSection } from './WalletBalancesSection.jsx'

/**
 * Date range presets for quick selection
 */
const DATE_PRESETS = [
  { label: '7d', days: 7 },
  { label: '30d', days: 30 },
  { label: '90d', days: 90 },
  { label: '6mo', days: 180 },
  { label: '1y', days: 365 },
]

/**
 * Common wallet presets
 */
const WALLET_PRESETS = [
  { label: '$1K', currency: 'USD', amount: 1000 },
  { label: '$10K', currency: 'USD', amount: 10000 },
  { label: '$100K', currency: 'USD', amount: 100000 },
]

function RunTypeSelector({ value, onChange }) {
  const options = [
    { value: 'backtest', label: 'Backtest', icon: Clock, description: 'Replay historical data' },
    { value: 'paper', label: 'Paper', icon: Play, description: 'Coming soon', disabled: true },
    { value: 'live', label: 'Live', icon: Zap, description: 'Coming soon', disabled: true },
  ]

  return (
    <div className="flex gap-2">
      {options.map((opt) => {
        const Icon = opt.icon
        const isActive = value === opt.value
        return (
          <button
            key={opt.value}
            type="button"
            onClick={() => !opt.disabled && onChange(opt.value)}
            disabled={opt.disabled}
            className={`flex flex-1 flex-col items-center gap-1 rounded-lg border px-3 py-2.5 text-center transition-all ${
              isActive
                ? 'border-slate-600 bg-slate-800/80 text-slate-100'
                : opt.disabled
                  ? 'cursor-not-allowed border-slate-800/50 bg-slate-950/30 text-slate-600'
                  : 'border-slate-800 bg-slate-950/50 text-slate-400 hover:border-slate-700 hover:bg-slate-900/50 hover:text-slate-300'
            }`}
          >
            <Icon className="size-4" />
            <span className="text-xs font-medium">{opt.label}</span>
          </button>
        )
      })}
    </div>
  )
}

function PlaybackModeSelector({ value, onChange }) {
  const options = [
    { value: 'instant', label: 'Instant', description: 'Fastest' },
    { value: 'fast', label: 'Fast', description: 'Skip intrabar' },
    { value: 'full', label: 'Full', description: 'All details' },
  ]

  return (
    <div className="space-y-1.5">
      <label className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Speed</label>
      <div className="flex gap-1">
        {options.map((opt) => {
          const isActive = (value || 'full') === opt.value
          return (
            <button
              key={opt.value}
              type="button"
              onClick={() => onChange(opt.value)}
              className={`flex-1 rounded-md px-2 py-1.5 text-xs font-medium transition-colors ${
                isActive
                  ? 'bg-slate-700 text-slate-100'
                  : 'bg-slate-900/50 text-slate-500 hover:bg-slate-800/50 hover:text-slate-400'
              }`}
              title={opt.description}
            >
              {opt.label}
            </button>
          )
        })}
      </div>
    </div>
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
  // Auto-generate name based on selected strategy
  const selectedStrategy = useMemo(() => {
    if (form.strategy_ids?.length === 1) {
      return strategies.find(s => s.id === form.strategy_ids[0])
    }
    return null
  }, [form.strategy_ids, strategies])

  const suggestedName = useMemo(() => {
    if (selectedStrategy) {
      const date = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
      return `${selectedStrategy.name} - ${date}`
    }
    return ''
  }, [selectedStrategy])

  // Handle date preset click
  const handleDatePreset = (days) => {
    const end = new Date()
    const start = new Date()
    start.setDate(start.getDate() - days)
    onBacktestRangeChange([start, end])
  }

  // Handle wallet preset click
  const handleWalletPreset = (preset) => {
    // Clear existing and set the preset as first balance
    onWalletBalanceChange(0, { currency: preset.currency, amount: preset.amount })
  }

  // Check if form has the minimum required fields
  const hasStrategy = form.strategy_ids?.length > 0
  const hasDateRange = form.backtest_start && form.backtest_end
  const hasWallet = form.wallet_balances?.length > 0 && form.wallet_balances.some(w => w.currency && w.amount)

  return (
    <form onSubmit={onSubmit} className="space-y-5">
      {/* Step 1: Run Type */}
      <div className="space-y-3">
        <div className="flex items-center gap-2">
          <span className="flex size-5 items-center justify-center rounded-full bg-slate-800 text-[10px] font-medium text-slate-400">1</span>
          <label className="text-xs font-medium text-slate-300">Run Type</label>
        </div>
        <RunTypeSelector
          value={form.run_type}
          onChange={(value) => onChange({ target: { name: 'run_type', value } })}
        />
      </div>

      {/* Step 2: Strategy Selection */}
      <div className="space-y-3">
        <div className="flex items-center gap-2">
          <span className={`flex size-5 items-center justify-center rounded-full text-[10px] font-medium ${
            hasStrategy ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-800 text-slate-400'
          }`}>2</span>
          <label className="text-xs font-medium text-slate-300">Strategy</label>
          {hasStrategy && <span className="text-[10px] text-emerald-500">✓</span>}
        </div>
        <StrategySelector
          strategies={strategies}
          selectedIds={form.strategy_ids}
          onToggle={onStrategyToggle}
          loading={strategiesLoading}
          error={strategyError}
          compact
        />
      </div>

      {/* Step 3: Date Range (for backtest) */}
      {form.run_type === 'backtest' && (
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <span className={`flex size-5 items-center justify-center rounded-full text-[10px] font-medium ${
                hasDateRange ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-800 text-slate-400'
              }`}>3</span>
              <label className="text-xs font-medium text-slate-300">Date Range</label>
              {hasDateRange && <span className="text-[10px] text-emerald-500">✓</span>}
            </div>
            {/* Quick presets */}
            <div className="flex gap-1">
              {DATE_PRESETS.map((preset) => (
                <button
                  key={preset.label}
                  type="button"
                  onClick={() => handleDatePreset(preset.days)}
                  className="rounded px-2 py-0.5 text-[10px] font-medium text-slate-500 transition hover:bg-slate-800 hover:text-slate-300"
                >
                  {preset.label}
                </button>
              ))}
            </div>
          </div>
          <BacktestRangeField
            start={form.backtest_start}
            end={form.backtest_end}
            onChange={onBacktestRangeChange}
            compact
          />
        </div>
      )}

      {/* Step 4: Initial Capital */}
      <div className="space-y-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className={`flex size-5 items-center justify-center rounded-full text-[10px] font-medium ${
              hasWallet ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-800 text-slate-400'
            }`}>{form.run_type === 'backtest' ? '4' : '3'}</span>
            <label className="text-xs font-medium text-slate-300">Initial Capital</label>
            {hasWallet && <span className="text-[10px] text-emerald-500">✓</span>}
          </div>
          {/* Quick presets */}
          <div className="flex gap-1">
            {WALLET_PRESETS.map((preset) => (
              <button
                key={preset.label}
                type="button"
                onClick={() => handleWalletPreset(preset)}
                className="rounded px-2 py-0.5 text-[10px] font-medium text-slate-500 transition hover:bg-slate-800 hover:text-slate-300"
              >
                {preset.label}
              </button>
            ))}
          </div>
        </div>
        <WalletBalancesSection
          walletBalances={form.wallet_balances}
          onWalletBalanceChange={onWalletBalanceChange}
          onWalletBalanceAdd={onWalletBalanceAdd}
          onWalletBalanceRemove={onWalletBalanceRemove}
          walletError={walletError}
          compact
        />
      </div>

      {/* Optional: Name and Playback Speed */}
      <div className="space-y-3 rounded-lg border border-slate-800/50 bg-slate-950/30 p-3">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <div className="space-y-1.5">
            <label className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Bot Name</label>
            <input
              type="text"
              name="name"
              value={form.name}
              onChange={onChange}
              placeholder={suggestedName || 'Auto-generated if empty'}
              className="w-full rounded-md border border-slate-800 bg-slate-950/50 px-3 py-2 text-sm text-slate-200 placeholder:text-slate-600 transition-colors focus:border-slate-700 focus:bg-slate-950 focus:outline-none"
            />
          </div>
          <PlaybackModeSelector
            value={form.playback_mode}
            onChange={(value) => onChange({ target: { name: 'playback_mode', value } })}
          />
        </div>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-900/50 bg-rose-950/20 px-3 py-2 text-xs text-rose-300">
          {error}
        </div>
      )}

      {/* Submit */}
      <div className="flex items-center justify-between border-t border-slate-800 pt-4">
        <div className="text-xs text-slate-600">
          {!hasStrategy && 'Select a strategy to continue'}
          {hasStrategy && !hasDateRange && form.run_type === 'backtest' && 'Set a date range'}
          {hasStrategy && hasDateRange && !hasWallet && 'Add initial capital'}
        </div>
        <button
          type="submit"
          className="inline-flex items-center gap-2 rounded-lg border border-slate-600 bg-slate-700/80 px-4 py-2 text-sm font-medium text-slate-100 transition-colors hover:border-slate-500 hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
          disabled={submitDisabled}
        >
          <PlusCircle className="size-4" /> Create & Run
        </button>
      </div>
    </form>
  )
}
