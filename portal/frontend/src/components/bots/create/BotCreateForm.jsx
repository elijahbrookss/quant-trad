import { PlusCircle, Zap, Clock, Play, ChevronLeft, ChevronRight } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
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
    { value: 'instant', label: 'Fast', description: 'No intrabar animation' },
    { value: 'walk-forward', label: 'Full', description: 'Intrabar animation' },
  ]

  return (
    <div className="space-y-1.5">
      <label className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Mode</label>
      <div className="flex gap-1">
        {options.map((opt) => {
          const isActive = (value || 'instant') === opt.value
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
  const [envText, setEnvText] = useState('')

  useEffect(() => {
    const entries = Object.entries(form.bot_env || {})
    setEnvText(entries.map(([k, v]) => `${k}=${v ?? ''}`).join('\n'))
  }, [form.bot_env])

  const handleEnvTextChange = (value) => {
    setEnvText(value)
    const next = {}
    for (const line of String(value || '').split('\n')) {
      const trimmed = line.trim()
      if (!trimmed || trimmed.startsWith('#')) continue
      const idx = trimmed.indexOf('=')
      if (idx <= 0) continue
      const key = trimmed.slice(0, idx).trim()
      const val = trimmed.slice(idx + 1).trim()
      if (key) next[key] = val
    }
    onChange('bot_env', next)
  }

  // Auto-generate name based on selected strategy
  const selectedStrategy = useMemo(() => {
    if (form.strategy_id) {
      return strategies.find((s) => s.id === form.strategy_id)
    }
    return null
  }, [form.strategy_id, strategies])

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
  const hasStrategy = Boolean(form.strategy_id)
  const hasDateRange = form.backtest_start && form.backtest_end
  const hasWallet = form.wallet_balances?.length > 0 && form.wallet_balances.some(w => w.currency && w.amount)
  const hasName = Boolean(form.name && form.name.trim())

  const steps = useMemo(() => {
    const baseSteps = [
      {
        key: 'setup',
        title: 'Pick the strategy and run type',
        hint: 'Select one strategy and choose how you want it to run.',
        isComplete: Boolean(form.run_type) && hasStrategy,
        content: (
          <div className="space-y-4">
            <div className="space-y-2">
              <div className="text-xs font-medium text-slate-300">Run type</div>
              <RunTypeSelector
                value={form.run_type}
                onChange={(value) => onChange('run_type', value)}
              />
            </div>
            <div className="space-y-2">
              <div className="text-xs font-medium text-slate-300">Strategy (choose one)</div>
              <StrategySelector
                strategies={strategies}
                selectedIds={form.strategy_id ? [form.strategy_id] : []}
                onToggle={onStrategyToggle}
                loading={strategiesLoading}
                error={strategyError}
                compact
              />
            </div>
          </div>
        ),
      },
      {
        key: 'backtest_config',
        title: 'Backtest details',
        hint: 'Fast skips intrabar animation. Full shows intrabar movement.',
        isComplete: form.run_type !== 'backtest' || (Boolean(form.mode) && Boolean(hasDateRange)),
        enabled: form.run_type === 'backtest',
        content: (
          <div className="space-y-4">
            <PlaybackModeSelector
              value={form.mode}
              onChange={(value) => onChange('mode', value)}
            />
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div className="text-xs font-medium text-slate-300">Date range</div>
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
          </div>
        ),
      },
      {
        key: 'finalize',
        title: 'Capital + name',
        hint: 'Set wallet balances and name the bot.',
        isComplete: hasWallet && hasName,
        content: (
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div className="text-xs font-medium text-slate-300">Initial capital</div>
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
            <div className="space-y-1.5">
              <label className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Snapshot interval (ms)</label>
              <input
                type="number"
                min={100}
                step={100}
                name="snapshot_interval_ms"
                value={form.snapshot_interval_ms || 1000}
                onChange={(event) => onChange('snapshot_interval_ms', Number(event.target.value) || 1000)}
                className="w-full rounded-md border border-slate-800 bg-slate-950/50 px-3 py-2 text-sm text-slate-200 placeholder:text-slate-600 transition-colors focus:border-slate-700 focus:bg-slate-950 focus:outline-none"
              />
            </div>
            <div className="space-y-1.5 md:col-span-2">
              <label className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Bot env overrides</label>
              <textarea
                value={envText}
                onChange={(event) => handleEnvTextChange(event.target.value)}
                rows={4}
                placeholder={'KEY=value\nANOTHER_KEY=value'}
                className="w-full rounded-md border border-slate-800 bg-slate-950/50 px-3 py-2 font-mono text-xs text-slate-200 placeholder:text-slate-600 transition-colors focus:border-slate-700 focus:bg-slate-950 focus:outline-none"
              />
              <p className="text-[11px] text-slate-500">Applied on container start. Changes require stop + restart.</p>
            </div>
            <div className="space-y-1.5">
              <label className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Bot name</label>
              <input
                type="text"
                name="name"
                value={form.name}
                onChange={onChange}
                placeholder={suggestedName || 'Auto-generated if empty'}
                className="w-full rounded-md border border-slate-800 bg-slate-950/50 px-3 py-2 text-sm text-slate-200 placeholder:text-slate-600 transition-colors focus:border-slate-700 focus:bg-slate-950 focus:outline-none"
              />
            </div>
          </div>
        ),
      },
    ]

    return baseSteps.filter((step) => step.enabled !== false)
  }, [
    form.run_type,
    form.strategy_id,
    form.backtest_start,
    form.backtest_end,
    form.wallet_balances,
    form.mode,
    form.name,
    hasStrategy,
    hasDateRange,
    hasWallet,
    hasName,
    onBacktestRangeChange,
    onChange,
    onStrategyToggle,
    onWalletBalanceAdd,
    onWalletBalanceChange,
    onWalletBalanceRemove,
    strategies,
    strategiesLoading,
    strategyError,
    suggestedName,
    walletError,
  ])

  const [stepIndex, setStepIndex] = useState(0)

  useEffect(() => {
    if (!steps.length) return
    if (stepIndex > steps.length - 1) {
      setStepIndex(steps.length - 1)
    }
  }, [stepIndex, steps.length])

  const currentStep = steps[stepIndex] || steps[0]
  const isFinalStep = stepIndex === steps.length - 1
  const canContinue = currentStep?.isComplete ?? true

  return (
    <form onSubmit={onSubmit} className="space-y-5">
      <div className="space-y-4 rounded-xl border border-slate-800/60 bg-slate-950/30 p-5">
        <div className="space-y-1">
          <h3 className="text-lg font-semibold text-slate-100">{currentStep?.title}</h3>
          <p className="text-sm text-slate-500">{currentStep?.hint}</p>
        </div>
        <div>{currentStep?.content}</div>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-900/50 bg-rose-950/20 px-3 py-2 text-xs text-rose-300">
          {error}
        </div>
      )}

      {/* Submit */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-800 pt-4">
        <div className="text-xs text-slate-600">
          {!canContinue && 'Complete this step to continue'}
          {canContinue && !isFinalStep && 'Ready for the next step'}
          {canContinue && isFinalStep && 'All set to run'}
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setStepIndex((prev) => Math.max(prev - 1, 0))}
            disabled={stepIndex === 0}
            className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900/60 px-4 py-2 text-sm font-medium text-slate-300 transition-colors hover:border-slate-600 hover:bg-slate-900 disabled:cursor-not-allowed disabled:opacity-40"
          >
            <ChevronLeft className="size-4" /> Back
          </button>
          {isFinalStep ? (
            <button
              type="submit"
              className="inline-flex items-center gap-2 rounded-lg border border-slate-600 bg-slate-700/80 px-4 py-2 text-sm font-medium text-slate-100 transition-colors hover:border-slate-500 hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
              disabled={submitDisabled}
            >
              <PlusCircle className="size-4" /> Create & Run
            </button>
          ) : (
            <button
              type="button"
              onClick={() => {
                if (!canContinue) return
                setStepIndex((prev) => Math.min(prev + 1, steps.length - 1))
              }}
              disabled={!canContinue}
              className="inline-flex items-center gap-2 rounded-lg border border-slate-600 bg-slate-700/80 px-4 py-2 text-sm font-medium text-slate-100 transition-colors hover:border-slate-500 hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Next <ChevronRight className="size-4" />
            </button>
          )}
        </div>
      </div>
    </form>
  )
}
