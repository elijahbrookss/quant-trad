import { ChevronDown, ChevronLeft, ChevronRight, Clock, Play, PlusCircle, Wallet, Zap } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'

import { BacktestRangeField } from './BacktestRangeField.jsx'
import { StrategySelector } from './StrategySelector.jsx'
import { WalletBalancesSection } from './WalletBalancesSection.jsx'

const DATE_PRESETS = [
  { label: '7d', days: 7 },
  { label: '30d', days: 30 },
  { label: '90d', days: 90 },
  { label: '6mo', days: 180 },
  { label: '1y', days: 365 },
]

const WALLET_PRESETS = [
  { label: '$1K', currency: 'USD', amount: 1000 },
  { label: '$10K', currency: 'USD', amount: 10000 },
  { label: '$100K', currency: 'USD', amount: 100000 },
]

const formatVariantValue = (value) => {
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  if (value === null) return 'null'
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

function StepCard({ title, hint, children }) {
  return (
    <section className="rounded-lg border border-slate-800/80 bg-slate-950/40 p-5">
      <div className="mb-4 space-y-1">
        <h3 className="text-base font-semibold text-slate-100">{title}</h3>
        {hint ? <p className="text-sm text-slate-500">{hint}</p> : null}
      </div>
      {children}
    </section>
  )
}

function RunTypeSelector({ value, onChange }) {
  const options = [
    { value: 'backtest', label: 'Backtest', icon: Clock },
    { value: 'paper', label: 'Paper', icon: Play },
    { value: 'live', label: 'Live', icon: Zap },
  ]

  return (
    <div className="grid gap-2 sm:grid-cols-3">
      {options.map((opt) => {
        const Icon = opt.icon
        const isActive = value === opt.value
        return (
          <button
            key={opt.value}
            type="button"
            onClick={() => onChange(opt.value)}
            className={`rounded-lg border px-4 py-3 text-left transition ${
              isActive
                ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] text-slate-100'
                : 'border-slate-800 bg-slate-950/50 text-slate-400 hover:border-slate-700 hover:bg-slate-900/60 hover:text-slate-200'
            }`}
          >
            <div className="flex items-center gap-2">
              <Icon className="size-4" />
              <span className="text-sm font-medium">{opt.label}</span>
            </div>
          </button>
        )
      })}
    </div>
  )
}

function PlaybackModeSelector({ value, onChange }) {
  const options = [
    { value: 'instant', label: 'Fast' },
    { value: 'walk-forward', label: 'Full' },
  ]

  return (
    <div className="space-y-2">
      <label className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Playback</label>
      <div className="grid gap-2 sm:grid-cols-2">
        {options.map((opt) => {
          const isActive = (value || 'instant') === opt.value
          return (
            <button
              key={opt.value}
              type="button"
              onClick={() => onChange(opt.value)}
              className={`rounded-lg border px-3 py-3 text-left text-sm transition ${
                isActive
                  ? 'border-slate-600 bg-slate-800/80 text-slate-100'
                  : 'border-slate-800 bg-slate-950/50 text-slate-400 hover:border-slate-700 hover:bg-slate-900/60'
              }`}
            >
              {opt.label}
            </button>
          )
        })}
      </div>
    </div>
  )
}

function ReviewItem({ label, value }) {
  return (
    <div className="rounded-lg border border-white/8 bg-black/20 px-3 py-3">
      <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500">{label}</p>
      <p className="mt-1 text-sm text-slate-200">{value || '—'}</p>
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
  onStrategySelect,
  onVariantSelect,
  onWalletBalanceChange,
  onWalletBalanceAdd,
  onWalletBalanceRemove,
  submitDisabled,
  error,
}) {
  const [envText, setEnvText] = useState('')
  const [advancedFundingOpen, setAdvancedFundingOpen] = useState(false)
  const [advancedRuntimeOpen, setAdvancedRuntimeOpen] = useState(false)
  const [stepIndex, setStepIndex] = useState(0)

  useEffect(() => {
    const entries = Object.entries(form.bot_env || {})
    setEnvText(entries.map(([k, v]) => `${k}=${v ?? ''}`).join('\n'))
  }, [form.bot_env])

  useEffect(() => {
    const rows = Array.isArray(form.wallet_balances) ? form.wallet_balances : []
    const hasAdvancedBalances =
      rows.length > 1 || rows.some((row, index) => index > 0 && (row?.currency || row?.amount))
    if (hasAdvancedBalances) {
      setAdvancedFundingOpen(true)
    }
  }, [form.wallet_balances])

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

  const selectedStrategy = useMemo(() => {
    if (!form.strategy_id) return null
    return strategies.find((strategy) => strategy.id === form.strategy_id) || null
  }, [form.strategy_id, strategies])

  const strategyVariants = useMemo(
    () => (Array.isArray(selectedStrategy?.variants) ? selectedStrategy.variants : []),
    [selectedStrategy],
  )

  const selectedVariant = useMemo(() => {
    if (!strategyVariants.length) return null
    return (
      strategyVariants.find((variant) => variant.id === form.strategy_variant_id)
      || strategyVariants.find((variant) => variant.is_default)
      || strategyVariants[0]
      || null
    )
  }, [form.strategy_variant_id, strategyVariants])

  const variantSummary = useMemo(() => {
    const overrides = selectedVariant?.param_overrides
    if (!overrides || typeof overrides !== 'object') return []
    return Object.entries(overrides)
  }, [selectedVariant])

  const suggestedName = useMemo(() => {
    if (!selectedStrategy) return ''
    const modeLabel =
      form.run_type === 'backtest' ? 'Backtest' : form.run_type === 'paper' ? 'Paper' : 'Live'
    return [selectedStrategy.name, selectedVariant?.is_default ? '' : selectedVariant?.name, modeLabel]
      .filter(Boolean)
      .join(' · ')
  }, [form.run_type, selectedStrategy, selectedVariant])

  const handleDatePreset = (days) => {
    const end = new Date()
    const start = new Date()
    start.setDate(start.getDate() - days)
    onBacktestRangeChange([start, end])
  }

  const handleWalletPreset = (preset) => {
    onWalletBalanceChange(0, { currency: preset.currency, amount: preset.amount })
  }

  const primaryFundingRow = Array.isArray(form.wallet_balances) && form.wallet_balances.length
    ? form.wallet_balances[0]
    : { currency: '', amount: '' }
  const fundingCurrency = primaryFundingRow?.currency || ''
  const fundingAmount = primaryFundingRow?.amount ?? ''
  const fundingSummary = fundingCurrency && fundingAmount
    ? `${String(fundingAmount)} ${String(fundingCurrency).toUpperCase()}`
    : 'Not configured'

  const hasIdentity = Boolean(form.strategy_id && form.name && form.name.trim())
  const hasMode = form.run_type !== 'backtest' || Boolean(form.backtest_start && form.backtest_end)
  const hasFunding = Boolean(fundingCurrency && fundingAmount !== '' && !walletError)

  const steps = useMemo(() => ([
    { key: 'identity', label: 'Identity', complete: hasIdentity },
    { key: 'mode', label: 'Run Mode', complete: hasMode },
    { key: 'funding', label: 'Funding', complete: hasFunding },
    { key: 'review', label: 'Review', complete: !submitDisabled },
  ]), [hasFunding, hasIdentity, hasMode, submitDisabled])

  const currentStep = steps[stepIndex] || steps[0]
  const isFinalStep = stepIndex === steps.length - 1
  const canAdvance = currentStep?.complete ?? true

  const reviewMode = form.run_type === 'backtest'
    ? `Backtest · ${(form.mode || 'instant') === 'walk-forward' ? 'Full' : 'Fast'}`
    : form.run_type === 'paper'
      ? 'Paper'
      : 'Live'
  const reviewRange =
    form.run_type === 'backtest'
      ? form.backtest_start && form.backtest_end
        ? `${new Date(form.backtest_start).toLocaleDateString()} → ${new Date(form.backtest_end).toLocaleDateString()}`
        : 'Select a date range'
      : 'Not required'

  return (
    <form onSubmit={onSubmit} className="space-y-4">
      <div className="grid gap-2 sm:grid-cols-4">
        {steps.map((step, index) => {
          const isActive = index === stepIndex
          return (
            <button
              key={step.key}
              type="button"
              onClick={() => setStepIndex(index)}
              className={`rounded-lg border px-3 py-2 text-left transition ${
                isActive
                  ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] text-slate-100'
                  : 'border-slate-800 bg-slate-950/40 text-slate-400 hover:border-slate-700 hover:bg-slate-900/50'
              }`}
            >
              <div className="text-[10px] uppercase tracking-[0.2em] text-slate-500">
                {String(index + 1).padStart(2, '0')}
              </div>
              <div className="mt-1 text-sm font-medium">{step.label}</div>
            </button>
          )
        })}
      </div>

      {currentStep?.key === 'identity' ? (
        <StepCard title="Choose strategy and variant" hint="Pick the saved strategy shape, then name the bot.">
          <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_280px]">
            <div className="space-y-2">
              <div className="text-xs font-medium text-slate-300">Strategy</div>
              <StrategySelector
                strategies={strategies}
                selectedIds={form.strategy_id ? [form.strategy_id] : []}
                onSelect={onStrategySelect}
                loading={strategiesLoading}
                error={strategyError}
                compact
              />
            </div>
            <div className="space-y-4 rounded-lg border border-white/8 bg-black/20 p-4">
              <div className="space-y-1.5">
                <label className="text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500">Bot name</label>
                <input
                  type="text"
                  name="name"
                  value={form.name}
                  onChange={onChange}
                  placeholder={suggestedName || 'My Strategy Bot'}
                  className="w-full rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2.5 text-sm text-slate-200 placeholder:text-slate-600 focus:border-slate-700 focus:outline-none"
                />
              </div>
              <div className="space-y-1.5">
                <label className="text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500">Variant</label>
                <select
                  value={form.strategy_variant_id || ''}
                  onChange={(event) => onVariantSelect(event.target.value)}
                  disabled={!selectedStrategy || !strategyVariants.length}
                  className="w-full rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2.5 text-sm text-slate-200 focus:border-slate-700 focus:outline-none disabled:cursor-not-allowed disabled:text-slate-600"
                >
                  {!strategyVariants.length ? (
                    <option value="">Strategy defaults</option>
                  ) : (
                    strategyVariants.map((variant) => (
                      <option key={variant.id} value={variant.id}>
                        {variant.name}{variant.is_default ? ' (Default)' : ''}
                      </option>
                    ))
                  )}
                </select>
                {selectedVariant ? (
                  <div className="rounded-lg border border-white/8 bg-white/[0.03] p-3">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium text-slate-200">{selectedVariant.name}</span>
                      {selectedVariant.is_default ? (
                        <span className="rounded-full bg-emerald-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-emerald-300">
                          Default
                        </span>
                      ) : null}
                    </div>
                    {variantSummary.length ? (
                      <div className="mt-3 flex flex-wrap gap-2">
                        {variantSummary.map(([key, value]) => (
                          <span
                            key={`${selectedVariant.id}-${key}`}
                            className="rounded-md border border-white/10 bg-black/30 px-2 py-1 text-xs text-slate-300"
                          >
                            <span className="font-medium text-slate-200">{key}</span>
                            <span className="mx-1 text-slate-500">=</span>
                            <span className="text-slate-400">{formatVariantValue(value)}</span>
                          </span>
                        ))}
                      </div>
                    ) : (
                      <p className="mt-2 text-xs text-slate-500">Uses strategy defaults.</p>
                    )}
                  </div>
                ) : (
                  <p className="text-xs text-slate-500">Uses strategy defaults.</p>
                )}
              </div>
            </div>
          </div>
        </StepCard>
      ) : null}

      {currentStep?.key === 'mode' ? (
        <StepCard title="Choose run mode" hint="Backtest uses a date range. Paper and live keep execution context separate from strategy design.">
          <div className="space-y-4">
            <RunTypeSelector value={form.run_type} onChange={(value) => onChange('run_type', value)} />
            {form.run_type === 'backtest' ? (
              <div className="space-y-4 rounded-lg border border-white/8 bg-black/20 p-4">
                <PlaybackModeSelector value={form.mode} onChange={(value) => onChange('mode', value)} />
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
                  />
                </div>
              </div>
            ) : (
              <div className="rounded-lg border border-white/8 bg-black/20 px-4 py-4 text-sm text-slate-400">
                {form.run_type === 'paper'
                  ? 'Paper mode uses the same strategy and variant, without live capital.'
                  : 'Live mode will attach execution account context here later, without duplicating strategy-layer settings.'}
              </div>
            )}
            <details
              open={advancedRuntimeOpen}
              onToggle={(event) => setAdvancedRuntimeOpen(event.currentTarget.open)}
              className="rounded-lg border border-white/8 bg-black/20"
            >
              <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-4 py-3 text-sm font-medium text-slate-200">
                <span>Advanced runtime options</span>
                <ChevronDown className={`size-4 text-slate-500 transition-transform ${advancedRuntimeOpen ? 'rotate-180' : ''}`} />
              </summary>
              <div className="grid gap-4 border-t border-white/8 px-4 py-4 md:grid-cols-2">
                <div className="space-y-1.5">
                  <label className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Snapshot interval (ms)</label>
                  <input
                    type="number"
                    min={100}
                    step={100}
                    name="snapshot_interval_ms"
                    value={form.snapshot_interval_ms || 1000}
                    onChange={(event) => onChange('snapshot_interval_ms', Number(event.target.value) || 1000)}
                    className="w-full rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2.5 text-sm text-slate-200 focus:border-slate-700 focus:outline-none"
                  />
                </div>
                <div className="space-y-1.5 md:col-span-2">
                  <label className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Bot env overrides</label>
                  <textarea
                    value={envText}
                    onChange={(event) => handleEnvTextChange(event.target.value)}
                    rows={4}
                    placeholder={'KEY=value\nANOTHER_KEY=value'}
                    className="w-full rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2.5 font-mono text-xs text-slate-200 placeholder:text-slate-600 focus:border-slate-700 focus:outline-none"
                  />
                </div>
              </div>
            </details>
          </div>
        </StepCard>
      ) : null}

      {currentStep?.key === 'funding' ? (
        <StepCard title="Set starting funding" hint="Keep it simple by default. Add more assets only if needed.">
          <div className="space-y-4">
            <div className="grid gap-4 lg:grid-cols-[160px_minmax(0,1fr)]">
              <div className="space-y-1.5">
                <label className="text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500">Base currency</label>
                <input
                  type="text"
                  value={fundingCurrency}
                  onChange={(event) => onWalletBalanceChange(0, { currency: event.target.value.toUpperCase() })}
                  placeholder="USD"
                  className="w-full rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2.5 text-sm font-medium uppercase text-slate-200 placeholder:text-slate-600 focus:border-slate-700 focus:outline-none"
                />
              </div>
              <div className="space-y-1.5">
                <div className="flex items-center justify-between">
                  <label className="text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500">Starting balance</label>
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
                <input
                  type="number"
                  step="any"
                  value={fundingAmount}
                  onChange={(event) => onWalletBalanceChange(0, { amount: event.target.value })}
                  placeholder="10000"
                  className="w-full rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2.5 text-sm text-slate-200 placeholder:text-slate-600 focus:border-slate-700 focus:outline-none"
                />
              </div>
            </div>

            <div className="flex items-center gap-3 rounded-lg border border-white/8 bg-black/20 px-3 py-3">
              <span className="inline-flex size-8 items-center justify-center rounded-lg bg-slate-900/80 text-slate-300">
                <Wallet className="size-4" />
              </span>
              <div className="min-w-0 flex-1">
                <p className="text-sm font-medium text-slate-200">Funding summary</p>
                <p className="text-xs text-slate-500">{fundingSummary}</p>
              </div>
            </div>

            <details
              open={advancedFundingOpen}
              onToggle={(event) => setAdvancedFundingOpen(event.currentTarget.open)}
              className="rounded-lg border border-white/8 bg-black/20"
            >
              <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-4 py-3 text-sm font-medium text-slate-200">
                <span>Advanced asset balances</span>
                <ChevronDown className={`size-4 text-slate-500 transition-transform ${advancedFundingOpen ? 'rotate-180' : ''}`} />
              </summary>
              <div className="border-t border-white/8 px-4 py-4">
                <WalletBalancesSection
                  walletBalances={form.wallet_balances}
                  onWalletBalanceChange={onWalletBalanceChange}
                  onWalletBalanceAdd={onWalletBalanceAdd}
                  onWalletBalanceRemove={onWalletBalanceRemove}
                  walletError={walletError}
                />
              </div>
            </details>
          </div>
        </StepCard>
      ) : null}

      {currentStep?.key === 'review' ? (
        <StepCard title="Review and create" hint="One bot, one concrete strategy instance.">
          <div className="grid gap-3 sm:grid-cols-2">
            <ReviewItem label="Strategy" value={selectedStrategy?.name || 'Select a strategy'} />
            <ReviewItem label="Variant" value={selectedVariant?.name || 'Strategy defaults'} />
            <ReviewItem label="Run mode" value={reviewMode} />
            <ReviewItem label="Funding" value={fundingSummary} />
            <ReviewItem label="Date range" value={reviewRange} />
            <ReviewItem label="Bot name" value={form.name || suggestedName || 'Name required'} />
          </div>
          {error ? (
            <div className="mt-4 rounded-lg border border-rose-900/50 bg-rose-950/20 px-4 py-3 text-sm text-rose-300">
              {error}
            </div>
          ) : null}
        </StepCard>
      ) : null}

      <div className="flex items-center justify-between gap-3 border-t border-slate-800 pt-4">
        <div className="text-xs text-slate-500">
          {isFinalStep ? 'Ready to create the bot.' : canAdvance ? 'Ready for the next step.' : 'Complete this step to continue.'}
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setStepIndex((prev) => Math.max(prev - 1, 0))}
            disabled={stepIndex === 0}
            className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900/60 px-4 py-2 text-sm font-medium text-slate-300 transition hover:border-slate-600 hover:bg-slate-900 disabled:cursor-not-allowed disabled:opacity-40"
          >
            <ChevronLeft className="size-4" /> Back
          </button>
          {isFinalStep ? (
            <button
              type="submit"
              className="inline-flex items-center gap-2 rounded-lg border border-slate-600 bg-slate-700/80 px-4 py-2 text-sm font-medium text-slate-100 transition hover:border-slate-500 hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
              disabled={submitDisabled}
            >
              <PlusCircle className="size-4" /> Create Bot
            </button>
          ) : (
            <button
              type="button"
              onClick={() => {
                if (!canAdvance) return
                setStepIndex((prev) => Math.min(prev + 1, steps.length - 1))
              }}
              disabled={!canAdvance}
              className="inline-flex items-center gap-2 rounded-lg border border-slate-600 bg-slate-700/80 px-4 py-2 text-sm font-medium text-slate-100 transition hover:border-slate-500 hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Next <ChevronRight className="size-4" />
            </button>
          )}
        </div>
      </div>
    </form>
  )
}

export default BotCreateForm
