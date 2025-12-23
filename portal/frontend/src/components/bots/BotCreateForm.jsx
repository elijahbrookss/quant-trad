import { useMemo, useState } from 'react'
import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { Bot, CheckSquare, PlusCircle, Search, X } from 'lucide-react'
import { DateRangePickerComponent } from '../ChartComponent/DateTimePickerComponent.jsx'
import DropdownSelect from '../ChartComponent/DropdownSelect.jsx'
import { symbolsFromInstrumentSlots } from '../../utils/instrumentSymbols.js'

function StrategySelector({ strategies, selectedIds, onToggle, loading, error }) {
  const [query, setQuery] = useState('')

  const filteredStrategies = useMemo(() => {
    const needle = query.trim().toLowerCase()
    if (!needle) return strategies
    return strategies.filter((strategy) => {
      const symbols = symbolsFromInstrumentSlots(strategy.instrument_slots).join(', ')
      const haystack = [strategy.name, strategy.timeframe, strategy.exchange, strategy.datasource, symbols]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(needle)
    })
  }, [query, strategies])

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.3em] text-slate-400">
          <CheckSquare className="size-4" /> Strategies
        </div>
        {loading ? <span className="text-[11px] text-slate-500">Loading…</span> : null}
      </div>
      <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-slate-200">
        <Search className="size-4 text-slate-500" />
        <input
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search strategies by name, timeframe, or venue"
          className="w-full bg-transparent text-sm text-white placeholder:text-slate-500 focus:outline-none"
        />
      </label>
      {error ? <p className="text-xs text-rose-300">{error}</p> : null}
      <div className="max-h-72 space-y-2 overflow-y-auto rounded-xl border border-white/10 bg-black/30 p-2">
        {strategies.length === 0 ? (
          <p className="px-2 py-1 text-xs text-slate-400">Create a strategy to start a bot.</p>
        ) : filteredStrategies.length === 0 ? (
          <p className="px-2 py-1 text-xs text-slate-400">No strategies match your search.</p>
        ) : (
          filteredStrategies.map((strategy) => {
            const checked = selectedIds.includes(strategy.id)
            const symbols = symbolsFromInstrumentSlots(strategy.instrument_slots).join(', ')
            return (
              <label
                key={strategy.id}
                className="flex cursor-pointer items-start gap-3 rounded-lg px-2 py-2 transition hover:bg-white/5"
              >
                <input
                  type="checkbox"
                  className="mt-0.5 size-4 rounded border border-white/30 bg-transparent"
                  checked={checked}
                  onChange={() => onToggle(strategy.id)}
                />
                <div className="flex flex-col gap-0.5">
                  <span className="text-sm font-semibold text-white">{strategy.name}</span>
                  <span className="text-[11px] uppercase tracking-[0.3em] text-slate-500">
                    {strategy.timeframe} • {symbols} • {strategy.exchange || strategy.datasource || '—'}
                  </span>
                </div>
              </label>
            )
          })
        )}
      </div>
    </div>
  )
}

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
  onSubmit,
  onChange,
  onBacktestRangeChange,
  onStrategyToggle,
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
            <div className="flex flex-col gap-2">
              <span className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Backtest range</span>
              <DateRangePickerComponent
                className="rounded-xl border border-white/10 bg-[#0f1524]"
                dateRange={[
                  form.backtest_start ? new Date(form.backtest_start) : undefined,
                  form.backtest_end ? new Date(form.backtest_end) : undefined,
                ]}
                setDateRange={onBacktestRangeChange}
              />
              <p className="text-[11px] text-slate-500">Provide start/end dates to walk through history.</p>
            </div>
          ) : null}
        </div>
        <StrategySelector
          strategies={strategies}
          selectedIds={form.strategy_ids}
          onToggle={onStrategyToggle}
          loading={strategiesLoading}
          error={strategyError}
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

export function BotCreateModal({
  open,
  onClose,
  form,
  strategies,
  strategiesLoading,
  strategyError,
  onSubmit,
  onChange,
  onBacktestRangeChange,
  onStrategyToggle,
  error,
}) {
  const submitDisabled =
    !strategies.length ||
    !form.name ||
    !form.strategy_ids.length ||
    (form.run_type === 'backtest' && (!form.backtest_start || !form.backtest_end))

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/60 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-5xl rounded-3xl border border-white/10 bg-[#0b1020] p-6 shadow-2xl shadow-black/50">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-1">
              <DialogTitle className="flex items-center gap-2 text-lg font-semibold text-white">
                <Bot className="size-5 text-[color:var(--accent-text-strong)]" /> Create bot
              </DialogTitle>
              <p className="text-sm text-slate-400">Attach strategies and pick a run type to launch your backtest.</p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="rounded-full p-2 text-slate-400 hover:bg-white/5 hover:text-white"
              aria-label="Close create bot"
            >
              <X className="size-5" />
            </button>
          </div>
          <div className="mt-4 rounded-2xl border border-white/5 bg-white/5 px-3 py-2 text-xs text-slate-300">
            <div className="flex flex-wrap items-center gap-3">
              <span className="rounded-full bg-white/10 px-2 py-1 text-[10px] uppercase tracking-[0.3em] text-white">Guided modal</span>
              <span>Timeframe and playback speed are inherited from the attached strategies and Bot Lens settings.</span>
            </div>
          </div>
          <div className="mt-6">
            <BotCreateForm
              form={form}
              strategies={strategies}
              strategiesLoading={strategiesLoading}
              strategyError={strategyError}
              onSubmit={onSubmit}
              onChange={onChange}
              onBacktestRangeChange={onBacktestRangeChange}
              onStrategyToggle={onStrategyToggle}
              submitDisabled={submitDisabled}
              error={error}
            />
          </div>
        </DialogPanel>
      </div>
    </Dialog>
  )
}

export function buildDefaultForm() {
  return {
    name: '',
    mode: 'walk-forward',
    run_type: 'backtest',
    backtest_start: '',
    backtest_end: '',
    strategy_ids: [],
  }
}
