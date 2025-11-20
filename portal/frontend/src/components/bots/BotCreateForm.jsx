import { PlusCircle } from 'lucide-react'
import ATMConfigForm, { DEFAULT_ATM_TEMPLATE, cloneATMTemplate } from '../atm/ATMConfigForm.jsx'
import { DateRangePickerComponent } from '../ChartComponent/DateTimePickerComponent.jsx'

export function BotCreateForm({
  form,
  strategies,
  strategiesLoading,
  strategyError,
  hasStrategies,
  onSubmit,
  onChange,
  onBacktestRangeChange,
  onStrategyToggle,
  onATMTemplateChange,
  onToggleCustomATM,
}) {
  return (
    <form onSubmit={onSubmit} className="grid grid-cols-1 gap-4 rounded-3xl border border-white/10 bg-gradient-to-br from-slate-950/80 via-slate-900/40 to-slate-900/20 p-6 md:grid-cols-2 lg:grid-cols-3">
      <div className="space-y-3">
        <div>
          <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Name</label>
          <input
            type="text"
            name="name"
            value={form.name}
            onChange={onChange}
            className="mt-1 w-full rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
            placeholder="My walk-forward bot"
          />
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Timeframe</label>
            <input
              type="text"
              name="timeframe"
              value={form.timeframe}
              onChange={onChange}
              className="mt-1 w-full rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
            />
          </div>
          <div>
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Run type</label>
            <div className="mt-1 flex items-center gap-2 rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white">
              <span className="rounded-full bg-white/10 px-2 py-0.5 text-[11px] uppercase tracking-[0.3em]">Walk-forward</span>
              <span className="text-xs text-slate-400">Instant playback available in Bot Lens</span>
            </div>
          </div>
        </div>
        <div>
          <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Playback speed (Bot Lens)</label>
          <input
            type="number"
            step="0.1"
            min="0"
            name="playback_speed"
            value={form.playback_speed}
            onChange={onChange}
            className="mt-1 w-full rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
          />
          <p className="mt-1 text-[11px] text-slate-500">Use 0 for instant playback inside Bot Lens.</p>
        </div>
        <div>
          <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Backtest range</label>
          <DateRangePickerComponent
            className="mt-1"
            startValue={form.backtest_start}
            endValue={form.backtest_end}
            onChange={onBacktestRangeChange}
          />
          <p className="mt-1 text-[11px] text-slate-500">Walk-forward requires both a start and end date.</p>
        </div>
      </div>

      <div className="space-y-3 lg:col-span-1">
        <div>
          <div className="flex items-center justify-between">
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Strategies</label>
            {strategiesLoading ? <span className="text-[11px] text-slate-500">Loading…</span> : null}
          </div>
          {strategyError ? (
            <p className="mt-1 text-xs text-rose-300">{strategyError}</p>
          ) : hasStrategies ? (
            <div className="mt-2 space-y-2 rounded-xl border border-white/10 bg-black/30 p-2">
              {strategies.map((strategy) => (
                <label key={strategy.id} className="flex cursor-pointer items-center gap-2 rounded-lg px-2 py-1 hover:bg-white/5">
                  <input
                    type="checkbox"
                    className="size-4 rounded border border-white/30 bg-transparent"
                    checked={form.strategy_ids.includes(strategy.id)}
                    onChange={() => onStrategyToggle(strategy.id)}
                  />
                  <div className="flex flex-col">
                    <span className="text-sm font-medium text-white">{strategy.name}</span>
                    <span className="text-[11px] uppercase tracking-[0.3em] text-slate-500">
                      {strategy.timeframe} • {strategy.exchange || strategy.datasource || '—'}
                    </span>
                  </div>
                </label>
              ))}
            </div>
          ) : (
            <p className="text-xs text-slate-400">Create a strategy in the Strategies tab to unlock bot creation.</p>
          )}
        </div>
      </div>

      <div className="space-y-3 lg:col-span-1">
        <button
          type="submit"
          className="inline-flex w-full items-center justify-center gap-2 rounded-2xl bg-[color:var(--accent-alpha-40)] px-4 py-3 text-sm font-semibold text-white disabled:opacity-40"
          disabled={!hasStrategies || !form.name || !form.strategy_ids.length || (form.run_type === 'backtest' && (!form.backtest_start || !form.backtest_end))}
        >
          <PlusCircle className="size-4" /> Create bot
        </button>

        <div className="rounded-2xl border border-white/10 bg-black/30 p-4 text-sm text-slate-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-[11px] uppercase tracking-[0.3em] text-slate-400">ATM override</p>
              <p className="text-xs text-slate-400">Optional custom contracts/targets for this bot run.</p>
            </div>
            <button
              type="button"
              onClick={onToggleCustomATM}
              className={`inline-flex items-center rounded-full border px-3 py-1 text-[11px] uppercase tracking-[0.3em] ${form.use_custom_atm ? 'border-emerald-400/40 text-emerald-200' : 'border-white/20 text-slate-300'}`}
            >
              {form.use_custom_atm ? 'Disable override' : 'Use override'}
            </button>
          </div>
          {form.use_custom_atm ? (
            <div className="mt-3">
              <ATMConfigForm value={form.atm_template} onChange={onATMTemplateChange} />
            </div>
          ) : (
            <p className="mt-3 text-xs text-slate-400">Bots will reuse each strategy's ATM template unless you enable an override.</p>
          )}
        </div>
      </div>
    </form>
  )
}

export function buildDefaultForm() {
  return {
    name: '',
    timeframe: '15m',
    mode: 'walk-forward',
    run_type: 'backtest',
    playback_speed: 10,
    backtest_start: '',
    backtest_end: '',
    strategy_ids: [],
    use_custom_atm: false,
    atm_template: cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
  }
}

