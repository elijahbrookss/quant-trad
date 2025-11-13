import { useCallback, useEffect, useMemo, useState } from 'react'
import { Play, Square, Eye, PlusCircle } from 'lucide-react'
import {
  listBots,
  createBot,
  startBot as startBotApi,
  stopBot as stopBotApi,
} from '../../adapters/bot.adapter.js'
import { fetchStrategies } from '../../adapters/strategy.adapter.js'
import { BotPerformanceModal } from './BotPerformanceModal.jsx'

const defaultForm = {
  name: '',
  timeframe: '15m',
  mode: 'walk-forward',
  fetch_seconds: 1,
  strategy_ids: [],
}

export function BotPanel() {
  const [bots, setBots] = useState([])
  const [loading, setLoading] = useState(false)
  const [form, setForm] = useState(defaultForm)
  const [lensBot, setLensBot] = useState(null)
  const [error, setError] = useState(null)
  const [strategies, setStrategies] = useState([])
  const [strategiesLoading, setStrategiesLoading] = useState(false)
  const [strategyError, setStrategyError] = useState(null)

  const loadBots = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await listBots()
      setBots(data)
    } catch (err) {
      setError(err?.message || 'Unable to load bots')
    } finally {
      setLoading(false)
    }
  }, [])

  const loadStrategies = useCallback(async () => {
    setStrategiesLoading(true)
    setStrategyError(null)
    try {
      const data = await fetchStrategies()
      setStrategies(data)
    } catch (err) {
      setStrategyError(err?.message || 'Unable to load strategies')
    } finally {
      setStrategiesLoading(false)
    }
  }, [])

  useEffect(() => {
    loadBots()
    loadStrategies()
  }, [loadBots, loadStrategies])

  const handleChange = (event) => {
    const { name, value } = event.target
    setForm((prev) => ({ ...prev, [name]: value }))
  }

  const handleStrategyToggle = (strategyId) => {
    setForm((prev) => {
      const next = prev.strategy_ids.includes(strategyId)
        ? prev.strategy_ids.filter((id) => id !== strategyId)
        : [...prev.strategy_ids, strategyId]
      return { ...prev, strategy_ids: next }
    })
  }

  const handleCreate = async (event) => {
    event.preventDefault()
    setError(null)
    if (!form.name) return
    if (!form.strategy_ids.length) {
      setError('Select at least one strategy for this bot.')
      return
    }
    try {
      const payload = await createBot({
        ...form,
        fetch_seconds: Number(form.fetch_seconds) || 0,
      })
      setBots((prev) => [...prev, payload])
      setForm((prev) => ({ ...defaultForm, strategy_ids: prev.strategy_ids }))
    } catch (err) {
      setError(err?.message || 'Unable to create bot')
    }
  }

  const handleStart = async (botId) => {
    setError(null)
    const target = bots.find((bot) => bot.id === botId)
    if (!target?.strategy_ids?.length) {
      setError('Assign at least one strategy before starting the bot.')
      return
    }
    try {
      await startBotApi(botId)
      loadBots()
    } catch (err) {
      setError(err?.message || 'Unable to start bot')
    }
  }

  const handleStop = async (botId) => {
    setError(null)
    try {
      await stopBotApi(botId)
      loadBots()
    } catch (err) {
      setError(err?.message || 'Unable to stop bot')
    }
  }

  const statusBadge = useCallback((status) => {
    const tone = status === 'running'
      ? 'bg-emerald-500/10 text-emerald-200 border-emerald-400/30'
      : status === 'stopped'
        ? 'bg-rose-500/10 text-rose-200 border-rose-400/30'
        : 'bg-slate-600/20 text-slate-200 border-white/10'
    return (
      <span className={`inline-flex items-center rounded-full border px-3 py-0.5 text-[11px] uppercase tracking-[0.3em] ${tone}`}>
        {status || 'idle'}
      </span>
    )
  }, [])

  const sortedBots = useMemo(() => {
    return [...bots].sort((a, b) => (a.name || '').localeCompare(b.name || ''))
  }, [bots])

  const sortedStrategies = useMemo(() => {
    return [...strategies].sort((a, b) => (a.name || '').localeCompare(b.name || ''))
  }, [strategies])

  const strategyLookup = useMemo(() => {
    const map = new Map()
    for (const strategy of sortedStrategies) {
      if (strategy?.id) {
        map.set(strategy.id, strategy)
      }
    }
    return map
  }, [sortedStrategies])

  const hasStrategies = strategies.length > 0

  return (
    <section className="space-y-6">
      <header className="flex flex-col gap-4 rounded-3xl border border-white/8 bg-white/5 p-6">
        <div className="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">Automation</p>
            <h3 className="text-xl font-semibold text-slate-100">Bot control tower</h3>
            <p className="text-sm text-slate-400">Launch instant or walk-forward backtests wired to live strategies.</p>
          </div>
          <div className="text-xs text-slate-400">
            {strategiesLoading ? 'Loading strategies…' : `${strategies.length} strategies available`}
          </div>
        </div>
        <form onSubmit={handleCreate} className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
          <div className="space-y-3 rounded-2xl border border-white/10 bg-black/30 p-4 text-sm text-slate-200">
            <div className="flex flex-col gap-1">
              <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Name</label>
              <input
                type="text"
                name="name"
                value={form.name}
                onChange={handleChange}
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-slate-100 focus:border-[color:var(--accent-alpha-50)] focus:outline-none"
                placeholder="Market Profile Bot"
                required
              />
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Mode</label>
              <select
                name="mode"
                value={form.mode}
                onChange={handleChange}
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2"
              >
                <option value="instant">Instant</option>
                <option value="walk-forward">Walk-forward</option>
              </select>
            </div>
            <div className="flex flex-col gap-1">
              <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Fetch seconds</label>
              <input
                type="number"
                name="fetch_seconds"
                min="0"
                value={form.fetch_seconds}
                onChange={handleChange}
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2"
              />
            </div>
          </div>
          <div className="space-y-3 rounded-2xl border border-white/10 bg-black/30 p-4 text-sm text-slate-200">
            <div className="flex items-center justify-between">
              <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Strategies</label>
              {!hasStrategies ? (
                <span className="text-[11px] uppercase tracking-[0.2em] text-rose-300">Add a strategy first</span>
              ) : (
                <span className="text-[11px] text-slate-500">Select 1+</span>
              )}
            </div>
            {strategyError ? (
              <div className="rounded-xl border border-rose-500/40 bg-rose-500/5 p-3 text-xs text-rose-200">{strategyError}</div>
            ) : null}
            {hasStrategies ? (
              <div className="max-h-40 space-y-2 overflow-auto pr-2 text-sm">
                {sortedStrategies.map((strategy) => (
                  <label key={strategy.id} className="flex items-center gap-3 rounded-xl border border-white/5 bg-white/5 px-3 py-2 text-slate-200">
                    <input
                      type="checkbox"
                      className="size-4 rounded border border-white/30 bg-transparent"
                      checked={form.strategy_ids.includes(strategy.id)}
                      onChange={() => handleStrategyToggle(strategy.id)}
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
              <p className="text-xs text-slate-400">
                Create a strategy in the Strategies tab to unlock bot creation.
              </p>
            )}
          </div>
          <div className="lg:col-span-2">
            <button
              type="submit"
              className="inline-flex w-full items-center justify-center gap-2 rounded-2xl bg-[color:var(--accent-alpha-40)] px-4 py-3 text-sm font-semibold text-white disabled:opacity-40"
              disabled={!hasStrategies || !form.name || !form.strategy_ids.length}
            >
              <PlusCircle className="size-4" /> Create bot
            </button>
          </div>
        </form>
      </header>

      {error ? (
        <div className="rounded-2xl border border-rose-500/40 bg-rose-500/5 p-4 text-sm text-rose-200">{error}</div>
      ) : null}

      <div className="space-y-3">
        {loading ? (
          <p className="text-sm text-slate-400">Loading bots…</p>
        ) : sortedBots.length === 0 ? (
          <p className="text-sm text-slate-400">No bots yet. Create one to begin a backtest.</p>
        ) : (
          sortedBots.map((bot) => {
            const assignedNames = (bot.strategy_ids || [])
              .map((id) => strategyLookup.get(id)?.name || id)
              .filter(Boolean)
            return (
              <div key={bot.id} className="flex flex-col gap-4 rounded-3xl border border-white/10 bg-white/5 p-4 md:flex-row md:items-center md:justify-between">
                <div>
                  <p className="text-sm uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">
                    {assignedNames.length ? assignedNames.join(', ') : 'No strategies assigned'}
                  </p>
                  <h4 className="text-xl font-semibold text-white">{bot.name}</h4>
                  <p className="text-xs text-slate-400">{bot.mode} • {bot.timeframe} • fetch {bot.fetch_seconds}s</p>
                  <div className="mt-2">{statusBadge(bot.status)}</div>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <button
                    type="button"
                    onClick={() => handleStart(bot.id)}
                    className="inline-flex items-center gap-2 rounded-full border border-emerald-500/30 px-4 py-2 text-sm text-emerald-200 hover:bg-emerald-500/10"
                  >
                    <Play className="size-4" /> Start
                  </button>
                  <button
                    type="button"
                    onClick={() => handleStop(bot.id)}
                    className="inline-flex items-center gap-2 rounded-full border border-rose-500/30 px-4 py-2 text-sm text-rose-200 hover:bg-rose-500/10"
                  >
                    <Square className="size-4" /> Stop
                  </button>
                  <button
                    type="button"
                    onClick={() => setLensBot(bot)}
                    className="inline-flex items-center gap-2 rounded-full border border-white/20 px-4 py-2 text-sm text-slate-200 hover:bg-white/10"
                  >
                    <Eye className="size-4" /> Lens
                  </button>
                </div>
              </div>
            )
          })
        )}
      </div>

      <BotPerformanceModal bot={lensBot} open={Boolean(lensBot)} onClose={() => setLensBot(null)} />
    </section>
  )
}
