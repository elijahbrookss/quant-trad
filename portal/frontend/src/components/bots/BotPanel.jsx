import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Play, Square, Eye, PlusCircle, Trash2, Pause, RotateCw, RefreshCw, Search } from 'lucide-react'
import {
  listBots,
  createBot,
  startBot as startBotApi,
  stopBot as stopBotApi,
  deleteBot as deleteBotApi,
  pauseBot as pauseBotApi,
  resumeBot as resumeBotApi,
  openBotsStream,
} from '../../adapters/bot.adapter.js'
import { fetchStrategies } from '../../adapters/strategy.adapter.js'
import { BotPerformanceModal } from './BotPerformanceModal.jsx'
import { DateRangePickerComponent } from '../ChartComponent/DateTimePickerComponent.jsx'
import ATMConfigForm, { DEFAULT_ATM_TEMPLATE, cloneATMTemplate } from '../atm/ATMConfigForm.jsx'

const defaultForm = {
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

const STATUS_ORDER = {
  running: 0,
  starting: 1,
  paused: 2,
  completed: 3,
  idle: 4,
  stopped: 5,
  error: 6,
}

const computeStatus = (bot) => (bot?.runtime?.status || bot?.status || 'idle').toLowerCase()

export function BotPanel() {
  const [bots, setBots] = useState([])
  const [loading, setLoading] = useState(false)
  const [form, setForm] = useState(defaultForm)
  const [lensBot, setLensBot] = useState(null)
  const [error, setError] = useState(null)
  const [strategies, setStrategies] = useState([])
  const [strategiesLoading, setStrategiesLoading] = useState(false)
  const [strategyError, setStrategyError] = useState(null)
  const [pendingDelete, setPendingDelete] = useState(null)
  const [search, setSearch] = useState('')
  const [botStreamState, setBotStreamState] = useState('idle')
  const botStreamRef = useRef(null)
  const formatPlaybackValue = useCallback((value) => {
    const numeric = Number(value)
    if (!Number.isFinite(numeric)) return '—'
    return numeric <= 0 ? 'Instant' : `${numeric.toFixed(2)}x`
  }, [])
  const playbackLabelFor = useCallback((bot) => {
    const raw =
      bot?.playback_speed ??
      bot?.runtime?.playback_speed ??
      bot?.config?.playback_speed ??
      10
    const value = Number(raw)
    if (!Number.isFinite(value)) return '—'
    if (value <= 0) return 'Instant'
    return `${value.toFixed(2)}x`
  }, [])

  const upsertBot = useCallback((payload) => {
    if (!payload?.id) return
    setBots((prev) => {
      const exists = prev.some((bot) => bot.id === payload.id)
      if (exists) {
        return prev.map((bot) => (bot.id === payload.id ? { ...bot, ...payload } : bot))
      }
      return [...prev, payload]
    })
  }, [])

  const loadBots = useCallback(
    async (withSpinner = true) => {
      if (withSpinner) setLoading(true)
      setError(null)
      try {
        const data = await listBots()
        setBots(data)
      } catch (err) {
        setError(err?.message || 'Unable to load bots')
      } finally {
        if (withSpinner) setLoading(false)
      }
    },
    [],
  )

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

  useEffect(() => {
    let retryTimer = null
    const connectStream = () => {
      if (botStreamRef.current) {
        botStreamRef.current.close()
        botStreamRef.current = null
      }
      const source = openBotsStream()
      if (!source) {
        setBotStreamState('error')
        return
      }
      botStreamRef.current = source
      setBotStreamState('connecting')
      const handlePayload = (event) => {
        try {
          const data = JSON.parse(event.data)
          if (Array.isArray(data)) {
            setBots(data)
          } else if (data?.id) {
            upsertBot(data)
          }
          setBotStreamState('open')
        } catch (err) {
          console.warn('bot stream payload parse failed', err)
        }
      }
      const handleError = () => {
        setBotStreamState('error')
        retryTimer = setTimeout(connectStream, 2500)
      }
      source.onmessage = handlePayload
      source.addEventListener('snapshot', handlePayload)
      source.addEventListener('update', handlePayload)
      source.onerror = handleError
      source.onopen = () => setBotStreamState('open')
    }
    connectStream()
    return () => {
      if (retryTimer) clearTimeout(retryTimer)
      if (botStreamRef.current) {
        botStreamRef.current.close()
        botStreamRef.current = null
      }
    }
  }, [upsertBot])

  useEffect(() => {
    if (botStreamState === 'open') return undefined
    const intervalMs = botStreamState === 'error' ? 2000 : 4000
    const id = setInterval(() => loadBots(false), intervalMs)
    return () => clearInterval(id)
  }, [botStreamState, loadBots])

  const handleChange = (event) => {
    const { name, value } = event.target
    setForm((prev) => {
      const next = { ...prev, [name]: value }
      if (name === 'run_type' && value !== 'backtest') {
        next.backtest_start = ''
        next.backtest_end = ''
      }
      return next
    })
  }

  const handleBacktestRangeChange = useCallback((range) => {
    const [start, end] = Array.isArray(range) ? range : []
    const normalize = (value) => {
      if (!(value instanceof Date)) return ''
      const time = value.getTime()
      return Number.isNaN(time) ? '' : new Date(time).toISOString()
    }
    setForm((prev) => ({
      ...prev,
      backtest_start: normalize(start),
      backtest_end: normalize(end),
    }))
  }, [])

  const handleStrategyToggle = (strategyId) => {
    setForm((prev) => {
      const next = prev.strategy_ids.includes(strategyId)
        ? prev.strategy_ids.filter((id) => id !== strategyId)
        : [...prev.strategy_ids, strategyId]
      return { ...prev, strategy_ids: next }
    })
  }

  const handleATMTemplateChange = useCallback((template) => {
    setForm((prev) => ({ ...prev, atm_template: cloneATMTemplate(template) }))
  }, [])

  const toggleCustomATM = () => {
    setForm((prev) => ({ ...prev, use_custom_atm: !prev.use_custom_atm }))
  }

  const handlePlaybackSpeedChange = (event) => {
    const value = Number(event?.target?.value)
    setForm((prev) => ({ ...prev, playback_speed: Number.isFinite(value) ? value : 0 }))
  }

  const handleCreate = async (event) => {
    event.preventDefault()
    setError(null)
    if (!form.name) return
    if (!form.strategy_ids.length) {
      setError('Select at least one strategy for this bot.')
      return
    }
    if (form.run_type === 'backtest' && (!form.backtest_start || !form.backtest_end)) {
      setError('Provide both a start and end date for backtests.')
      return
    }
    const startISO = form.backtest_start ? new Date(form.backtest_start).toISOString() : undefined
    const endISO = form.backtest_end ? new Date(form.backtest_end).toISOString() : undefined
    try {
      const { use_custom_atm, atm_template, ...rest } = form
      const payloadBody = {
        ...rest,
        playback_speed: Number.isFinite(Number(form.playback_speed))
          ? Number(form.playback_speed)
          : 0,
        backtest_start: form.run_type === 'backtest' ? startISO : undefined,
        backtest_end: form.run_type === 'backtest' ? endISO : undefined,
      }
      if (use_custom_atm) {
        payloadBody.risk = atm_template
      }
      const payload = await createBot(payloadBody)
      upsertBot(payload)
      setForm((prev) => ({
        ...defaultForm,
        strategy_ids: prev.strategy_ids,
        run_type: prev.run_type,
        playback_speed: prev.playback_speed,
        atm_template: use_custom_atm
          ? cloneATMTemplate(atm_template)
          : cloneATMTemplate(DEFAULT_ATM_TEMPLATE),
        use_custom_atm: use_custom_atm && Boolean(payloadBody.risk),
      }))
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
      const payload = await startBotApi(botId)
      upsertBot(payload)
      loadBots(false)
    } catch (err) {
      setError(err?.message || 'Unable to start bot')
    }
  }

  const handleStop = async (botId) => {
    setError(null)
    try {
      const payload = await stopBotApi(botId)
      upsertBot(payload)
      loadBots(false)
    } catch (err) {
      setError(err?.message || 'Unable to stop bot')
    }
  }

  const handlePause = async (botId) => {
    setError(null)
    try {
      const payload = await pauseBotApi(botId)
      upsertBot(payload)
      loadBots(false)
    } catch (err) {
      setError(err?.message || 'Unable to pause bot')
    }
  }

  const handleResume = async (botId) => {
    setError(null)
    try {
      const payload = await resumeBotApi(botId)
      upsertBot(payload)
      loadBots(false)
    } catch (err) {
      setError(err?.message || 'Unable to resume bot')
    }
  }

  const handleDelete = async (botId) => {
    if (!botId) return
    if (!window.confirm('Delete this bot? This cannot be undone.')) return
    setError(null)
    setPendingDelete(botId)
    try {
      await deleteBotApi(botId)
      setBots((prev) => prev.filter((bot) => bot.id !== botId))
    } catch (err) {
      setError(err?.message || 'Unable to delete bot')
    } finally {
      setPendingDelete(null)
    }
  }

  const statusBadge = useCallback((status) => {
    const tone = status === 'running'
      ? 'bg-emerald-500/10 text-emerald-200 border-emerald-400/30'
      : status === 'paused'
        ? 'bg-amber-500/10 text-amber-200 border-amber-400/30'
        : status === 'stopped'
          ? 'bg-rose-500/10 text-rose-200 border-rose-400/30'
          : status === 'completed'
            ? 'bg-sky-500/10 text-sky-200 border-sky-400/30'
            : 'bg-slate-600/20 text-slate-200 border-white/10'
    return (
      <span className={`inline-flex items-center rounded-full border px-3 py-0.5 text-[11px] uppercase tracking-[0.3em] ${tone}`}>
        {status || 'idle'}
      </span>
    )
  }, [])

  const formatDate = (value) => {
    if (!value) return '—'
    try {
      return new Date(value).toLocaleString()
    } catch {
      return value
    }
  }

  const describeRange = (bot) => {
    if ((bot?.run_type || '').toLowerCase() === 'backtest') {
      return `${formatDate(bot?.backtest_start)} → ${formatDate(bot?.backtest_end)}`
    }
    return 'Sim trade (live)'
  }

  const sortedBots = useMemo(() => {
    return [...bots].sort((a, b) => {
      const sa = STATUS_ORDER[computeStatus(a)] ?? 10
      const sb = STATUS_ORDER[computeStatus(b)] ?? 10
      if (sa !== sb) return sa - sb
      return (a.name || '').localeCompare(b.name || '')
    })
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

  const filteredBots = useMemo(() => {
    const query = search.trim().toLowerCase()
    if (!query) return sortedBots
    return sortedBots.filter((bot) => {
      const assignedNames = (bot.strategy_ids || [])
        .map((id) => strategyLookup.get(id)?.name || id)
        .join(' ')
      const haystack = [
        bot.name,
        computeStatus(bot),
        describeRange(bot),
        assignedNames,
        bot.timeframe,
        bot.datasource,
        bot.exchange,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(query)
    })
  }, [search, sortedBots, strategyLookup])

  const hasStrategies = strategies.length > 0

  const describeBotMeta = useCallback(
    (botItem, field) => {
      if (!botItem) return ''
      const values = new Set()
      for (const strategyId of botItem.strategy_ids || []) {
        const strategy = strategyLookup.get(strategyId)
        const value = strategy?.[field]
        if (value) values.add(value)
      }
      if (values.size) {
        return Array.from(values).join(', ')
      }
      return botItem?.[field] || ''
    },
    [strategyLookup],
  )

  const [defaultBacktestStart, defaultBacktestEnd] = useMemo(() => {
    const now = new Date()
    const start = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)
    return [start, now]
  }, [])

  const backtestRange = useMemo(() => {
    const parse = (value) => {
      if (!value) return null
      const date = new Date(value)
      return Number.isNaN(date.getTime()) ? null : date
    }
    const start = parse(form.backtest_start)
    const end = parse(form.backtest_end)
    return start && end ? [start, end] : null
  }, [form.backtest_start, form.backtest_end])

  const refreshSummary = `${filteredBots.length} of ${sortedBots.length} bots`

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
        <div className="flex flex-col gap-3 rounded-3xl border border-white/5 bg-black/30 p-4 text-sm text-slate-200 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex flex-1 flex-wrap items-center gap-3">
            <label className="flex min-w-[220px] flex-1 items-center gap-2 rounded-2xl border border-white/10 bg-black/40 px-3 py-2 text-slate-200">
              <Search className="size-4 text-slate-500" />
              <input
                type="search"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Search bots by name, status, or strategy"
                className="w-full bg-transparent text-sm text-white placeholder:text-slate-500 focus:outline-none"
              />
            </label>
            <button
              type="button"
              onClick={() => loadBots()}
              className="inline-flex items-center gap-2 rounded-2xl border border-white/10 px-4 py-2 text-sm text-slate-200 hover:border-white/40"
              disabled={loading}
            >
              <RefreshCw className={`size-4 ${loading ? 'animate-spin' : ''}`} /> Refresh
            </button>
          </div>
          <span className="text-xs uppercase tracking-[0.3em] text-slate-500">{refreshSummary}</span>
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
              <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Run type</label>
              <select
                name="run_type"
                value={form.run_type}
                onChange={handleChange}
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2"
              >
                <option value="backtest">Backtest</option>
                <option value="sim_trade">Sim trade</option>
              </select>
            </div>
            <div className="space-y-2">
              <div className="flex items-center justify-between text-[11px] uppercase tracking-[0.3em] text-slate-400">
                <span>Playback speed</span>
                <span className="text-xs normal-case tracking-normal text-white">
                  {formatPlaybackValue(form.playback_speed)}
                </span>
              </div>
              <input
                type="range"
                min="0"
                max="25"
                step="0.5"
                name="playback_speed"
                value={form.playback_speed}
                onChange={handlePlaybackSpeedChange}
                className="w-full accent-[color:var(--accent-alpha-60)]"
              />
              <p className="text-[11px] text-slate-500">
                0 = instant playback. 10x is the normal pace; drag right to accelerate walk-forward loops.
              </p>
            </div>
            {form.run_type === 'backtest' ? (
              <div className="rounded-2xl border border-white/10 bg-black/20 p-3">
                <DateRangePickerComponent
                  dateRange={backtestRange}
                  defaultStart={defaultBacktestStart}
                  defaultEnd={defaultBacktestEnd}
                  setDateRange={handleBacktestRangeChange}
                />
              </div>
            ) : null}
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
              <p className="text-xs text-slate-400">Create a strategy in the Strategies tab to unlock bot creation.</p>
            )}
          </div>
          <div className="lg:col-span-2">
            <button
              type="submit"
              className="inline-flex w-full items-center justify-center gap-2 rounded-2xl bg-[color:var(--accent-alpha-40)] px-4 py-3 text-sm font-semibold text-white disabled:opacity-40"
              disabled={
                !hasStrategies
                || !form.name
                || !form.strategy_ids.length
                || (form.run_type === 'backtest' && (!form.backtest_start || !form.backtest_end))
              }
            >
              <PlusCircle className="size-4" /> Create bot
            </button>
          </div>
        </form>
        <div className="rounded-2xl border border-white/10 bg-black/30 p-4 text-sm text-slate-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-[11px] uppercase tracking-[0.3em] text-slate-400">ATM override</p>
              <p className="text-xs text-slate-400">Optional custom contracts/targets for this bot run.</p>
            </div>
            <button
              type="button"
              onClick={toggleCustomATM}
              className={`inline-flex items-center rounded-full border px-3 py-1 text-[11px] uppercase tracking-[0.3em] ${form.use_custom_atm ? 'border-emerald-400/40 text-emerald-200' : 'border-white/20 text-slate-300'}`}
            >
              {form.use_custom_atm ? 'Disable override' : 'Use override'}
            </button>
          </div>
          {form.use_custom_atm ? (
            <div className="mt-3">
              <ATMConfigForm value={form.atm_template} onChange={handleATMTemplateChange} />
            </div>
          ) : (
            <p className="mt-3 text-xs text-slate-400">
              Bots will reuse each strategy's ATM template unless you enable an override.
            </p>
          )}
        </div>
      </header>

      {error ? (
        <div className="rounded-2xl border border-rose-500/40 bg-rose-500/5 p-4 text-sm text-rose-200">{error}</div>
      ) : null}

      <div className="space-y-3">
        {loading && sortedBots.length === 0 ? (
          <p className="text-sm text-slate-400">Loading bots…</p>
        ) : filteredBots.length === 0 ? (
          <p className="text-sm text-slate-400">
            {search.trim() ? 'No bots match your search.' : 'No bots yet. Create one to begin a backtest.'}
          </p>
        ) : (
          filteredBots.map((bot) => {
            const assignedNames = (bot.strategy_ids || [])
              .map((id) => strategyLookup.get(id)?.name || id)
              .filter(Boolean)
            const runtimeStatus = computeStatus(bot)
            const progressValue =
              typeof bot.runtime?.progress === 'number'
                ? bot.runtime.progress
                : runtimeStatus === 'completed'
                  ? 1
                  : 0
            const progressPct = `${Math.round(progressValue * 1000) / 10}%`
            const progressWidth = `${Math.min(100, Math.max(0, progressValue * 100))}%`
            const nextBarLabel =
              typeof bot.runtime?.next_bar_in_seconds === 'number'
                ? `${Math.max(0, Math.round(bot.runtime.next_bar_in_seconds))}s`
                : '—'
            const showPause = runtimeStatus === 'running' && bot.mode === 'walk-forward'
            const showResume = runtimeStatus === 'paused'
            const timeframeLabel = describeBotMeta(bot, 'timeframe')
            const datasourceLabel = describeBotMeta(bot, 'datasource')
            const exchangeLabel = describeBotMeta(bot, 'exchange')
            const summaryParts = [
              bot.mode,
              timeframeLabel ? `TF ${timeframeLabel}` : null,
              datasourceLabel ? `DS ${datasourceLabel}` : null,
              exchangeLabel ? `EX ${exchangeLabel}` : null,
              `speed ${playbackLabelFor(bot)}`,
              (bot.run_type || 'backtest').replace('_', ' '),
            ].filter(Boolean)
            const canStart = ['idle', 'stopped', 'completed', 'error'].includes(runtimeStatus)
            const canStop = ['running', 'paused', 'starting'].includes(runtimeStatus)
            const startLabel = runtimeStatus === 'completed' ? 'Rerun' : runtimeStatus === 'stopped' ? 'Restart' : 'Start'
            const keyStats = ['total_trades', 'wins', 'losses', 'win_rate']
            const statsEntries = keyStats
              .map((key) => ({ key, value: bot.last_stats?.[key] ?? bot.runtime?.stats?.[key] }))
              .filter((entry) => entry.value !== undefined && entry.value !== null)

            return (
              <article key={bot.id} className="flex flex-col gap-4 rounded-3xl border border-white/10 bg-white/5 p-5">
                <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                  <div>
                    <p className="text-sm uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">
                      {assignedNames.length ? assignedNames.join(', ') : 'No strategies assigned'}
                    </p>
                    <h4 className="text-xl font-semibold text-white">{bot.name}</h4>
                    <p className="text-xs text-slate-400">{summaryParts.join(' • ')}</p>
                    <p className="text-[11px] uppercase tracking-[0.35em] text-slate-500">{describeRange(bot)}</p>
                  </div>
                  {statusBadge(runtimeStatus)}
                </div>
                <div className="flex flex-wrap gap-3 text-[11px] uppercase tracking-[0.25em] text-slate-500">
                  <span>
                    Progress: <span className="text-slate-200">{progressPct}</span>
                  </span>
                  {bot.mode === 'walk-forward' ? (
                    <span>
                      Next bar: <span className="text-slate-200">{nextBarLabel}</span>
                    </span>
                  ) : null}
                </div>
                <div className="h-2 w-full rounded-full bg-white/5">
                  <div className="h-full rounded-full bg-emerald-500/60 transition-all" style={{ width: progressWidth }} />
                </div>
                {statsEntries.length ? (
                  <div className="flex flex-wrap gap-3 text-xs text-slate-300">
                    {statsEntries.map(({ key, value }) => (
                      <div key={key} className="rounded-2xl border border-white/10 bg-black/30 px-3 py-2">
                        <p className="text-[10px] uppercase tracking-[0.35em] text-slate-500">{key.replace(/_/g, ' ')}</p>
                        <p className="text-base font-semibold text-white">{value}</p>
                      </div>
                    ))}
                  </div>
                ) : null}
                <div className="flex flex-wrap items-center gap-2">
                  {canStart ? (
                    <button
                      type="button"
                      onClick={() => handleStart(bot.id)}
                      className="inline-flex items-center gap-2 rounded-full border border-emerald-500/30 px-4 py-2 text-sm text-emerald-200 hover:bg-emerald-500/10"
                    >
                      <Play className="size-4" /> {startLabel}
                    </button>
                  ) : null}
                  {canStop ? (
                    <button
                      type="button"
                      onClick={() => handleStop(bot.id)}
                      className="inline-flex items-center gap-2 rounded-full border border-rose-500/30 px-4 py-2 text-sm text-rose-200 hover:bg-rose-500/10"
                    >
                      <Square className="size-4" /> Stop
                    </button>
                  ) : null}
                  {showPause ? (
                    <button
                      type="button"
                      onClick={() => handlePause(bot.id)}
                      className="inline-flex items-center gap-2 rounded-full border border-amber-500/30 px-4 py-2 text-sm text-amber-200 hover:bg-amber-500/10"
                    >
                      <Pause className="size-4" /> Pause
                    </button>
                  ) : null}
                  {showResume ? (
                    <button
                      type="button"
                      onClick={() => handleResume(bot.id)}
                      className="inline-flex items-center gap-2 rounded-full border border-emerald-500/30 px-4 py-2 text-sm text-emerald-200 hover:bg-emerald-500/10"
                    >
                      <RotateCw className="size-4" /> Resume
                    </button>
                  ) : null}
                  <button
                    type="button"
                    onClick={() => setLensBot(bot)}
                    className="inline-flex items-center gap-2 rounded-full border border-white/20 px-4 py-2 text-sm text-slate-200 hover:bg-white/10"
                  >
                    <Eye className="size-4" /> Lens
                  </button>
                  <button
                    type="button"
                    onClick={() => handleDelete(bot.id)}
                    disabled={pendingDelete === bot.id}
                    className="inline-flex items-center gap-2 rounded-full border border-white/20 px-4 py-2 text-sm text-slate-200 hover:bg-white/10 disabled:opacity-40"
                  >
                    <Trash2 className="size-4" /> Delete
                  </button>
                </div>
              </article>
            )
          })
        )}
      </div>

      <BotPerformanceModal bot={lensBot} open={Boolean(lensBot)} onClose={() => setLensBot(null)} onRefresh={loadBots} />
    </section>
  )
}
