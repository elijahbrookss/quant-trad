import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { PlusCircle, RefreshCw, Search } from 'lucide-react'
import {
  listBots,
  createBot,
  startBot as startBotApi,
  stopBot as stopBotApi,
  deleteBot as deleteBotApi,
  pauseBot as pauseBotApi,
  resumeBot as resumeBotApi,
} from '../../adapters/bot.adapter.js'
import { fetchStrategies } from '../../adapters/strategy.adapter.js'
import { createLogger } from '../../utils/logger.js'
import { BotPerformanceModal } from './BotPerformanceModal.jsx'
import { BotCreateModal } from './create/BotCreateModal.jsx'
import { buildDefaultForm } from './create/botCreateFormDefaults.js'
import { useBotCreateForm } from './create/useBotCreateForm.js'
import { BotCard, sortBots } from './BotCard.jsx'
import { useBotStream } from './useBotStream.js'

const computeStatus = (bot) => (bot?.runtime?.status || bot?.status || 'idle').toLowerCase()

export function BotPanel() {
  const [bots, setBots] = useState([])
  const [loading, setLoading] = useState(false)
  const {
    form,
    walletConfig,
    walletError,
    handleChange,
    handleBacktestRangeChange,
    handleStrategyToggle,
    handleWalletBalanceChange,
    handleWalletBalanceAdd,
    handleWalletBalanceRemove,
    resetForm,
  } = useBotCreateForm(buildDefaultForm())
  const [createOpen, setCreateOpen] = useState(false)
  const [createError, setCreateError] = useState(null)
  const [lensBot, setLensBot] = useState(null)
  const [error, setError] = useState(null)
  const [strategies, setStrategies] = useState([])
  const [strategiesLoading, setStrategiesLoading] = useState(false)
  const [strategyError, setStrategyError] = useState(null)
  const [pendingDelete, setPendingDelete] = useState(null)
  const [pendingStart, setPendingStart] = useState(null)
  const [search, setSearch] = useState('')
  const [nowEpochMs, setNowEpochMs] = useState(() => Date.now())
  const logger = useMemo(() => createLogger('BotPanel'), [])
  const runtimeQueueRef = useRef(new Map())
  const runtimeFrame = useRef(null)
  const shallowEqualRuntime = useCallback((next = {}, prev = {}) => {
    if (next === prev) return true
    const keys = new Set([...Object.keys(next || {}), ...Object.keys(prev || {})])
    for (const key of keys) {
      if (next?.[key] !== prev?.[key]) return false
    }
    return true
  }, [])

  const mergeBots = useCallback(
    (incoming, options = {}) => {
      const { replace = false } = options || {}
      if (!Array.isArray(incoming)) return
      setBots((prev) => {
        const prevMap = new Map(prev.map((bot) => [bot.id, bot]))
        const next = replace ? [] : [...prev]
        const nextMap = new Map(next.map((bot) => [bot.id, bot]))
        let changed = false

        for (const bot of incoming) {
          if (!bot?.id) continue
          const current = nextMap.get(bot.id) || prevMap.get(bot.id)
          if (!current) {
            next.push(bot)
            nextMap.set(bot.id, bot)
            changed = true
            continue
          }
          const runtimeSame = shallowEqualRuntime(bot.runtime, current.runtime)
          const merged = {
            ...current,
            ...bot,
            runtime: runtimeSame
              ? current.runtime
              : { ...(current.runtime || {}), ...(bot.runtime || {}) },
          }
          const nonRuntimeChanged = Object.keys(bot || {}).some(
            (key) => key !== 'runtime' && bot[key] !== current[key],
          )
          if (!nonRuntimeChanged && runtimeSame) {
            continue
          }
          const index = next.findIndex((item) => item.id === merged.id)
          if (index !== -1) {
            next[index] = merged
          } else {
            next.push(merged)
          }
          nextMap.set(merged.id, merged)
          changed = true
        }
        return changed || replace ? next : prev
      })
    },
    [shallowEqualRuntime],
  )

  const upsertBot = useCallback(
    (payload) => {
      if (!payload?.id) return
      mergeBots([payload])
    },
    [logger, mergeBots],
  )

  const removeBot = useCallback((botId) => {
    if (!botId) return
    setBots((prev) => prev.filter((bot) => bot.id !== botId))
  }, [])

  const flushRuntimeQueue = useCallback(() => {
    setBots((prev) => {
      let nextState = prev
      runtimeQueueRef.current.forEach((runtime, botId) => {
        const index = nextState.findIndex((bot) => bot.id === botId)
        if (index === -1) return
        const bot = nextState[index]
        const mergedRuntime = { ...(bot.runtime || {}), ...runtime }
        if (shallowEqualRuntime(mergedRuntime, bot.runtime)) return
        if (nextState === prev) nextState = [...prev]
        nextState[index] = { ...bot, runtime: mergedRuntime }
      })
      runtimeQueueRef.current.clear()
      return nextState
    })
    runtimeFrame.current = null
  }, [shallowEqualRuntime])

  const applyRuntime = useCallback(
    (botId, runtime) => {
      if (!botId || !runtime) return
      const existing = runtimeQueueRef.current.get(botId) || {}
      runtimeQueueRef.current.set(botId, { ...existing, ...runtime })
      if (!runtimeFrame.current) {
        runtimeFrame.current = requestAnimationFrame(flushRuntimeQueue)
      }
    },
    [flushRuntimeQueue],
  )

  const loadBots = useCallback(
    async (withSpinner = true) => {
      if (withSpinner) setLoading(true)
      setError(null)
      logger.info('bots_load_start', { with_spinner: withSpinner })
      try {
        const data = await listBots()
        logger.info('bots_load_success', { count: Array.isArray(data) ? data.length : 0 })
        mergeBots(data, { replace: true })
      } catch (err) {
        logger.error('bots_load_failed', { message: err?.message }, err)
        setError(err?.message || 'Unable to load bots')
      } finally {
        if (withSpinner) setLoading(false)
      }
    },
    [mergeBots],
  )

  const loadStrategies = useCallback(async () => {
    setStrategiesLoading(true)
    setStrategyError(null)
    logger.info('strategies_load_start')
    try {
      const data = await fetchStrategies()
      setStrategies(data)
      logger.info('strategies_load_success', { count: Array.isArray(data) ? data.length : 0 })
    } catch (err) {
      logger.error('strategies_load_failed', { message: err?.message }, err)
      setStrategyError(err?.message || 'Unable to load strategies')
    } finally {
      setStrategiesLoading(false)
    }
  }, [logger])

  useEffect(() => {
    loadStrategies()
    logger.info('bot_panel_mounted')
  }, [loadStrategies, logger])

  useEffect(() => {
    logger.info('bot_create_modal_state', { open: createOpen })
  }, [createOpen, logger])

  useEffect(() => {
    if (createOpen) {
      logger.debug('bot_panel_tick_paused', { reason: 'create_modal_open' })
      return undefined
    }
    logger.debug('bot_panel_tick_started')
    const timer = setInterval(() => setNowEpochMs(Date.now()), 1000)
    return () => {
      clearInterval(timer)
      logger.debug('bot_panel_tick_stopped')
    }
  }, [createOpen, logger])

  useEffect(() => () => {
    if (runtimeFrame.current) cancelAnimationFrame(runtimeFrame.current)
  }, [])

  const { state: botStreamState, reconnect: reconnectBotStream } = useBotStream({
    mergeBots,
    upsertBot,
    removeBot,
    applyRuntime,
    loadBots,
  })

  const closeCreateModal = useCallback(() => {
    setCreateOpen(false)
    setCreateError(null)
  }, [])

  const handleCreate = async (event) => {
    event.preventDefault()
    setError(null)
    setCreateError(null)
    if (!form.name) return
    if (!form.strategy_ids.length) {
      setCreateError('Select at least one strategy for this bot.')
      return
    }
    if (form.run_type === 'backtest' && (!form.backtest_start || !form.backtest_end)) {
      setCreateError('Provide both a start and end date for backtests.')
      return
    }
    if (walletError || !walletConfig) {
      setCreateError(walletError || 'Wallet config is required.')
      return
    }
    const startISO = form.backtest_start ? new Date(form.backtest_start).toISOString() : undefined
    const endISO = form.backtest_end ? new Date(form.backtest_end).toISOString() : undefined
    const normalizedMode = form.run_type === 'backtest' ? form.mode : 'walk-forward'
    logger.info('bot_create_request', {
      run_type: form.run_type,
      mode: normalizedMode,
      strategy_count: form.strategy_ids.length,
      strategy_ids: form.strategy_ids,
      backtest_start: startISO,
      backtest_end: endISO,
    })
    try {
      const { wallet_balances, ...rest } = form
      const payloadBody = {
        ...rest,
        mode: normalizedMode,
        backtest_start: form.run_type === 'backtest' ? startISO : undefined,
        backtest_end: form.run_type === 'backtest' ? endISO : undefined,
        wallet_config: walletConfig,
      }
      const payload = await createBot(payloadBody)
      logger.info('bot_create_success', { bot_id: payload?.id })
      upsertBot(payload)
      resetForm({ strategy_ids: form.strategy_ids, run_type: form.run_type })
      closeCreateModal()
    } catch (err) {
      logger.error('bot_create_failed', { message: err?.message }, err)
      setCreateError(err?.message || 'Unable to create bot')
    }
  }

  const handleStart = async (botId) => {
    setError(null)
    const target = bots.find((bot) => bot.id === botId)
    if (!target?.strategy_ids?.length) {
      setError('Assign at least one strategy before starting the bot.')
      return
    }
    logger.info('bot_start_requested', { bot_id: botId })
    setPendingStart(botId)
    setBots((prev) =>
      prev.map((bot) =>
        bot.id === botId
          ? {
              ...bot,
              runtime: { ...(bot.runtime || {}), status: 'starting' },
            }
          : bot,
      ),
    )
    try {
      const payload = await startBotApi(botId)
      upsertBot(payload)
      loadBots(false)
    } catch (err) {
      logger.error('bot_start_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to start bot')
    } finally {
      setPendingStart(null)
    }
  }

  const handleStop = async (botId) => {
    setError(null)
    logger.info('bot_stop_requested', { bot_id: botId })
    try {
      const payload = await stopBotApi(botId)
      upsertBot(payload)
      loadBots(false)
    } catch (err) {
      logger.error('bot_stop_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to stop bot')
    }
  }

  const handlePause = async (botId) => {
    setError(null)
    logger.info('bot_pause_requested', { bot_id: botId })
    try {
      const payload = await pauseBotApi(botId)
      upsertBot(payload)
      loadBots(false)
    } catch (err) {
      logger.error('bot_pause_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to pause bot')
    }
  }

  const handleResume = async (botId) => {
    setError(null)
    logger.info('bot_resume_requested', { bot_id: botId })
    try {
      const payload = await resumeBotApi(botId)
      upsertBot(payload)
      loadBots(false)
    } catch (err) {
      logger.error('bot_resume_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to resume bot')
    }
  }

  const handleDelete = async (botId) => {
    if (!botId) return
    if (!window.confirm('Delete this bot? This cannot be undone.')) return
    setError(null)
    logger.info('bot_delete_requested', { bot_id: botId })
    setPendingDelete(botId)
    try {
      await deleteBotApi(botId)
      setBots((prev) => prev.filter((bot) => bot.id !== botId))
    } catch (err) {
      logger.error('bot_delete_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to delete bot')
    } finally {
      setPendingDelete(null)
    }
  }

  const statusBadge = useCallback((status) => {
    const config = {
      running: { color: 'bg-emerald-500/10 text-emerald-300 border-emerald-800/50', dot: 'bg-emerald-400' },
      paused: { color: 'bg-amber-500/10 text-amber-300 border-amber-800/50', dot: 'bg-amber-400' },
      stopped: { color: 'bg-rose-500/10 text-rose-300 border-rose-800/50', dot: 'bg-rose-400' },
      crashed: { color: 'bg-rose-500/10 text-rose-300 border-rose-800/50', dot: 'bg-rose-400' },
      completed: { color: 'bg-sky-500/10 text-sky-300 border-sky-800/50', dot: 'bg-sky-400' },
      starting: { color: 'bg-slate-700/30 text-slate-400 border-slate-700/50', dot: 'bg-slate-500' },
      idle: { color: 'bg-slate-800/40 text-slate-500 border-slate-800', dot: 'bg-slate-600' },
    }
    const { color, dot } = config[status] || config.idle
    return (
      <span className={`inline-flex items-center gap-1.5 rounded border px-2 py-1 text-[10px] font-medium uppercase tracking-wider ${color}`}>
        <span className={`h-1.5 w-1.5 rounded-full ${dot}`} />
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

    const sortedBots = useMemo(() => sortBots(bots), [bots])

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

  return (
    <section className="space-y-5">
      <header className="space-y-4">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="space-y-1.5">
            <div className="flex items-baseline gap-3">
              <h3 className="text-2xl font-medium tracking-tight text-slate-50">Bots</h3>
              <span className="text-xs font-medium tabular-nums text-slate-500">
                {strategiesLoading ? 'Loading…' : `${strategies.length} ${strategies.length === 1 ? 'strategy' : 'strategies'}`}
              </span>
            </div>
            <p className="text-sm leading-relaxed text-slate-400">
              Monitor and control walk-forward backtests across all configured strategies
            </p>
          </div>
          <button
            type="button"
            onClick={() => {
              logger.info('bot_create_modal_open')
              setCreateError(null)
              setCreateOpen(true)
            }}
            className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/50 px-4 py-2.5 text-sm font-medium text-slate-200 backdrop-blur-sm transition-colors hover:border-slate-600 hover:bg-slate-800 hover:text-slate-50 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-slate-500"
          >
            <PlusCircle className="size-4" /> New Bot
          </button>
        </div>

        <div className="flex flex-col gap-3 rounded-lg border border-slate-800 bg-slate-900/50 p-3 backdrop-blur-sm lg:flex-row lg:items-center lg:justify-between">
          <div className="flex flex-1 flex-wrap items-center gap-2.5">
            <label className="flex min-w-[240px] flex-1 items-center gap-2.5 rounded-md border border-slate-800 bg-slate-950/80 px-3 py-2 text-slate-200 focus-within:border-slate-700 focus-within:bg-slate-950">
              <Search className="size-3.5 shrink-0 text-slate-600" />
              <input
                type="search"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Filter by name, strategy, or status…"
                className="min-w-0 flex-1 bg-transparent text-sm text-slate-200 placeholder:text-slate-600 focus:outline-none"
              />
            </label>
            <button
              type="button"
              onClick={() => {
                if (botStreamState === 'open') {
                  logger.info('bot_stream_refresh_requested')
                  reconnectBotStream()
                  return
                }
                loadBots()
              }}
              className="inline-flex items-center gap-2 rounded-md border border-slate-800 bg-slate-950/80 px-3.5 py-2 text-sm font-medium text-slate-400 transition-colors hover:border-slate-700 hover:bg-slate-950 hover:text-slate-300 disabled:cursor-not-allowed disabled:opacity-50"
              disabled={loading}
            >
              <RefreshCw className={`size-3.5 ${loading ? 'animate-spin' : ''}`} />
              <span className="hidden sm:inline">Refresh</span>
            </button>
          </div>
          <div className="flex items-center gap-2 text-xs tabular-nums">
            <span className="font-medium text-slate-400">{filteredBots.length}</span>
            <span className="text-slate-600">of</span>
            <span className="font-medium text-slate-500">{sortedBots.length}</span>
          </div>
        </div>
      </header>

      {error ? (
        <div className="rounded-lg border border-rose-900/50 bg-rose-950/20 px-4 py-3 text-sm text-rose-300">
          {error}
        </div>
      ) : null}

      <div className="space-y-2.5">
        {loading && sortedBots.length === 0 ? (
          <div className="rounded-lg border border-slate-800 bg-slate-900/40 px-4 py-8 text-center">
            <p className="text-sm text-slate-500">Loading bots…</p>
          </div>
        ) : filteredBots.length === 0 ? (
          <div className="rounded-lg border border-slate-800 bg-slate-900/40 px-4 py-8 text-center">
            <p className="text-sm text-slate-400">
              {search.trim() ? 'No bots match your filter.' : 'No bots configured.'}
            </p>
            {!search.trim() ? (
              <p className="mt-1 text-xs text-slate-600">Create your first bot to begin backtesting.</p>
            ) : null}
          </div>
        ) : (
            filteredBots.map((bot) => (
              <BotCard
                key={bot.id}
                bot={bot}
                strategyLookup={strategyLookup}
                describeRange={describeRange}
                statusBadge={statusBadge}
                nowEpochMs={nowEpochMs}
                onStart={handleStart}
                onStop={handleStop}
                onPause={handlePause}
                onResume={handleResume}
                onDelete={handleDelete}
                onOpen={setLensBot}
                pendingStart={pendingStart}
                pendingDelete={pendingDelete}
              />
            ))
          )}
        </div>

        <BotPerformanceModal bot={lensBot} open={Boolean(lensBot)} onClose={() => setLensBot(null)} onRefresh={loadBots} />
        <BotCreateModal
          open={createOpen}
          onClose={closeCreateModal}
          form={form}
          strategies={sortedStrategies}
          strategiesLoading={strategiesLoading}
          strategyError={strategyError}
          walletError={walletError}
          onSubmit={handleCreate}
          onChange={handleChange}
          onBacktestRangeChange={handleBacktestRangeChange}
          onStrategyToggle={handleStrategyToggle}
          onWalletBalanceChange={handleWalletBalanceChange}
          onWalletBalanceAdd={handleWalletBalanceAdd}
          onWalletBalanceRemove={handleWalletBalanceRemove}
          error={createError}
        />
      </section>
    )
  }
