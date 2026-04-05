import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { PlusCircle, RefreshCw, Search } from 'lucide-react'
import {
  listBots,
  fetchBotRuntimeCapacity,
  startBot as startBotApi,
  stopBot as stopBotApi,
  deleteBot as deleteBotApi,
} from '../../adapters/bot.adapter.js'
import { fetchStrategiesWithVariants } from '../../adapters/strategy.adapter.js'
import { createLogger } from '../../utils/logger.js'
import { BotCreateModal } from './create/BotCreateModal.jsx'
import { BotLensLiveModal } from './BotLensLiveModal.jsx'
import { useBotCreateController } from './create/useBotCreateController.js'
import { getBotStatus } from './botStatusModel.js'
import { BotCard, sortBots } from './BotCard.jsx'
import { useBotStream } from './useBotStream.js'
import { usePortalSettings } from '../../contexts/PortalSettingsContext.jsx'

export function BotPanel() {
  const [bots, setBots] = useState([])
  const [loading, setLoading] = useState(false)
  const { settings } = usePortalSettings()
  const [createOpen, setCreateOpen] = useState(false)
  const [createError, setCreateError] = useState(null)
  const [lensBotId, setLensBotId] = useState(null)
  const [error, setError] = useState(null)
  const [strategies, setStrategies] = useState([])
  const [strategiesLoading, setStrategiesLoading] = useState(false)
  const [strategyError, setStrategyError] = useState(null)
  const [pendingDelete, setPendingDelete] = useState(null)
  const [pendingStart, setPendingStart] = useState(null)
  const [search, setSearch] = useState('')
  const [runtimeCapacity, setRuntimeCapacity] = useState(null)
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
    [mergeBots],
  )

  const {
    form,
    walletError,
    handleChange,
    handleBacktestRangeChange,
    handleStrategySelect,
    handleVariantSelect,
    handleWalletBalanceChange,
    handleWalletBalanceAdd,
    handleWalletBalanceRemove,
    prepareForCreate,
    submit: submitCreate,
  } = useBotCreateController({
    strategies,
    logger,
    defaults: {
      snapshotIntervalMs: Number(settings?.botDefaults?.snapshotIntervalMs || 1000),
      envText: settings?.botDefaults?.envText || '',
    },
    onCreated: (payload) => {
      upsertBot(payload)
      setCreateOpen(false)
      setCreateError(null)
    },
  })

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
        const nextStatus = String(runtime?.status || '').trim().toLowerCase()
        const nextLifecycle = { ...(bot.lifecycle || {}) }
        const nextControls = { ...(bot.controls || {}) }
        if (nextStatus) {
          nextLifecycle.status = nextStatus
          if (nextStatus === 'running') {
            nextLifecycle.phase = Number(runtime?.seq || 0) > 0 ? 'live' : 'awaiting_snapshot'
            nextLifecycle.reason = Number(runtime?.seq || 0) > 0 ? 'live_runtime' : 'awaiting_first_snapshot'
            nextLifecycle.live = Number(runtime?.seq || 0) > 0
            nextControls.can_start = false
            nextControls.can_stop = true
          } else if (nextStatus === 'starting') {
            nextLifecycle.phase = 'starting_container'
            nextLifecycle.reason = 'container_start_pending'
            nextLifecycle.live = false
            nextControls.can_start = false
            nextControls.can_stop = true
          } else if (['completed', 'stopped'].includes(nextStatus)) {
            nextLifecycle.phase = nextStatus === 'completed' ? 'completed' : 'stopped'
            nextLifecycle.reason = nextStatus === 'completed' ? 'run_completed' : 'run_stopped'
            nextLifecycle.live = false
            nextControls.can_start = true
            nextControls.can_stop = false
          } else if (['degraded', 'telemetry_degraded'].includes(nextStatus)) {
            nextLifecycle.phase = 'degraded'
            nextLifecycle.reason = 'runtime_degraded'
            nextLifecycle.live = true
            nextControls.can_start = true
            nextControls.can_stop = true
          } else if (['error', 'failed', 'crashed'].includes(nextStatus)) {
            nextLifecycle.phase = 'failed'
            nextLifecycle.reason = 'runtime_failed'
            nextLifecycle.live = false
            nextControls.can_start = true
            nextControls.can_stop = false
          }
        }
        if (nextState === prev) nextState = [...prev]
        nextState[index] = {
          ...bot,
          status: nextStatus || bot.status,
          active_run_id: runtime?.run_id || bot.active_run_id,
          controls: nextControls,
          lifecycle: nextLifecycle,
          runtime: mergedRuntime,
        }
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

  const loadRuntimeCapacity = useCallback(async () => {
    try {
      const payload = await fetchBotRuntimeCapacity()
      setRuntimeCapacity(payload && typeof payload === 'object' ? payload : null)
    } catch (err) {
      logger.warn('bot_runtime_capacity_load_failed', { message: err?.message }, err)
    }
  }, [logger])

  const loadBots = useCallback(
    async (withSpinner = true) => {
      if (withSpinner) setLoading(true)
      setError(null)
      logger.info('bots_load_start', { with_spinner: withSpinner })
      try {
        const data = await listBots()
        logger.info('bots_load_success', { count: Array.isArray(data) ? data.length : 0 })
        mergeBots(data, { replace: true })
        await loadRuntimeCapacity()
      } catch (err) {
        logger.error('bots_load_failed', { message: err?.message }, err)
        setError(err?.message || 'Unable to load bots')
      } finally {
        if (withSpinner) setLoading(false)
      }
    },
    [loadRuntimeCapacity, logger, mergeBots],
  )

  const loadStrategies = useCallback(async () => {
    setStrategiesLoading(true)
    setStrategyError(null)
    logger.info('strategies_load_start')
    try {
      const data = await fetchStrategiesWithVariants({
        onVariantError: (strategyId, err) => {
          logger.warn('bot_panel_strategy_variants_load_failed', { strategyId, message: err?.message || err })
        },
      })
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
    loadRuntimeCapacity()
  }, [loadRuntimeCapacity])

  useEffect(() => {
    const timer = setInterval(() => {
      loadRuntimeCapacity()
    }, 15000)
    return () => clearInterval(timer)
  }, [loadRuntimeCapacity])

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
    setError(null)
    setCreateError(null)
    try {
      await submitCreate(event)
    } catch (err) {
      logger.error('bot_create_failed', { message: err?.message }, err)
      setCreateError(err?.message || 'Unable to create bot')
    }
  }

  const handleStart = async (botId) => {
    setError(null)
    const target = bots.find((bot) => bot.id === botId)
    if (!target?.strategy_id) {
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
              status: 'starting',
              controls: {
                ...(bot.controls || {}),
                can_start: false,
                can_stop: true,
                start_label: 'Starting',
              },
              lifecycle: {
                ...(bot.lifecycle || {}),
                status: 'starting',
                phase: 'starting_container',
                reason: 'container_start_pending',
                live: false,
              },
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
      setBots((prev) =>
        prev.map((bot) =>
          bot.id === botId
            ? {
                ...bot,
                status: 'error',
                controls: {
                  ...(bot.controls || {}),
                  can_start: true,
                  can_stop: false,
                  start_label: 'Restart',
                },
                lifecycle: {
                  ...(bot.lifecycle || {}),
                  status: 'error',
                  phase: 'failed',
                  reason: 'runtime_failed',
                  live: false,
                },
                last_run_artifact: {
                  ...(bot.last_run_artifact || {}),
                  error: { message: err?.message || 'Container start failed', phase: 'container_start' },
                },
                runtime: {
                  ...(bot.runtime || {}),
                  status: 'error',
                  error: { message: err?.message || 'Container start failed', phase: 'container_start' },
                },
              }
            : bot,
        ),
      )
    } finally {
      setPendingStart(null)
    }
  }

  const handleStop = async (botId) => {
    setError(null)
    logger.info('bot_stop_requested', { bot_id: botId })
    setBots((prev) =>
      prev.map((bot) =>
        bot.id === botId
          ? {
              ...bot,
              controls: {
                ...(bot.controls || {}),
                can_start: false,
                can_stop: false,
              },
            }
          : bot,
      ),
    )
    try {
      const payload = await stopBotApi(botId)
      upsertBot(payload)
      loadBots(false)
    } catch (err) {
      logger.error('bot_stop_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to stop bot')
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
      if (lensBotId === botId) setLensBotId(null)
      setBots((prev) => prev.filter((bot) => bot.id !== botId))
    } catch (err) {
      logger.error('bot_delete_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to delete bot')
    } finally {
      setPendingDelete(null)
    }
  }

  const formatDate = (value) => {
    if (!value) return '—'
    try {
      return new Date(value).toLocaleString()
    } catch {
      return value
    }
  }

  const describeRange = useCallback((bot) => {
    const runType = (bot?.run_type || '').toLowerCase()
    if (runType === 'backtest') {
      return `${formatDate(bot?.backtest_start)} → ${formatDate(bot?.backtest_end)}`
    }
    if (runType === 'paper' || runType === 'sim_trade') {
      return 'Paper execution'
    }
    if (runType === 'live') {
      return 'Live execution'
    }
    return 'Execution run'
  }, [])

    const sortedBots = useMemo(() => sortBots(bots), [bots])

  const sortedStrategies = useMemo(() => {
    return [...strategies].sort((a, b) => (a.name || '').localeCompare(b.name || ''))
  }, [strategies])

  const lensBot = useMemo(
    () => bots.find((bot) => bot.id === lensBotId) || null,
    [bots, lensBotId],
  )

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
      const assignedNames = bot.strategy_id ? (strategyLookup.get(bot.strategy_id)?.name || bot.strategy_id) : ''
      const haystack = [
        bot.name,
        getBotStatus(bot),
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
  }, [describeRange, search, sortedBots, strategyLookup])

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
              Monitor and control backtest, paper, and live-ready bots across all configured strategies
            </p>
          </div>
          <button
            type="button"
            onClick={() => {
              logger.info('bot_create_modal_open')
              setCreateError(null)
              prepareForCreate({
                strategyId: form.strategy_id || '',
                variantId: form.strategy_variant_id || '',
                runType: form.run_type || 'backtest',
              })
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
          <div className="flex flex-col items-end gap-1 text-xs tabular-nums">
            <div className="flex items-center gap-2">
              <span className="font-medium text-slate-400">{filteredBots.length}</span>
              <span className="text-slate-600">of</span>
              <span className="font-medium text-slate-500">{sortedBots.length}</span>
            </div>
            {runtimeCapacity ? (
              <div className="text-[11px] text-slate-600">
                <span>
                  CPU {Number(runtimeCapacity.workers_in_use || 0)}/{Number(runtimeCapacity.host_cpu_cores || 0)}
                </span>
                <span className="ml-1 text-slate-700">({Number(runtimeCapacity.in_use_pct || 0)}%)</span>
                <span className="mx-1.5 text-slate-700">•</span>
                <span>{Number(runtimeCapacity.running_bots || 0)} running</span>
              </div>
            ) : null}
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
              <p className="mt-1 text-xs text-slate-600">Create your first bot to begin running a strategy.</p>
            ) : null}
          </div>
        ) : (
            filteredBots.map((bot) => (
              <BotCard
                key={bot.id}
                bot={bot}
                strategyLookup={strategyLookup}
                nowEpochMs={nowEpochMs}
                onStart={handleStart}
                onStop={handleStop}
                onDelete={handleDelete}
                onOpen={(selectedBot) => setLensBotId(selectedBot?.id || null)}
                pendingStart={pendingStart}
                pendingDelete={pendingDelete}
              />
            ))
          )}
        </div>

        <BotLensLiveModal bot={lensBot} open={Boolean(lensBot)} onClose={() => setLensBotId(null)} />
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
          onStrategySelect={handleStrategySelect}
          onVariantSelect={handleVariantSelect}
          onWalletBalanceChange={handleWalletBalanceChange}
          onWalletBalanceAdd={handleWalletBalanceAdd}
          onWalletBalanceRemove={handleWalletBalanceRemove}
          error={createError}
        />
      </section>
    )
  }
