import { useCallback, useEffect, useMemo, useState } from 'react'
import { PlusCircle, RefreshCw, Search } from 'lucide-react'
import {
  listBots,
  fetchBotRuntimeCapacity,
  startBot as startBotApi,
  stopBot as stopBotApi,
  deleteBot as deleteBotApi,
} from '../../adapters/bot.adapter.js'
import { fetchStrategies, fetchStrategy } from '../../adapters/strategy.adapter.js'
import { createLogger } from '../../utils/logger.js'
import ActionButton from '../strategy/ui/ActionButton.jsx'
import { BotCreateModal } from './create/BotCreateModal.jsx'
import { BotLensLiveModal } from './BotLensLiveModal.jsx'
import { BotDiagnosticsModal } from './BotDiagnosticsModal.jsx'
import { useBotCreateController } from './create/useBotCreateController.js'
import { getBotCardDisplayState } from './botStatusModel.js'
import { buildBotFleetSummary } from './botControlSurfaceModel.js'
import { BotCard, sortBots } from './BotCard.jsx'
import { useBotStream } from './useBotStream.js'

export function BotPanel() {
  const [bots, setBots] = useState([])
  const [loading, setLoading] = useState(false)
  const [createOpen, setCreateOpen] = useState(false)
  const [createError, setCreateError] = useState(null)
  const [lensBotId, setLensBotId] = useState(null)
  const [diagnosticsBotId, setDiagnosticsBotId] = useState(null)
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

  const upsertStrategy = useCallback((incoming) => {
    if (!incoming?.id) {
      return null
    }
    setStrategies((prev) => {
      const next = Array.isArray(prev) ? [...prev] : []
      const index = next.findIndex((strategy) => strategy?.id === incoming.id)
      if (index === -1) {
        next.push(incoming)
        return next
      }
      next[index] = { ...next[index], ...incoming }
      return next
    })
    return incoming
  }, [])

  const loadStrategyDetail = useCallback(
    async (strategyId) => {
      if (!strategyId) {
        return null
      }
      const detail = await fetchStrategy(strategyId)
      return upsertStrategy(detail)
    },
    [upsertStrategy],
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
    fetchStrategyDetail: loadStrategyDetail,
    logger,
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
    loadBots()
    logger.info('bot_panel_mounted')
  }, [loadBots, loadStrategies, logger])

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

  const { state: botStreamState, reconnect: reconnectBotStream } = useBotStream({
    mergeBots,
    upsertBot,
    removeBot,
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
    // Keep fleet control responsive while the authoritative lifecycle stream catches up.
    setBots((prev) =>
      prev.map((bot) =>
        bot.id === botId
          ? {
              ...bot,
              status: 'starting',
              lifecycle: {
                ...(bot.lifecycle || {}),
                status: 'starting',
                phase: 'preparing_run',
                reason: 'container_start_pending',
                message: 'Preparing run',
                updated_at: new Date().toISOString(),
              },
              controls: {
                ...(bot.controls || {}),
                can_start: false,
                can_stop: false,
                can_delete: false,
                start_label: 'Starting',
              },
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
      loadBots(false)
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
      if (diagnosticsBotId === botId) setDiagnosticsBotId(null)
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

  const lensBot = useMemo(() => {
    const selectedBot = bots.find((bot) => bot.id === lensBotId) || null
    if (!selectedBot) return null
    const display = getBotCardDisplayState(selectedBot, {
      nowEpochMs,
      pendingStart: pendingStart === selectedBot.id,
    })
    return display.controls.canOpenLens ? selectedBot : null
  }, [bots, lensBotId, nowEpochMs, pendingStart])

  const diagnosticsBot = useMemo(
    () => bots.find((bot) => bot.id === diagnosticsBotId) || null,
    [bots, diagnosticsBotId],
  )

  useEffect(() => {
    if (lensBotId && !lensBot) {
      setLensBotId(null)
    }
  }, [lensBot, lensBotId])

  useEffect(() => {
    if (diagnosticsBotId && !diagnosticsBot) {
      setDiagnosticsBotId(null)
    }
  }, [diagnosticsBot, diagnosticsBotId])

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

  const fleetSummary = useMemo(
    () => buildBotFleetSummary(sortedBots, { nowEpochMs, pendingStartId: pendingStart }),
    [nowEpochMs, pendingStart, sortedBots],
  )

  return (
    <section className="mx-auto max-w-[1320px] space-y-4">
      <header className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold text-white">Monitor runs and intervene quickly</h2>
            <p className="mt-0.5 text-xs text-slate-500">
              Keep startup, live runtime, and failures visible from one control surface.
            </p>
          </div>
          <ActionButton
            variant="ghost"
            className="inline-flex items-center"
            onClick={async () => {
              logger.info('bot_create_modal_open')
              setCreateError(null)
              try {
                await prepareForCreate({
                  strategyId: form.strategy_id || '',
                  variantId: form.strategy_variant_id || '',
                  runType: form.run_type || 'backtest',
                })
                setCreateOpen(true)
              } catch (err) {
                logger.error('bot_create_prepare_failed', { message: err?.message }, err)
                setCreateError(err?.message || 'Unable to prepare bot create form')
              }
            }}
          >
            <PlusCircle className="mr-1 size-3.5" /> New Bot
          </ActionButton>
        </div>

        <div className="flex flex-wrap items-center gap-2 rounded-lg border border-white/[0.06] bg-black/30 px-3 py-2">
          {fleetSummary.items.map((item) => (
            <SummaryPill key={item.key} label={item.label} value={item.value} />
          ))}
          {runtimeCapacity ? (
            <SummaryPill
              label="CPU"
              value={`${Number(runtimeCapacity.workers_in_use || 0)}/${Number(runtimeCapacity.host_cpu_cores || 0)}`}
            />
          ) : null}
        </div>

        <div className="flex flex-col gap-2 rounded-lg border border-white/[0.06] bg-black/20 px-3 py-2 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex flex-1 flex-wrap items-center gap-2">
            <label className="flex min-w-[240px] flex-1 items-center gap-2 rounded-md border border-white/[0.06] bg-black/40 px-3 py-2 text-slate-200 focus-within:border-white/[0.12] focus-within:bg-black/50">
              <Search className="size-3.5 shrink-0 text-slate-600" />
              <input
                type="search"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Filter by name, strategy, or status…"
                className="min-w-0 flex-1 bg-transparent text-sm text-slate-200 placeholder:text-slate-600 focus:outline-none"
              />
            </label>
            <ActionButton
              variant="ghost"
              className="inline-flex items-center disabled:cursor-not-allowed disabled:opacity-50"
              onClick={() => {
                if (botStreamState === 'open') {
                  logger.info('bot_stream_refresh_requested')
                  reconnectBotStream()
                  return
                }
                loadBots()
              }}
              disabled={loading}
            >
              <RefreshCw className={`size-3.5 ${loading ? 'animate-spin' : ''}`} />
              <span className="hidden sm:inline">Refresh</span>
            </ActionButton>
          </div>
          <div className="flex items-center gap-2 text-[11px] tabular-nums text-slate-500">
            <span>{filteredBots.length}</span>
            <span className="text-slate-700">shown</span>
            <span className="text-slate-700">/</span>
            <span>{sortedBots.length}</span>
            {runtimeCapacity ? (
              <span className="text-slate-700">
                • {Number(runtimeCapacity.running_bots || 0)} active
              </span>
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
                onOpenLens={(selectedBot) => setLensBotId(selectedBot?.id || null)}
                onOpenDiagnostics={(selectedBot) => setDiagnosticsBotId(selectedBot?.id || null)}
                pendingStart={pendingStart}
                pendingDelete={pendingDelete}
              />
            ))
          )}
        </div>

        <BotLensLiveModal bot={lensBot} open={Boolean(lensBot)} onClose={() => setLensBotId(null)} />
        <BotDiagnosticsModal bot={diagnosticsBot} open={Boolean(diagnosticsBot)} onClose={() => setDiagnosticsBotId(null)} />
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

function SummaryPill({ label, value }) {
  return (
    <div className="inline-flex items-center gap-2 rounded-md border border-white/[0.06] bg-black/35 px-2.5 py-1.5">
      <span className="qt-mono text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500">{label}</span>
      <span className="qt-mono text-[12px] font-semibold text-slate-200">{value}</span>
    </div>
  )
}
