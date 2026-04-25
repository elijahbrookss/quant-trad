import { useCallback, useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import {
  deleteBot as deleteBotApi,
  fetchBotRuntimeCapacity,
  listBots,
  startBot as startBotApi,
  stopBot as stopBotApi,
} from '../../../adapters/bot.adapter.js'
import { fetchStrategies, fetchStrategy } from '../../../adapters/strategy.adapter.js'
import { buildBotFleetSummary, sortBots } from '../fleet/buildBotFleetViewModel.js'
import {
  getBotCardDisplayState,
  getBotStatus,
} from '../state/botRuntimeStatus.js'
import { mapRunToViewModel } from '../viewModels/runViewModel.js'
import { useBotCreateController } from '../create/useBotCreateController.js'
import { useBotStream } from '../../../components/bots/useBotStream.js'
import { createLogger } from '../../../utils/logger.js'

function formatDate(value) {
  if (!value) return '—'
  try {
    return new Date(value).toLocaleString()
  } catch {
    return value
  }
}

function mergeBotRuntimeState(currentRuntime = {}, nextRuntime = {}) {
  return { ...(currentRuntime || {}), ...(nextRuntime || {}) }
}

export function replaceFleetBotsSnapshot(incoming) {
  return Array.isArray(incoming) ? incoming.filter((bot) => bot?.id) : []
}

export function upsertFleetBotRecord(previousBots, incomingBot) {
  if (!incomingBot?.id) return Array.isArray(previousBots) ? previousBots : []
  const currentBots = Array.isArray(previousBots) ? previousBots : []
  const currentIndex = currentBots.findIndex((bot) => bot?.id === incomingBot.id)
  if (currentIndex === -1) {
    return [...currentBots, incomingBot]
  }

  const currentBot = currentBots[currentIndex]
  const nextBot = {
    ...currentBot,
    ...incomingBot,
    runtime: mergeBotRuntimeState(currentBot?.runtime, incomingBot?.runtime),
  }
  const nextBots = [...currentBots]
  nextBots[currentIndex] = nextBot
  return nextBots
}

export function mergeFleetBotRuntime(previousBots, botId, runtime) {
  if (!botId || !runtime || typeof runtime !== 'object') {
    return Array.isArray(previousBots) ? previousBots : []
  }
  const currentBots = Array.isArray(previousBots) ? previousBots : []
  let changed = false
  const nextBots = currentBots.map((bot) => {
    if (bot?.id !== botId) return bot
    changed = true
    return {
      ...bot,
      runtime: mergeBotRuntimeState(bot?.runtime, runtime),
    }
  })
  return changed ? nextBots : currentBots
}

export function removeFleetBotRecord(previousBots, botId) {
  if (!botId) return Array.isArray(previousBots) ? previousBots : []
  const currentBots = Array.isArray(previousBots) ? previousBots : []
  const nextBots = currentBots.filter((bot) => bot?.id !== botId)
  return nextBots.length === currentBots.length ? currentBots : nextBots
}

export async function initializeBotsPageRuntime({ loadStrategies, logger }) {
  await loadStrategies()
  logger.info('bots_page_mounted')
}

export async function runManualFleetRefresh({
  replaceFleetBots,
  requestFleetSnapshot,
  loadRuntimeCapacity,
  logger,
  markFleetHydrated,
  setError,
  setRefreshing,
}) {
  setRefreshing(true)
  setError(null)
  logger.info('bots_manual_refresh_start')
  try {
    const data = await requestFleetSnapshot()
    replaceFleetBots(data)
    markFleetHydrated()
    await loadRuntimeCapacity()
    logger.info('bots_manual_refresh_success', { count: Array.isArray(data) ? data.length : 0 })
  } catch (err) {
    logger.error('bots_manual_refresh_failed', { message: err?.message }, err)
    setError(err?.message || 'Unable to refresh bots')
  } finally {
    setRefreshing(false)
  }
}

export async function runBotStartAction({
  bot,
  botId,
  logger,
  loadRuntimeCapacity,
  startBot,
}) {
  if (!bot?.strategy_id) {
    throw new Error('Assign at least one strategy before starting the bot.')
  }
  logger.info('bot_start_requested', { bot_id: botId })
  const startedBot = await startBot(botId)
  await loadRuntimeCapacity()
  return startedBot
}

export async function runBotStopAction({
  botId,
  logger,
  loadRuntimeCapacity,
  stopBot,
}) {
  logger.info('bot_stop_requested', { bot_id: botId })
  await stopBot(botId)
  await loadRuntimeCapacity()
}

export function useBotsPageController() {
  const [bots, setBots] = useState([])
  const [refreshingFleet, setRefreshingFleet] = useState(false)
  const [hasFleetSnapshot, setHasFleetSnapshot] = useState(false)
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
  const [pendingStop, setPendingStop] = useState(null)
  const [search, setSearch] = useState('')
  const [runtimeCapacity, setRuntimeCapacity] = useState(null)
  const [nowEpochMs, setNowEpochMs] = useState(() => Date.now())
  const navigate = useNavigate()
  const logger = useMemo(() => createLogger('BotsPage'), [])
  const replaceFleetBots = useCallback((incoming) => {
    setBots(replaceFleetBotsSnapshot(incoming))
  }, [])

  const upsertBot = useCallback((payload) => {
    setBots((prev) => upsertFleetBotRecord(prev, payload))
  }, [])

  const mergeBotRuntime = useCallback((botId, runtime) => {
    setBots((prev) => mergeFleetBotRuntime(prev, botId, runtime))
  }, [])

  const removeBot = useCallback((botId) => {
    setBots((prev) => removeFleetBotRecord(prev, botId))
  }, [])

  const upsertStrategy = useCallback((incoming) => {
    if (!incoming?.id) return null
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
      if (!strategyId) return null
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
    onCreated: () => {
      setCreateOpen(false)
      setCreateError(null)
    },
  })

  const loadRuntimeCapacity = useCallback(async () => {
    try {
      const payload = await fetchBotRuntimeCapacity()
      setRuntimeCapacity(payload && typeof payload === 'object' ? payload : null)
    } catch (err) {
      logger.warn('bot_runtime_capacity_load_failed', { message: err?.message }, err)
    }
  }, [logger])

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
    initializeBotsPageRuntime({ loadStrategies, logger })
  }, [loadStrategies, logger])

  useEffect(() => {
    const timer = setInterval(() => {
      loadRuntimeCapacity()
    }, 15000)
    return () => clearInterval(timer)
  }, [loadRuntimeCapacity])

  useEffect(() => {
    if (createOpen) return undefined
    const timer = setInterval(() => setNowEpochMs(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [createOpen])

  const { state: botStreamState, hasReceivedSnapshot } = useBotStream({
    replaceBots: replaceFleetBots,
    upsertBot,
    mergeBotRuntime,
    removeBot,
  })

  useEffect(() => {
    if (hasReceivedSnapshot) {
      setHasFleetSnapshot(true)
    }
  }, [hasReceivedSnapshot])

  const handleCreate = useCallback(async (event) => {
    setError(null)
    setCreateError(null)
    try {
      await submitCreate(event)
    } catch (err) {
      logger.error('bot_create_failed', { message: err?.message }, err)
      setCreateError(err?.message || 'Unable to create bot')
    }
  }, [logger, submitCreate])

  const handleStart = useCallback(async (botId) => {
    setError(null)
    const target = bots.find((bot) => bot.id === botId)
    setPendingStart(botId)
    try {
      const startedBot = await runBotStartAction({
        bot: target,
        botId,
        logger,
        loadRuntimeCapacity,
        startBot: startBotApi,
      })
      if (startedBot?.id) {
        upsertBot(startedBot)
      }
    } catch (err) {
      logger.error('bot_start_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to start bot')
    } finally {
      setPendingStart(null)
    }
  }, [bots, loadRuntimeCapacity, logger, upsertBot])

  const handleStop = useCallback(async (botId) => {
    setError(null)
    setPendingStop(botId)
    try {
      await runBotStopAction({
        botId,
        logger,
        loadRuntimeCapacity,
        stopBot: stopBotApi,
      })
    } catch (err) {
      logger.error('bot_stop_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to stop bot')
    } finally {
      setPendingStop(null)
    }
  }, [loadRuntimeCapacity, logger])

  const handleDelete = useCallback(async (botId) => {
    if (!botId) return
    if (!window.confirm('Delete this bot? This cannot be undone.')) return
    setError(null)
    logger.info('bot_delete_requested', { bot_id: botId })
    setPendingDelete(botId)
    try {
      await deleteBotApi(botId)
      if (lensBotId === botId) setLensBotId(null)
      if (diagnosticsBotId === botId) setDiagnosticsBotId(null)
    } catch (err) {
      logger.error('bot_delete_failed', { bot_id: botId, message: err?.message }, err)
      setError(err?.message || 'Unable to delete bot')
    } finally {
      setPendingDelete(null)
    }
  }, [diagnosticsBotId, lensBotId, logger])

  const handleViewReport = useCallback((bot) => {
    const display = getBotCardDisplayState(bot, {
      nowEpochMs,
      pendingStart: pendingStart === bot?.id,
    })
    const runView = mapRunToViewModel(bot, { display })
    if (!runView.runId) return
    navigate(`/reports?runId=${encodeURIComponent(runView.runId)}`)
  }, [navigate, nowEpochMs, pendingStart])

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
  const sortedStrategies = useMemo(
    () => [...strategies].sort((a, b) => (a.name || '').localeCompare(b.name || '')),
    [strategies],
  )

  const strategyLookup = useMemo(() => {
    const map = new Map()
    for (const strategy of sortedStrategies) {
      if (strategy?.id) map.set(strategy.id, strategy)
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
        bot.active_run_id,
        bot?.runtime?.run_id,
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

  const manualRefreshFleet = useCallback(async () => {
    await runManualFleetRefresh({
      replaceFleetBots,
      requestFleetSnapshot: listBots,
      loadRuntimeCapacity,
      logger,
      markFleetHydrated: () => setHasFleetSnapshot(true),
      setError,
      setRefreshing: setRefreshingFleet,
    })
  }, [loadRuntimeCapacity, logger, replaceFleetBots])

  return {
    botStreamState,
    closeCreateModal: () => {
      setCreateOpen(false)
      setCreateError(null)
    },
    createError,
    createOpen,
    diagnosticsBot,
    error,
    filteredBots,
    fleetSummary,
    form,
    handleBacktestRangeChange,
    handleChange,
    handleCreate,
    handleDelete,
    handleOpenCreate: async () => {
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
    },
    handleStart,
    handleStop,
    handleViewReport,
    handleStrategySelect,
    handleVariantSelect,
    handleWalletBalanceAdd,
    handleWalletBalanceChange,
    handleWalletBalanceRemove,
    hasFleetSnapshot,
    lensBot,
    pendingStop,
    refreshingFleet,
    nowEpochMs,
    pendingDelete,
    pendingStart,
    manualRefreshFleet,
    runtimeCapacity,
    search,
    setDiagnosticsBotId,
    setLensBotId,
    setSearch,
    sortedBots,
    sortedStrategies,
    strategiesLoading,
    strategyError,
    strategyLookup,
    walletError,
  }
}
