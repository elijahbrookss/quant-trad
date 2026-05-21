import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react'

import {
  fetchBotLensChartHistory,
  fetchBotLensRunBootstrap,
  fetchBotLensSelectedSymbolSnapshot,
} from '../../../../adapters/bot.adapter.js'
import { normalizeSeriesKey } from '../../../../components/bots/botlensProjection.js'
import { createLogger } from '../../../../utils/logger.js'
import { useBotLensLiveTransport } from './useBotLensLiveTransport.js'
import {
  selectChartHistoryCacheCount,
  selectActiveRunId,
  selectSelectedSymbolChartHistory,
  selectSelectedSymbolChartHistoryStatus,
  selectOpenTrades,
  selectSelectedSymbolBaseSlices,
  selectSelectedSymbolBootstrapStatus,
  selectSelectedSymbolChartCandles,
  selectSelectedSymbolDecisions,
  selectSelectedSymbolKey,
  selectSelectedSymbolLogs,
  selectSelectedSymbolMetadata,
  selectSelectedSymbolOverlays,
  selectSelectedSymbolRecentTrades,
  selectSelectedSymbolSignals,
  selectSelectedSymbolState,
  selectSelectedSymbolSummary,
  selectSymbolOptions,
  selectWarningItems,
} from '../state/botlensRuntimeSelectors.js'
import { createInitialBotLensState, reduceBotLensState } from '../state/botlensRuntimeState.js'

export function shouldLoadOlderBotLensHistory({
  activeRunId,
  selectedSymbolKey,
  chartCandles,
  chartHistoryStatus,
}) {
  return Boolean(
    activeRunId
    && selectedSymbolKey
    && Array.isArray(chartCandles)
    && chartCandles.length > 0
    && chartHistoryStatus !== 'loading',
  )
}

export function resolveBotLensContractState(payload, fallback = 'idle') {
  const state = String(payload?.contract_state || payload?.state || fallback || '').trim().toLowerCase()
  return state || String(fallback || 'idle').trim().toLowerCase() || 'idle'
}

export function isBotLensRunBootstrapReady(payload) {
  return ['ready', 'bootstrap_ready'].includes(resolveBotLensContractState(payload))
}

export function isBotLensSelectedSymbolSnapshotReady(payload) {
  return ['ready', 'snapshot_ready'].includes(resolveBotLensContractState(payload))
}

const BOTLENS_BOOTSTRAP_RETRY_MS = 1000
const RETRYABLE_RUN_BOOTSTRAP_STATES = new Set([
  'waiting_for_symbols',
  'start_requested',
  'validating_configuration',
  'resolving_strategy',
  'resolving_runtime_dependencies',
  'preparing_run',
  'stamping_starting_state',
  'launching_container',
  'container_launched',
  'awaiting_container_boot',
  'container_booting',
  'loading_bot_config',
  'claiming_run',
  'loading_strategy_snapshot',
  'preparing_wallet',
  'planning_series_workers',
  'spawning_series_workers',
  'waiting_for_series_bootstrap',
  'warming_up_runtime',
  'runtime_subscribing',
  'awaiting_first_snapshot',
  'awaiting_live_runtime_facts',
])

function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, Number(ms || 0) || 0))
  })
}

export function shouldRetryBotLensRunBootstrap(payload) {
  if (isBotLensRunBootstrapReady(payload)) return false
  const runId = String(payload?.scope?.run_id || payload?.run?.meta?.run_id || '').trim()
  if (!runId) return false
  const state = resolveBotLensContractState(payload)
  if (['inactive', 'startup_failed', 'crashed', 'stopped', 'completed'].includes(state)) {
    return false
  }
  if (RETRYABLE_RUN_BOOTSTRAP_STATES.has(state)) {
    return true
  }
  return String(payload?.run?.lifecycle?.status || '').trim().toLowerCase() === 'starting'
}

export function shouldRetryBotLensSelectedSymbolBootstrap(payload) {
  if (isBotLensSelectedSymbolSnapshotReady(payload)) return false
  const runId = String(payload?.scope?.run_id || '').trim()
  if (!runId) return false
  const unavailableReason = String(payload?.unavailable_reason || '').trim().toLowerCase()
  return unavailableReason === 'symbol_snapshot_unavailable'
}

export function shouldPollSelectedSymbolVisual({
  open,
  activeRunId,
  transportEligible,
  selectedSymbolKey,
  selectedSymbolReady,
}) {
  return Boolean(
    open
    && activeRunId
    && transportEligible
    && normalizeSeriesKey(selectedSymbolKey || '')
    && selectedSymbolReady,
  )
}

export function resolveSelectedSymbolVisualRefreshIntervalMs(payload) {
  const intervalMs = Number(payload?.refresh?.interval_ms || 0)
  return intervalMs > 0 ? intervalMs : 4000
}

export function shouldCommitSelectedSymbolBootstrap({
  requestedRunId,
  requestedSymbolKey,
  activeRunId,
  activeSelectedSymbolKey,
  requestId,
  activeRequestId,
  activeRequestRunId,
  activeRequestSymbolKey,
}) {
  const normalizedRequestedSymbolKey = normalizeSeriesKey(requestedSymbolKey || '')
  const normalizedActiveSelectedSymbolKey = normalizeSeriesKey(activeSelectedSymbolKey || '')
  const normalizedActiveRequestSymbolKey = normalizeSeriesKey(activeRequestSymbolKey || '')
  const normalizedRequestedRunId = String(requestedRunId || '').trim()
  const normalizedActiveRunId = String(activeRunId || '').trim()
  const normalizedActiveRequestRunId = String(activeRequestRunId || '').trim()
  const normalizedRequestId = Number(requestId || 0)
  const normalizedActiveRequestId = Number(activeRequestId || 0)
  if (!normalizedRequestedRunId || !normalizedRequestedSymbolKey || normalizedRequestId <= 0) {
    return false
  }
  return Boolean(
    normalizedRequestedRunId === normalizedActiveRunId
    && normalizedRequestedSymbolKey === normalizedActiveSelectedSymbolKey
    && normalizedRequestedRunId === normalizedActiveRequestRunId
    && normalizedRequestedSymbolKey === normalizedActiveRequestSymbolKey
    && normalizedRequestId === normalizedActiveRequestId,
  )
}

export function useBotLensController({ open, bot, onClose }) {
  const logger = useMemo(() => createLogger('BotLensRuntime', { botId: bot?.id || null }), [bot?.id])
  const [state, dispatch] = useReducer(
    reduceBotLensState,
    createInitialBotLensState({ botId: bot?.id || null }),
  )
  const [reloadTick, setReloadTick] = useState(0)
  const bootstrapTokenRef = useRef(0)
  const bootstrapLoadRef = useRef(new Set())
  const snapshotRefreshLoadRef = useRef(new Set())
  const latestSelectionRef = useRef({ runId: null, symbolKey: null })
  const latestSelectionBootstrapRequestRef = useRef({ runId: null, symbolKey: null, requestId: 0 })

  const activeRunId = selectActiveRunId(state)
  const selectedSymbolKey = selectSelectedSymbolKey(state)
  const selectedSymbolSlices = useMemo(() => selectSelectedSymbolBaseSlices(state), [state])
  const selectedSymbolMetadata = useMemo(() => selectSelectedSymbolMetadata(state), [state])
  const selectedSymbolState = selectSelectedSymbolState(state)
  const selectedSummary = selectSelectedSymbolSummary(state)
  const selectedLabel = selectedSymbolSlices?.metadata?.display_label
    || selectedSummary?.display_label
    || selectedSymbolKey
    || '—'
  const selectedSymbolBootstrapStatus = selectSelectedSymbolBootstrapStatus(state)
  const selectedSymbolReady = Boolean(selectedSymbolState?.readiness?.snapshot_ready)
  const symbolOptions = useMemo(() => selectSymbolOptions(state), [state])
  const warningItems = useMemo(() => selectWarningItems(state), [state])
  const openTrades = useMemo(() => selectOpenTrades(state), [state])
  const chartCandles = useMemo(() => selectSelectedSymbolChartCandles(state), [state])
  const chartHistory = useMemo(() => selectSelectedSymbolChartHistory(state), [state])
  const chartHistoryStatus = useMemo(() => selectSelectedSymbolChartHistoryStatus(state), [state])
  const selectedSymbolOverlays = useMemo(() => selectSelectedSymbolOverlays(state), [state])
  const selectedSymbolRecentTrades = useMemo(() => selectSelectedSymbolRecentTrades(state), [state])
  const selectedSymbolLogs = useMemo(() => selectSelectedSymbolLogs(state), [state])
  const selectedSymbolSignals = useMemo(() => selectSelectedSymbolSignals(state), [state])
  const selectedSymbolDecisions = useMemo(() => selectSelectedSymbolDecisions(state), [state])
  const chartHistoryCacheCount = useMemo(() => selectChartHistoryCacheCount(state), [state])
  const transportEligible = Boolean(state.runState?.transportEligible)

  useEffect(() => {
    latestSelectionRef.current = {
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
    }
  }, [activeRunId, selectedSymbolKey])

  const refreshSession = useCallback(() => {
    bootstrapLoadRef.current.clear()
    snapshotRefreshLoadRef.current.clear()
    setReloadTick((value) => value + 1)
  }, [])

  const loadSelectedSymbolSnapshot = useCallback(
    async ({
      runId,
      symbolKey,
      statusMessage = null,
      mode = 'background',
      requestId = 0,
    }) => {
      const resolvedRunId = String(runId || '').trim()
      const resolvedSymbolKey = normalizeSeriesKey(symbolKey || '')
      if (!resolvedRunId || !resolvedSymbolKey) return null
      const requestKey = `${resolvedRunId}:${resolvedSymbolKey}`
      if (snapshotRefreshLoadRef.current.has(requestKey)) return null
      snapshotRefreshLoadRef.current.add(requestKey)
      if (mode === 'bootstrap') {
        dispatch({
          type: 'selection/bootstrapStarted',
          symbolKey: resolvedSymbolKey,
          statusMessage: statusMessage || `Loading symbol snapshot for ${resolvedSymbolKey}...`,
        })
      }
      try {
        while (true) {
          const selectedSymbolSnapshot = await fetchBotLensSelectedSymbolSnapshot(
            resolvedRunId,
            resolvedSymbolKey,
            { limit: 320 },
          )
          if (String(selectedSymbolSnapshot?.scope?.run_id || '') !== resolvedRunId) {
            throw new Error('Selected symbol snapshot returned a mismatched run scope')
          }
          if (normalizeSeriesKey(selectedSymbolSnapshot?.selection?.selected_symbol_key || '') !== resolvedSymbolKey) {
            throw new Error('Selected symbol snapshot returned a mismatched symbol scope')
          }
          if (
            mode === 'bootstrap'
            && !shouldCommitSelectedSymbolBootstrap({
              requestedRunId: resolvedRunId,
              requestedSymbolKey: resolvedSymbolKey,
              activeRunId: latestSelectionRef.current.runId,
              activeSelectedSymbolKey: latestSelectionRef.current.symbolKey,
              requestId,
              activeRequestId: latestSelectionBootstrapRequestRef.current.requestId,
              activeRequestRunId: latestSelectionBootstrapRequestRef.current.runId,
              activeRequestSymbolKey: latestSelectionBootstrapRequestRef.current.symbolKey,
            })
          ) {
            return null
          }
          if (isBotLensSelectedSymbolSnapshotReady(selectedSymbolSnapshot)) {
            dispatch({
              type: 'selection/bootstrapReady',
              runId: resolvedRunId,
              symbolKey: resolvedSymbolKey,
              bootstrapPayload: selectedSymbolSnapshot,
              statusMessage,
            })
            return selectedSymbolSnapshot
          }
          if (mode === 'bootstrap' && shouldRetryBotLensSelectedSymbolBootstrap(selectedSymbolSnapshot)) {
            dispatch({
              type: 'selection/bootstrapPending',
              symbolKey: resolvedSymbolKey,
              statusMessage: String(
                selectedSymbolSnapshot?.message
                  || `Waiting for symbol snapshot for ${resolvedSymbolKey}...`,
              ),
            })
            await delay(BOTLENS_BOOTSTRAP_RETRY_MS)
            continue
          }
          dispatch({
            type: 'selection/bootstrapUnavailable',
            symbolKey: resolvedSymbolKey,
            statusMessage: String(
              selectedSymbolSnapshot?.message
                || `Symbol snapshot unavailable for ${resolvedSymbolKey}.`,
            ),
            unavailableReason: String(selectedSymbolSnapshot?.unavailable_reason || '').trim() || null,
          })
          return selectedSymbolSnapshot
        }
      } catch (err) {
        if (mode === 'bootstrap') {
          if (
            !shouldCommitSelectedSymbolBootstrap({
              requestedRunId: resolvedRunId,
              requestedSymbolKey: resolvedSymbolKey,
              activeRunId: latestSelectionRef.current.runId,
              activeSelectedSymbolKey: latestSelectionRef.current.symbolKey,
              requestId,
              activeRequestId: latestSelectionBootstrapRequestRef.current.requestId,
              activeRequestRunId: latestSelectionBootstrapRequestRef.current.runId,
              activeRequestSymbolKey: latestSelectionBootstrapRequestRef.current.symbolKey,
            })
          ) {
            return null
          }
          dispatch({
            type: 'selection/bootstrapFailed',
            symbolKey: resolvedSymbolKey,
            statusMessage: `Failed to load symbol snapshot for ${resolvedSymbolKey}.`,
            error: err?.message || `Failed to load symbol snapshot for ${resolvedSymbolKey}`,
          })
        }
        logger.warn(
          mode === 'bootstrap' ? 'botlens_selected_symbol_snapshot_load_failed' : 'botlens_selected_symbol_snapshot_refresh_failed',
          {
            bot_id: bot?.id || null,
            run_id: resolvedRunId,
            symbol_key: resolvedSymbolKey,
            mode,
          },
          err,
        )
        return null
      } finally {
        snapshotRefreshLoadRef.current.delete(requestKey)
      }
    },
    [bot?.id, dispatch, logger],
  )

  useEffect(() => {
    if (!open || !bot?.id) {
      bootstrapLoadRef.current.clear()
      snapshotRefreshLoadRef.current.clear()
      dispatch({ type: 'session/reset', botId: bot?.id || null })
      return
    }

    let cancelled = false
    const token = ++bootstrapTokenRef.current

    dispatch({
      type: 'run/bootstrapStarted',
      botId: bot.id,
      statusMessage: 'Bootstrapping BotLens run...',
    })

    const load = async () => {
      let initialSelectedSymbolKey = ''
      try {
        while (!cancelled && token === bootstrapTokenRef.current) {
          const runBootstrap = await fetchBotLensRunBootstrap(bot.id)
          if (cancelled || token !== bootstrapTokenRef.current) return
          if (isBotLensRunBootstrapReady(runBootstrap)) {
            const initialRunId = String(runBootstrap?.run?.meta?.run_id || '').trim()
            initialSelectedSymbolKey = normalizeSeriesKey(runBootstrap?.navigation?.selected_symbol_key || '')
            if (!initialRunId || !initialSelectedSymbolKey) {
              throw new Error('BotLens run bootstrap is missing selected symbol bootstrap scope')
            }

            dispatch({
              type: 'run/bootstrapReady',
              runBootstrap,
              statusMessage: String(runBootstrap?.message || 'BotLens run bootstrap ready.'),
            })
            return
          }
          if (shouldRetryBotLensRunBootstrap(runBootstrap)) {
            dispatch({
              type: 'run/bootstrapPending',
              statusMessage: String(runBootstrap?.message || 'Waiting for BotLens run bootstrap...'),
            })
            await delay(BOTLENS_BOOTSTRAP_RETRY_MS)
            continue
          }
          dispatch({
            type: 'run/bootstrapUnavailable',
            statusMessage: String(runBootstrap?.message || 'BotLens run bootstrap unavailable'),
          })
          return
        }
      } catch (err) {
        if (cancelled || token !== bootstrapTokenRef.current) return
        dispatch({
          type: 'run/bootstrapFailed',
          error: err?.message || 'BotLens bootstrap failed',
          statusMessage: 'BotLens bootstrap failed.',
        })
        logger.warn('botlens_bootstrap_load_failed', { bot_id: bot.id }, err)
      } finally {
        bootstrapLoadRef.current.delete(initialSelectedSymbolKey)
      }
    }

    load()
    return () => {
      cancelled = true
      bootstrapLoadRef.current.clear()
    }
  }, [bot?.id, loadSelectedSymbolSnapshot, logger, open, reloadTick])

  useEffect(() => {
    if (!open || !activeRunId || !selectedSymbolKey) return

    if (selectedSymbolReady) {
      dispatch({
        type: 'ui/statusMessage',
        statusMessage: `Viewing ${selectedLabel}`,
      })
      return
    }

    if (bootstrapLoadRef.current.has(selectedSymbolKey)) return

    let cancelled = false
    bootstrapLoadRef.current.add(selectedSymbolKey)
    const requestId = latestSelectionBootstrapRequestRef.current.requestId + 1
    latestSelectionBootstrapRequestRef.current = {
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
      requestId,
    }
    dispatch({
      type: 'selection/bootstrapStarted',
      symbolKey: selectedSymbolKey,
      statusMessage: `Loading symbol snapshot for ${selectedLabel}...`,
    })

    loadSelectedSymbolSnapshot({
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
      statusMessage: `Viewing ${selectedLabel}`,
      mode: 'bootstrap',
      requestId,
    })
      .then((selectedSymbolSnapshot) => {
        if (cancelled) return
        if (!selectedSymbolSnapshot) return
      })
      .catch((err) => {
        if (cancelled) return
        logger.warn(
          'botlens_selected_symbol_snapshot_load_failed',
          {
            bot_id: bot?.id || null,
            run_id: activeRunId,
            symbol_key: selectedSymbolKey,
          },
          err,
        )
      })
      .finally(() => {
        bootstrapLoadRef.current.delete(selectedSymbolKey)
      })

    return () => {
      cancelled = true
    }
  }, [activeRunId, bot?.id, loadSelectedSymbolSnapshot, logger, open, selectedLabel, selectedSymbolKey, selectedSymbolReady])

  useBotLensLiveTransport({
    open,
    botId: bot?.id || null,
    runId: activeRunId,
    transportEligible,
    selectedSymbolKey,
    selectedSymbolReady,
    streamSessionId: state.live.sessionId,
    resumeFromSeq: state.live.lastStreamSeq,
    dispatch,
    refreshSession,
    logger,
  })

  const changeSelectedSymbol = useCallback(
    (symbolKey) => {
      const normalizedSymbolKey = normalizeSeriesKey(symbolKey || '')
      if (!normalizedSymbolKey) return
      dispatch({ type: 'selection/requested', symbolKey: normalizedSymbolKey })
      dispatch({
        type: 'ui/statusMessage',
        statusMessage: `Loading symbol snapshot for ${normalizedSymbolKey}...`,
      })
      logger.info('botlens_symbol_switch_requested', {
        bot_id: bot?.id || null,
        run_id: activeRunId,
        symbol_key: normalizedSymbolKey,
        had_cached_symbol_state: Boolean(state.runState?.symbolStates?.[normalizedSymbolKey]?.readiness?.snapshot_ready),
        state_cache_size: Object.keys(state.runState?.symbolStates || {}).length,
      })
    },
    [activeRunId, bot?.id, logger, state.runState?.symbolStates],
  )

  const loadOlderHistory = useCallback(async () => {
    if (!shouldLoadOlderBotLensHistory({
      activeRunId,
      selectedSymbolKey,
      chartCandles,
      chartHistoryStatus,
    })) {
      return
    }
    const oldest = chartCandles[0]
    const endTime = oldest?.time ? new Date(Number(oldest.time) * 1000).toISOString() : undefined
    dispatch({
      type: 'retrieval/chartRequest',
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
    })
    try {
      const page = await fetchBotLensChartHistory(activeRunId, selectedSymbolKey, { endTime, limit: 240 })
      const candles = Array.isArray(page?.candles) ? page.candles : []
      dispatch({
        type: 'retrieval/chartSuccess',
        runId: activeRunId,
        symbolKey: selectedSymbolKey,
        candles,
        range: page?.range,
      })
    } catch (err) {
      dispatch({
        type: 'retrieval/chartFailed',
        runId: activeRunId,
        symbolKey: selectedSymbolKey,
        error: err?.message || 'Chart retrieval failed',
      })
      logger.warn(
        'botlens_history_page_failed',
        {
          bot_id: bot?.id || null,
          run_id: activeRunId,
          symbol_key: selectedSymbolKey,
        },
        err,
      )
    }
  }, [activeRunId, bot?.id, chartCandles, chartHistoryStatus, dispatch, logger, selectedSymbolKey])

  const clearError = useCallback(() => {
    dispatch({ type: 'ui/error', error: null })
  }, [])

  const closeModal = useCallback(() => {
    dispatch({ type: 'session/reset', botId: bot?.id || null })
    bootstrapLoadRef.current.clear()
    snapshotRefreshLoadRef.current.clear()
    onClose?.()
  }, [bot?.id, onClose])

  return {
    activeRunId,
    chartCandles,
    chartHistory,
    chartHistoryCacheCount,
    chartHistoryStatus,
    chartOverlays: selectedSymbolOverlays,
    chartTrades: selectedSymbolRecentTrades,
    clearError,
    closeModal,
    changeSelectedSymbol,
    error: state.ui.error,
    loadOlderHistory,
    logs: selectedSymbolLogs,
    openTrades,
    refreshSession,
    runtimeStatus: state.status,
    runState: state.runState,
    selectedSymbolDecisions,
    selectedLabel,
    selectedSymbolMetadata,
    selectedSymbolSignals,
    selectedSummary,
    selectedSymbolBootstrapStatus,
    selectedSymbolKey,
    selectedSymbolReady,
    selectedSymbolSlices,
    selectedSymbolState,
    statusMessage: state.ui.statusMessage,
    streamState: state.live.connectionState,
    symbolOptions,
    warningItems,
  }
}
