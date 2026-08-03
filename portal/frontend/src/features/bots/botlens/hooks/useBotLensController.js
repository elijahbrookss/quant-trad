import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react'

import {
  fetchBotLensChartHistory,
  fetchBotLensExactRunBootstrap,
  fetchBotLensForensicEvents,
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
  hasMoreBefore,
}) {
  return Boolean(
    activeRunId
    && selectedSymbolKey
    && Array.isArray(chartCandles)
    && chartCandles.length > 0
    && chartHistoryStatus !== 'loading'
    && hasMoreBefore !== false,
  )
}

export function shouldLoadInitialBotLensHistory({
  open,
  activeRunId,
  selectedSymbolKey,
  selectedSymbolReady,
  datasetId,
  chartHistoryStatus,
}) {
  return Boolean(
    open
    && activeRunId
    && selectedSymbolKey
    && selectedSymbolReady
    && datasetId
    && chartHistoryStatus === 'idle',
  )
}

export function resolveBotLensInitialHistoryEnd(runMeta) {
  return runMeta?.backtest_end || runMeta?.materialization_end || runMeta?.ended_at || null
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

const BOTLENS_DECISION_EVENT_NAMES = [
  'SIGNAL_EMITTED',
  'DECISION_EMITTED',
  'ENTRY_FILLED',
  'EXIT_FILLED',
  'TRADE_OPENED',
  'TRADE_UPDATED',
  'TRADE_CLOSED',
]

function emptyForensicReplayState(scopeKey = null) {
  return {
    scopeKey,
    documents: [],
    status: 'idle',
    error: null,
    hasMore: true,
    nextCursor: { afterSeq: 0, afterRowId: 0 },
  }
}

export function mergeBotLensForensicDocuments(existing, incoming) {
  const byId = new Map()
  ;[...(Array.isArray(existing) ? existing : []), ...(Array.isArray(incoming) ? incoming : [])]
    .forEach((document) => {
      const documentId = String(document?.document_id || '').trim()
      if (documentId) byId.set(documentId, document)
    })
  return Array.from(byId.values()).sort((left, right) => {
    const leftCursor = left?.cursor || {}
    const rightCursor = right?.cursor || {}
    const seqDelta = Number(leftCursor.after_seq || 0) - Number(rightCursor.after_seq || 0)
    if (seqDelta !== 0) return seqDelta
    return Number(leftCursor.after_row_id || 0) - Number(rightCursor.after_row_id || 0)
  })
}

export function shouldLoadMoreBotLensForensics({ status, hasMore, scopeKey }) {
  return Boolean(scopeKey && status !== 'loading' && hasMore !== false)
}

const BOTLENS_BOOTSTRAP_RETRY_MS = 1000
const BOTLENS_EXACT_BOOTSTRAP_TIMEOUT_MS = 30_000
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

function historicalReplayTimeoutError() {
  const error = new Error('Historical BotLens replay timed out')
  error.name = 'TimeoutError'
  return error
}

async function fetchExactRunBootstrapBeforeDeadline(runId, deadlineEpochMs) {
  const remainingMs = deadlineEpochMs - Date.now()
  if (remainingMs <= 0) throw historicalReplayTimeoutError()
  if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.timeout === 'function') {
    return fetchBotLensExactRunBootstrap(runId, { signal: AbortSignal.timeout(remainingMs) })
  }
  return Promise.race([
    fetchBotLensExactRunBootstrap(runId),
    delay(remainingMs).then(() => { throw historicalReplayTimeoutError() }),
  ])
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

export function useBotLensController({ open, bot, onClose, runId = null }) {
  const logger = useMemo(
    () => createLogger('BotLensRuntime', { botId: bot?.id || null, runId: runId || null }),
    [bot?.id, runId],
  )
  const [state, dispatch] = useReducer(
    reduceBotLensState,
    createInitialBotLensState({ botId: bot?.id || null }),
  )
  const [reloadTick, setReloadTick] = useState(0)
  const [forensicReplay, setForensicReplay] = useState(() => emptyForensicReplayState())
  const stateRef = useRef(state)
  stateRef.current = state
  const bootstrapTokenRef = useRef(0)
  const bootstrapLoadRef = useRef(new Set())
  const snapshotRefreshLoadRef = useRef(new Set())
  const initialChartLoadRef = useRef(new Set())
  const forensicReplayRef = useRef(forensicReplay)
  forensicReplayRef.current = forensicReplay
  const forensicLoadRef = useRef(new Set())
  const latestSelectionRef = useRef({ runId: null, symbolKey: null })
  const latestSelectionBootstrapRequestRef = useRef({ runId: null, symbolKey: null, requestId: 0 })

  const activeRunId = selectActiveRunId(state)
  const selectedSymbolKey = selectSelectedSymbolKey(state)

  // Every selector below is keyed on the specific nested slice it actually
  // reads, not the whole reducer `state` object. This is safe because the
  // delta-application functions in botlensProjection.js (withSymbolState,
  // applyRunHealthDelta, applyOpenTradesDelta, applyRunSymbolCatalogDelta,
  // etc.) only replace the sub-slice they touch — sibling slices, and other
  // symbols' entries within symbolStates, keep stable references across an
  // unrelated dispatch. Narrow deps mean an unrelated websocket tick (e.g. a
  // different symbol's candle, a health ping) skips recomputing values this
  // component doesn't use, instead of recomputing everything on every message.
  const selectedSymbolProjection = state.runState?.symbolStates?.[selectedSymbolKey] || null
  const symbolIndex = state.runState?.symbolIndex
  const health = state.runState?.health
  const openTradesIndex = state.runState?.openTradesIndex
  const chartHistoryForSymbol = state.retrieval?.chartHistoryBySymbol?.[selectedSymbolKey] || null
  const chartHistoryBySymbol = state.retrieval?.chartHistoryBySymbol

  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const selectedSymbolSlices = useMemo(() => selectSelectedSymbolBaseSlices(state), [selectedSymbolProjection, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const selectedSymbolMetadata = useMemo(() => selectSelectedSymbolMetadata(state), [selectedSymbolProjection, selectedSymbolKey])
  const selectedSymbolState = selectSelectedSymbolState(state)
  const selectedSummary = selectSelectedSymbolSummary(state)
  const selectedLabel = selectedSymbolSlices?.metadata?.display_label
    || selectedSummary?.display_label
    || selectedSymbolKey
    || '—'
  const selectedSymbolBootstrapStatus = selectSelectedSymbolBootstrapStatus(state)
  const selectedSymbolReady = Boolean(selectedSymbolState?.readiness?.snapshot_ready)
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const symbolOptions = useMemo(() => selectSymbolOptions(state), [symbolIndex])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const warningItems = useMemo(() => selectWarningItems(state), [health])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const openTrades = useMemo(() => selectOpenTrades(state), [openTradesIndex])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const chartCandles = useMemo(() => selectSelectedSymbolChartCandles(state), [selectedSymbolProjection, chartHistoryForSymbol, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const chartHistory = useMemo(() => selectSelectedSymbolChartHistory(state), [chartHistoryForSymbol, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const chartHistoryStatus = useMemo(() => selectSelectedSymbolChartHistoryStatus(state), [chartHistoryForSymbol, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const selectedSymbolOverlays = useMemo(() => selectSelectedSymbolOverlays(state), [selectedSymbolProjection, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const selectedSymbolRecentTrades = useMemo(() => selectSelectedSymbolRecentTrades(state), [selectedSymbolProjection, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const selectedSymbolLogs = useMemo(() => selectSelectedSymbolLogs(state), [selectedSymbolProjection, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const selectedSymbolSignals = useMemo(() => selectSelectedSymbolSignals(state), [selectedSymbolProjection, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const selectedSymbolDecisions = useMemo(() => selectSelectedSymbolDecisions(state), [selectedSymbolProjection, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const chartHistoryCacheCount = useMemo(() => selectChartHistoryCacheCount(state), [chartHistoryBySymbol])
  const transportEligible = Boolean(state.runState?.transportEligible)
  const runMeta = state.runState?.runMeta || null
  const datasetId = String(runMeta?.dataset?.dataset_id || '').trim()
  const initialHistoryEnd = resolveBotLensInitialHistoryEnd(runMeta)
  const forensicScopeKey = activeRunId && selectedSymbolKey
    ? [activeRunId, selectedSymbolKey].join(':')
    : null

  useEffect(() => {
    latestSelectionRef.current = {
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
    }
  }, [activeRunId, selectedSymbolKey])

  const refreshSession = useCallback(() => {
    bootstrapLoadRef.current.clear()
    snapshotRefreshLoadRef.current.clear()
    initialChartLoadRef.current.clear()
    forensicLoadRef.current.clear()
    setForensicReplay(emptyForensicReplayState())
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
    const bootstrapLoads = bootstrapLoadRef.current
    if (!open || !bot?.id) {
      bootstrapLoads.clear()
      snapshotRefreshLoadRef.current.clear()
      initialChartLoadRef.current.clear()
      forensicLoadRef.current.clear()
      setForensicReplay(emptyForensicReplayState())
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
      const exactBootstrapDeadline = runId ? Date.now() + BOTLENS_EXACT_BOOTSTRAP_TIMEOUT_MS : null
      try {
        while (!cancelled && token === bootstrapTokenRef.current) {
          const runBootstrap = runId
            ? await fetchExactRunBootstrapBeforeDeadline(runId, exactBootstrapDeadline)
            : await fetchBotLensRunBootstrap(bot.id)
          if (cancelled || token !== bootstrapTokenRef.current) return
          const returnedRunId = String(
            runBootstrap?.scope?.run_id || runBootstrap?.run?.meta?.run_id || '',
          ).trim()
          if (runId && returnedRunId !== String(runId).trim()) {
            throw new Error('BotLens bootstrap returned a mismatched run scope')
          }
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
            const remainingMs = exactBootstrapDeadline ? exactBootstrapDeadline - Date.now() : BOTLENS_BOOTSTRAP_RETRY_MS
            if (remainingMs <= 0) throw historicalReplayTimeoutError()
            dispatch({
              type: 'run/bootstrapPending',
              statusMessage: String(runBootstrap?.message || 'Waiting for BotLens run bootstrap...'),
            })
            await delay(Math.min(BOTLENS_BOOTSTRAP_RETRY_MS, remainingMs))
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
        const timedOut = ['AbortError', 'TimeoutError'].includes(String(err?.name || ''))
        const message = timedOut
          ? 'Historical BotLens replay did not become ready within 30 seconds. Retry, or inspect the persisted report evidence.'
          : err?.message || 'BotLens bootstrap failed'
        dispatch({
          type: 'run/bootstrapFailed',
          error: message,
          statusMessage: message,
        })
        logger.warn('botlens_bootstrap_load_failed', { bot_id: bot.id, run_id: runId || null }, err)
      } finally {
        bootstrapLoads.delete(initialSelectedSymbolKey)
      }
    }

    load()
    return () => {
      cancelled = true
      bootstrapLoads.clear()
    }
  }, [bot?.id, loadSelectedSymbolSnapshot, logger, open, reloadTick, runId])

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

  const loadMoreDecisionEvidence = useCallback(async ({ reset = false } = {}) => {
    const scopeKey = forensicScopeKey
    if (!scopeKey || !activeRunId || !selectedSymbolKey || !bot?.id) return
    const current = forensicReplayRef.current
    if (!reset && !shouldLoadMoreBotLensForensics({
      status: current.status,
      hasMore: current.hasMore,
      scopeKey: current.scopeKey === scopeKey ? scopeKey : null,
    })) return

    const cursor = reset
      ? { afterSeq: 0, afterRowId: 0 }
      : current.nextCursor || { afterSeq: 0, afterRowId: 0 }
    const requestKey = [scopeKey, cursor.afterSeq, cursor.afterRowId].join(':')
    if (forensicLoadRef.current.has(requestKey)) return
    forensicLoadRef.current.add(requestKey)
    setForensicReplay((previous) => ({
      ...(reset || previous.scopeKey !== scopeKey
        ? emptyForensicReplayState(scopeKey)
        : previous),
      scopeKey,
      status: 'loading',
      error: null,
    }))

    try {
      const page = await fetchBotLensForensicEvents(bot.id, activeRunId, {
        seriesKey: selectedSymbolKey,
        afterSeq: cursor.afterSeq,
        afterRowId: cursor.afterRowId,
        limit: 200,
        eventNames: BOTLENS_DECISION_EVENT_NAMES,
      })
      if (String(page?.run_id || '') !== String(activeRunId)) {
        throw new Error('Decision replay returned a mismatched run scope')
      }
      setForensicReplay((previous) => {
        if (previous.scopeKey !== scopeKey) return previous
        const next = page?.next_cursor || {}
        return {
          ...previous,
          documents: mergeBotLensForensicDocuments(
            reset ? [] : previous.documents,
            page?.documents,
          ),
          status: 'ready',
          error: null,
          hasMore: Boolean(page?.has_more),
          nextCursor: {
            afterSeq: Number(next.after_seq || cursor.afterSeq || 0),
            afterRowId: Number(next.after_row_id || cursor.afterRowId || 0),
          },
        }
      })
    } catch (err) {
      setForensicReplay((previous) => previous.scopeKey === scopeKey
        ? {
            ...previous,
            status: 'error',
            error: err?.message || 'Decision replay failed',
          }
        : previous)
      logger.warn('botlens_decision_replay_failed', {
        bot_id: bot.id,
        run_id: activeRunId,
        symbol_key: selectedSymbolKey,
        after_seq: cursor.afterSeq,
        after_row_id: cursor.afterRowId,
      }, err)
    } finally {
      forensicLoadRef.current.delete(requestKey)
    }
  }, [activeRunId, bot?.id, forensicScopeKey, logger, selectedSymbolKey])

  useEffect(() => {
    if (!open || !selectedSymbolReady || !forensicScopeKey) {
      if (forensicReplayRef.current.scopeKey) {
        setForensicReplay(emptyForensicReplayState())
      }
      return
    }
    loadMoreDecisionEvidence({ reset: true })
  }, [forensicScopeKey, loadMoreDecisionEvidence, open, reloadTick, selectedSymbolReady])

  useEffect(() => {
    if (!shouldLoadInitialBotLensHistory({
      open,
      activeRunId,
      selectedSymbolKey,
      selectedSymbolReady,
      datasetId,
      chartHistoryStatus,
    })) return undefined

    const requestKey = [activeRunId, selectedSymbolKey].join(':')
    if (initialChartLoadRef.current.has(requestKey)) return undefined
    initialChartLoadRef.current.add(requestKey)
    let cancelled = false
    dispatch({
      type: 'retrieval/chartRequest',
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
    })
    fetchBotLensChartHistory(activeRunId, selectedSymbolKey, {
      endTime: initialHistoryEnd || undefined,
      limit: 240,
    })
      .then((page) => {
        if (cancelled) return
        if (String(page?.run_id || '') !== String(activeRunId)) {
          throw new Error('Chart history returned a mismatched run scope')
        }
        if (normalizeSeriesKey(page?.symbol_key || '') !== normalizeSeriesKey(selectedSymbolKey)) {
          throw new Error('Chart history returned a mismatched symbol scope')
        }
        dispatch({
          type: 'retrieval/chartSuccess',
          runId: activeRunId,
          symbolKey: selectedSymbolKey,
          candles: Array.isArray(page?.candles) ? page.candles : [],
          range: page?.range,
          evidenceSource: page?.evidence_source,
        })
      })
      .catch((err) => {
        if (cancelled) return
        dispatch({
          type: 'retrieval/chartFailed',
          runId: activeRunId,
          symbolKey: selectedSymbolKey,
          error: err?.message || 'Frozen chart retrieval failed',
        })
        logger.warn('botlens_initial_history_failed', {
          bot_id: bot?.id || null,
          run_id: activeRunId,
          symbol_key: selectedSymbolKey,
          dataset_id: datasetId,
        }, err)
      })
      .finally(() => {
        initialChartLoadRef.current.delete(requestKey)
      })

    return () => {
      cancelled = true
    }
  }, [
    activeRunId,
    bot?.id,
    chartHistoryStatus,
    datasetId,
    initialHistoryEnd,
    logger,
    open,
    selectedSymbolKey,
    selectedSymbolReady,
  ])

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
      // Reads via stateRef (not the closed-over `state`) so this callback's
      // identity stays stable across unrelated ticks — it's diagnostic-only,
      // or downstream React.memo boundaries (ChartPanel, TradesTab) would be
      // defeated by a fresh function reference every time symbolStates changes.
      const latestSymbolStates = stateRef.current.runState?.symbolStates
      logger.info('botlens_symbol_switch_requested', {
        bot_id: bot?.id || null,
        run_id: activeRunId,
        symbol_key: normalizedSymbolKey,
        had_cached_symbol_state: Boolean(latestSymbolStates?.[normalizedSymbolKey]?.readiness?.snapshot_ready),
        state_cache_size: Object.keys(latestSymbolStates || {}).length,
      })
    },
    [activeRunId, bot?.id, logger],
  )

  const loadOlderHistory = useCallback(async () => {
    if (!shouldLoadOlderBotLensHistory({
      activeRunId,
      selectedSymbolKey,
      chartCandles,
      chartHistoryStatus,
      hasMoreBefore: chartHistory?.range?.has_more_before,
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
        evidenceSource: page?.evidence_source,
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
  }, [activeRunId, bot?.id, chartCandles, chartHistory?.range?.has_more_before, chartHistoryStatus, dispatch, logger, selectedSymbolKey])

  const clearError = useCallback(() => {
    dispatch({ type: 'ui/error', error: null })
  }, [])

  const closeModal = useCallback(() => {
    dispatch({ type: 'session/reset', botId: bot?.id || null })
    bootstrapLoadRef.current.clear()
    snapshotRefreshLoadRef.current.clear()
    initialChartLoadRef.current.clear()
    forensicLoadRef.current.clear()
    setForensicReplay(emptyForensicReplayState())
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
    forensicDocuments: forensicReplay.documents,
    forensicError: forensicReplay.error,
    forensicHasMore: forensicReplay.hasMore,
    forensicNextCursor: forensicReplay.nextCursor,
    forensicStatus: forensicReplay.status,
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
