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
import { getDecisionDataset, getReportDiagnosticsPage, getTradeDataset } from '../../../../adapters/report.adapter.js'
import { useBotLensLiveTransport } from './useBotLensLiveTransport.js'
import {
  selectChartHistoryCacheCount,
  selectActiveRunId,
  selectSelectedSymbolChartHistory,
  selectSelectedSymbolChartHistoryStatus,
  selectSelectedSymbolChartTrades,
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
  datasetId,
  chartHistoryStatus,
  transportEligible = false,
  chartCandles = [],
}) {
  const liveBootstrapReady = Boolean(
    transportEligible
    && Array.isArray(chartCandles)
    && chartCandles.length > 0,
  )
  return Boolean(
    open
    && activeRunId
    && selectedSymbolKey
    && datasetId
    && chartHistoryStatus === 'idle'
    && !liveBootstrapReady,
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

const BOTLENS_EVIDENCE_PAGE_SIZE = 100
export const BOTLENS_DURABLE_EVIDENCE_STAGES = Object.freeze(['decisions', 'trades', 'diagnostics'])

export function shouldStartDurableEvidenceStages({
  open,
  scopeKey,
  chartHistoryStatus,
  stageKey,
  activeStageKey,
  started,
}) {
  return Boolean(
    open
    && scopeKey
    && chartHistoryStatus === 'ready'
    && stageKey === activeStageKey
    && !started,
  )
}

function emptyEvidencePage() {
  return { items: [], total: null, offset: 0, limit: BOTLENS_EVIDENCE_PAGE_SIZE, status: 'idle', error: null }
}

function emptyDurableEvidenceState(scopeKey = null) {
  return {
    scopeKey,
    decisions: emptyEvidencePage(),
    trades: emptyEvidencePage(),
    diagnostics: { ...emptyEvidencePage(), summary: {} },
  }
}

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

export function shouldAutoLoadInitialBotLensForensics({ open, scopeKey, transportEligible }) {
  return Boolean(open && scopeKey && !transportEligible)
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

function isAbortError(error) {
  return ['AbortError', 'TimeoutError'].includes(String(error?.name || ''))
}

async function fetchExactRunBootstrapBeforeDeadline(runId, deadlineEpochMs, externalSignal = null) {
  const remainingMs = deadlineEpochMs - Date.now()
  if (remainingMs <= 0) throw historicalReplayTimeoutError()
  if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.timeout === 'function') {
    const timeoutSignal = AbortSignal.timeout(remainingMs)
    const signal = externalSignal && typeof AbortSignal.any === 'function'
      ? AbortSignal.any([externalSignal, timeoutSignal])
      : timeoutSignal
    return fetchBotLensExactRunBootstrap(runId, { signal })
  }
  return Promise.race([
    fetchBotLensExactRunBootstrap(runId, { signal: externalSignal || undefined }),
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
  const [durableEvidence, setDurableEvidence] = useState(() => emptyDurableEvidenceState())
  const stateRef = useRef(state)
  stateRef.current = state
  const bootstrapTokenRef = useRef(0)
  const bootstrapLoadRef = useRef(new Set())
  const snapshotRefreshLoadRef = useRef(new Set())
  const initialChartLoadRef = useRef(new Set())
  const forensicReplayRef = useRef(forensicReplay)
  forensicReplayRef.current = forensicReplay
  const forensicLoadRef = useRef(new Set())
  const durableEvidenceRequestRef = useRef({})
  const durableEvidenceStageRef = useRef({ stageKey: null, token: null })
  const latestSelectionRef = useRef({ runId: null, symbolKey: null })
  const latestSelectionBootstrapRequestRef = useRef({ runId: null, symbolKey: null, requestId: 0 })
  const chartRequestSequenceRef = useRef(0)
  const chartRequestsRef = useRef(new Map())

  const abortChartRequests = useCallback((keepSymbolKey = null) => {
    const normalizedKeep = normalizeSeriesKey(keepSymbolKey || '')
    chartRequestsRef.current.forEach((request, scopeKey) => {
      if (normalizedKeep && request.symbolKey === normalizedKeep) return
      request.controller.abort()
      chartRequestsRef.current.delete(scopeKey)
    })
  }, [])

  const beginChartRequest = useCallback(({ runId: requestedRunId, symbolKey, requestKey }) => {
    const normalizedRunId = String(requestedRunId || '').trim()
    const normalizedSymbolKey = normalizeSeriesKey(symbolKey || '')
    if (!normalizedRunId || !normalizedSymbolKey || !requestKey) return null
    const scopeKey = `${normalizedRunId}:${normalizedSymbolKey}`
    const existing = chartRequestsRef.current.get(scopeKey)
    if (existing?.requestKey === requestKey) return null
    existing?.controller.abort()

    const requestId = ++chartRequestSequenceRef.current
    const request = {
      controller: new AbortController(),
      requestId,
      requestKey,
      runId: normalizedRunId,
      scopeKey,
      symbolKey: normalizedSymbolKey,
    }
    chartRequestsRef.current.set(scopeKey, request)
    dispatch({
      type: 'retrieval/chartRequest',
      runId: normalizedRunId,
      symbolKey: normalizedSymbolKey,
      requestId,
    })
    return request
  }, [])

  const isCurrentChartRequest = useCallback((request) => {
    if (!request) return false
    const current = chartRequestsRef.current.get(request.scopeKey)
    const selection = latestSelectionRef.current
    return Boolean(
      current?.requestId === request.requestId
      && String(selection.runId || '').trim() === request.runId
      && normalizeSeriesKey(selection.symbolKey || '') === request.symbolKey
    )
  }, [])

  const finishChartRequest = useCallback((request) => {
    if (!request) return
    const current = chartRequestsRef.current.get(request.scopeKey)
    if (current?.requestId === request.requestId) {
      chartRequestsRef.current.delete(request.scopeKey)
    }
  }, [])

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
  // eslint-disable-next-line react-hooks/exhaustive-deps -- live projection and durable history are independently scoped
  const selectedSymbolOverlays = useMemo(() => selectSelectedSymbolOverlays(state), [selectedSymbolProjection, chartHistoryForSymbol, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally scoped, see comment above
  const selectedSymbolRecentTrades = useMemo(() => selectSelectedSymbolRecentTrades(state), [selectedSymbolProjection, selectedSymbolKey])
  // eslint-disable-next-line react-hooks/exhaustive-deps -- history and live tails are independently scoped
  const chartTrades = useMemo(() => selectSelectedSymbolChartTrades(state), [selectedSymbolProjection, chartHistoryForSymbol, selectedSymbolKey])
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
  const durableEvidenceInstrumentId = String(selectedSymbolMetadata?.instrument_id || selectedSummary?.instrument_id || '').trim()
  const durableEvidenceSymbol = String(selectedSymbolMetadata?.symbol || selectedSummary?.symbol || '').trim()
  const durableEvidenceIdentity = durableEvidenceInstrumentId || durableEvidenceSymbol
  const durableEvidenceScopeKey = activeRunId && durableEvidenceIdentity && !transportEligible
    ? [activeRunId, durableEvidenceIdentity].join(':')
    : null

  useEffect(() => {
    latestSelectionRef.current = {
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
    }
  }, [activeRunId, selectedSymbolKey])

  const refreshSession = useCallback(() => {
    abortChartRequests()
    bootstrapLoadRef.current.clear()
    snapshotRefreshLoadRef.current.clear()
    initialChartLoadRef.current.clear()
    forensicLoadRef.current.clear()
    durableEvidenceRequestRef.current = {}
    durableEvidenceStageRef.current = { stageKey: null, token: null }
    setForensicReplay(emptyForensicReplayState())
    setDurableEvidence(emptyDurableEvidenceState())
    setReloadTick((value) => value + 1)
  }, [abortChartRequests])

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
      abortChartRequests()
      bootstrapLoads.clear()
      snapshotRefreshLoadRef.current.clear()
      initialChartLoadRef.current.clear()
      forensicLoadRef.current.clear()
      setForensicReplay(emptyForensicReplayState())
      dispatch({ type: 'session/reset', botId: bot?.id || null })
      return
    }

    let cancelled = false
    const bootstrapController = new AbortController()
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
            ? await fetchExactRunBootstrapBeforeDeadline(runId, exactBootstrapDeadline, bootstrapController.signal)
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
      bootstrapController.abort()
      bootstrapLoads.clear()
    }
  }, [abortChartRequests, bot?.id, loadSelectedSymbolSnapshot, logger, open, reloadTick, runId])

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

  const loadDurableEvidence = useCallback(async (section, pageIndex = 0, { force = false } = {}) => {
    const scopeKey = durableEvidenceScopeKey
    if (!scopeKey || !activeRunId || !durableEvidenceIdentity) return
    const normalizedPageIndex = Math.max(0, Number(pageIndex || 0) || 0)
    const offset = normalizedPageIndex * BOTLENS_EVIDENCE_PAGE_SIZE
    const token = {}
    durableEvidenceRequestRef.current[section] = token
    setDurableEvidence((previous) => {
      const base = previous.scopeKey === scopeKey ? previous : emptyDurableEvidenceState(scopeKey)
      return {
        ...base,
        scopeKey,
        [section]: { ...base[section], status: 'loading', error: null },
      }
    })
    try {
      let payload
      if (section === 'decisions') {
        payload = await getDecisionDataset(activeRunId, {
          limit: BOTLENS_EVIDENCE_PAGE_SIZE,
          offset,
          ...(durableEvidenceInstrumentId
            ? { instrumentId: durableEvidenceInstrumentId }
            : { symbol: durableEvidenceSymbol }),
        }, { force })
      } else if (section === 'trades') {
        payload = await getTradeDataset(activeRunId, {
          limit: BOTLENS_EVIDENCE_PAGE_SIZE,
          offset,
          ...(durableEvidenceInstrumentId
            ? { instrumentId: durableEvidenceInstrumentId }
            : { symbol: durableEvidenceSymbol }),
        }, { force })
      } else {
        payload = await getReportDiagnosticsPage(activeRunId, {
          limit: BOTLENS_EVIDENCE_PAGE_SIZE,
          offset,
        }, { force })
      }
      if (durableEvidenceRequestRef.current[section] !== token) return
      if (String(payload?.run_id || '') !== String(activeRunId)) {
        throw new Error(section + ' evidence returned a mismatched run scope')
      }
      setDurableEvidence((previous) => {
        if (previous.scopeKey !== scopeKey) return previous
        if (section === 'diagnostics') {
          return {
            ...previous,
            diagnostics: {
              items: Array.isArray(payload?.items) ? payload.items : [],
              summary: payload?.summary && typeof payload.summary === 'object' ? payload.summary : {},
              total: Math.max(0, Number(payload?.total || 0) || 0),
              offset: Math.max(0, Number(payload?.offset || offset) || 0),
              limit: Math.max(1, Number(payload?.limit || BOTLENS_EVIDENCE_PAGE_SIZE) || BOTLENS_EVIDENCE_PAGE_SIZE),
              status: 'ready',
              error: null,
            },
          }
        }
        return {
          ...previous,
          [section]: {
            items: Array.isArray(payload?.items) ? payload.items : [],
            total: Math.max(0, Number(payload?.total || 0) || 0),
            offset: Math.max(0, Number(payload?.offset || offset) || 0),
            limit: Math.max(1, Number(payload?.limit || BOTLENS_EVIDENCE_PAGE_SIZE) || BOTLENS_EVIDENCE_PAGE_SIZE),
            status: 'ready',
            error: null,
          },
        }
      })
    } catch (err) {
      if (durableEvidenceRequestRef.current[section] !== token) return
      setDurableEvidence((previous) => previous.scopeKey === scopeKey
        ? {
            ...previous,
            [section]: {
              ...previous[section],
              status: "error",
              error: err?.message || (section + ' evidence failed'),
            },
          }
        : previous)
      logger.warn('botlens_durable_evidence_failed', {
        bot_id: bot?.id || null,
        run_id: activeRunId,
        instrument_id: durableEvidenceInstrumentId,
        section,
        offset,
      }, err)
    }
  }, [activeRunId, bot?.id, durableEvidenceIdentity, durableEvidenceInstrumentId, durableEvidenceScopeKey, durableEvidenceSymbol, logger])

  const loadDecisionEvidencePage = useCallback((pageIndex) => (
    loadDurableEvidence('decisions', pageIndex)
  ), [loadDurableEvidence])

  const loadTradeEvidencePage = useCallback((pageIndex) => (
    loadDurableEvidence('trades', pageIndex)
  ), [loadDurableEvidence])

  const loadDiagnosticEvidencePage = useCallback((pageIndex) => (
    loadDurableEvidence('diagnostics', pageIndex)
  ), [loadDurableEvidence])

  useEffect(() => {
    if (!open || !durableEvidenceScopeKey) {
      durableEvidenceRequestRef.current = {}
      durableEvidenceStageRef.current = { stageKey: null, token: null }
      setDurableEvidence((previous) => previous.scopeKey ? emptyDurableEvidenceState() : previous)
      return
    }

    const stageKey = `${durableEvidenceScopeKey}:${reloadTick}`
    if (durableEvidenceStageRef.current.stageKey !== stageKey) {
      durableEvidenceStageRef.current = { stageKey, token: null }
      setDurableEvidence(emptyDurableEvidenceState(durableEvidenceScopeKey))
    }
    if (!shouldStartDurableEvidenceStages({
      open,
      scopeKey: durableEvidenceScopeKey,
      chartHistoryStatus,
      stageKey,
      activeStageKey: durableEvidenceStageRef.current.stageKey,
      started: Boolean(durableEvidenceStageRef.current.token),
    })) return

    const token = {}
    durableEvidenceStageRef.current = { stageKey, token }
    const isCurrentStage = () => durableEvidenceStageRef.current.stageKey === stageKey
      && durableEvidenceStageRef.current.token === token
    const loadEvidenceStages = async () => {
      logger.info('botlens_durable_evidence_stages_started', {
        bot_id: bot?.id || null,
        run_id: activeRunId,
        instrument_id: durableEvidenceInstrumentId,
        stages: BOTLENS_DURABLE_EVIDENCE_STAGES,
      })
      for (const section of BOTLENS_DURABLE_EVIDENCE_STAGES) {
        await loadDurableEvidence(section, 0)
        if (!isCurrentStage()) return
      }
      logger.info('botlens_durable_evidence_stages_ready', {
        bot_id: bot?.id || null,
        run_id: activeRunId,
        instrument_id: durableEvidenceInstrumentId,
      })
    }
    loadEvidenceStages()
  }, [activeRunId, bot?.id, chartHistoryStatus, durableEvidenceInstrumentId, durableEvidenceScopeKey, loadDurableEvidence, logger, open, reloadTick])

  useEffect(() => {
    if (durableEvidenceScopeKey) {
      if (forensicReplayRef.current.scopeKey) setForensicReplay(emptyForensicReplayState())
      return
    }
    if (!open || !forensicScopeKey) {
      if (forensicReplayRef.current.scopeKey) {
        setForensicReplay(emptyForensicReplayState())
      }
      return
    }
    if (!shouldAutoLoadInitialBotLensForensics({
      open,
      scopeKey: forensicScopeKey,
      transportEligible,
    })) {
      if (forensicReplayRef.current.scopeKey !== forensicScopeKey) {
        setForensicReplay(emptyForensicReplayState(forensicScopeKey))
      }
      return
    }
    loadMoreDecisionEvidence({ reset: true })
  }, [durableEvidenceScopeKey, forensicScopeKey, loadMoreDecisionEvidence, open, reloadTick, transportEligible])

  useEffect(() => {
    const currentState = stateRef.current
    const currentChartHistoryStatus = selectSelectedSymbolChartHistoryStatus(currentState)
    const currentChartCandles = selectSelectedSymbolChartCandles(currentState)
    if (!shouldLoadInitialBotLensHistory({
      open,
      activeRunId,
      selectedSymbolKey,
      datasetId,
      chartHistoryStatus: currentChartHistoryStatus,
      transportEligible,
      chartCandles: currentChartCandles,
    })) return undefined

    const request = beginChartRequest({
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
      requestKey: `initial:${initialHistoryEnd || 'latest'}`,
    })
    if (!request) return undefined
    fetchBotLensChartHistory(activeRunId, selectedSymbolKey, {
      endTime: initialHistoryEnd || undefined,
      limit: 240,
      signal: request.controller.signal,
    })
      .then((page) => {
        if (!isCurrentChartRequest(request)) return
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
          trades: Array.isArray(page?.trades) ? page.trades : [],
          overlays: Array.isArray(page?.overlays) ? page.overlays : [],
          range: page?.range,
          evidenceSource: page?.evidence_source,
          tradeEvidence: page?.trade_evidence,
          overlayEvidence: page?.overlay_evidence,
          mergeMode: 'replace',
          requestId: request.requestId,
        })
      })
      .catch((err) => {
        if (isAbortError(err) || !isCurrentChartRequest(request)) return
        dispatch({
          type: 'retrieval/chartFailed',
          runId: activeRunId,
          symbolKey: selectedSymbolKey,
          requestId: request.requestId,
          error: err?.message || 'Frozen chart retrieval failed',
        })
        logger.warn('botlens_initial_history_failed', {
          bot_id: bot?.id || null,
          run_id: activeRunId,
          symbol_key: selectedSymbolKey,
          dataset_id: datasetId,
        }, err)
      })
      .finally(() => finishChartRequest(request))

    return () => {
      request.controller.abort()
      finishChartRequest(request)
    }
  }, [
    activeRunId,
    beginChartRequest,
    bot?.id,
    datasetId,
    finishChartRequest,
    initialHistoryEnd,
    isCurrentChartRequest,
    logger,
    open,
    selectedSymbolKey,
    transportEligible,
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
      abortChartRequests(normalizedSymbolKey)
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
    [abortChartRequests, activeRunId, bot?.id, logger],
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
    const request = beginChartRequest({
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
      requestKey: `older:${endTime || 'start'}`,
    })
    if (!request) return
    try {
      const page = await fetchBotLensChartHistory(activeRunId, selectedSymbolKey, {
        endTime,
        limit: 240,
        signal: request.controller.signal,
      })
      if (!isCurrentChartRequest(request)) return
      if (String(page?.run_id || '') !== String(activeRunId)) {
        throw new Error('Older chart history returned a mismatched run scope')
      }
      if (normalizeSeriesKey(page?.symbol_key || '') !== normalizeSeriesKey(selectedSymbolKey)) {
        throw new Error('Older chart history returned a mismatched symbol scope')
      }
      const candles = Array.isArray(page?.candles) ? page.candles : []
      dispatch({
        type: 'retrieval/chartSuccess',
        runId: activeRunId,
        symbolKey: selectedSymbolKey,
        candles,
        trades: Array.isArray(page?.trades) ? page.trades : [],
        overlays: Array.isArray(page?.overlays) ? page.overlays : [],
        range: page?.range,
        evidenceSource: page?.evidence_source,
        tradeEvidence: page?.trade_evidence,
        overlayEvidence: page?.overlay_evidence,
        mergeMode: 'prepend',
        requestId: request.requestId,
      })
    } catch (err) {
      if (isAbortError(err) || !isCurrentChartRequest(request)) return
      dispatch({
        type: 'retrieval/chartFailed',
        runId: activeRunId,
        symbolKey: selectedSymbolKey,
        requestId: request.requestId,
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
    } finally {
      finishChartRequest(request)
    }
  }, [activeRunId, beginChartRequest, bot?.id, chartCandles, chartHistory?.range?.has_more_before, chartHistoryStatus, finishChartRequest, isCurrentChartRequest, logger, selectedSymbolKey])

  const focusEvidence = useCallback(async (evidence, {
    kind = 'evidence',
    focusValue = evidence?.entry_time || evidence?.opened_at || evidence?.bar_time || evidence?.known_at || evidence?.event_ts,
    endValue = evidence?.exit_time || evidence?.closed_at,
    evidenceId = evidence?.trade_id || evidence?.decision_id || evidence?.event_id || focusValue,
  } = {}) => {
    const focusEpochMs = Date.parse(focusValue || '')
    if (!activeRunId || !selectedSymbolKey || !Number.isFinite(focusEpochMs)) return
    const timeframeText = String(selectedSymbolMetadata?.timeframe || selectedSummary?.timeframe || '1h').trim().toLowerCase()
    const timeframeMatch = timeframeText.match(/(\d+)([mhd])/)
    const unitSeconds = timeframeMatch?.[2] === 'd' ? 86400 : timeframeMatch?.[2] === 'm' ? 60 : 3600
    const timeframeSeconds = Math.max(60, Number(timeframeMatch?.[1] || 1) * unitSeconds)
    const exitEpochMs = Date.parse(endValue || '')
    const startTime = new Date(focusEpochMs - timeframeSeconds * 72 * 1000).toISOString()
    const endAnchorMs = Number.isFinite(exitEpochMs) ? exitEpochMs : focusEpochMs
    const endTime = new Date(endAnchorMs + timeframeSeconds * 72 * 1000).toISOString()
    const request = beginChartRequest({
      runId: activeRunId,
      symbolKey: selectedSymbolKey,
      requestKey: `focus:${kind}:${evidenceId}:${startTime}:${endTime}`,
    })
    if (!request) return
    try {
      const page = await fetchBotLensChartHistory(activeRunId, selectedSymbolKey, {
        startTime,
        endTime,
        limit: 320,
        signal: request.controller.signal,
      })
      if (!isCurrentChartRequest(request)) return
      if (String(page?.run_id || '') !== String(activeRunId)) throw new Error(`${kind} focus returned a mismatched run scope`)
      if (normalizeSeriesKey(page?.symbol_key || '') !== normalizeSeriesKey(selectedSymbolKey)) {
        throw new Error(`${kind} focus returned a mismatched symbol scope`)
      }
      dispatch({
        type: 'retrieval/chartSuccess',
        runId: activeRunId,
        symbolKey: selectedSymbolKey,
        candles: Array.isArray(page?.candles) ? page.candles : [],
        trades: Array.isArray(page?.trades) ? page.trades : [],
        overlays: Array.isArray(page?.overlays) ? page.overlays : [],
        range: page?.range,
        evidenceSource: page?.evidence_source,
        tradeEvidence: page?.trade_evidence,
        overlayEvidence: page?.overlay_evidence,
        mergeMode: 'replace',
        focusTime: focusValue,
        focusToken: `${kind}:${evidenceId}:${request.requestId}`,
        requestId: request.requestId,
      })
    } catch (err) {
      if (isAbortError(err) || !isCurrentChartRequest(request)) return
      dispatch({
        type: 'retrieval/chartFailed',
        runId: activeRunId,
        symbolKey: selectedSymbolKey,
        requestId: request.requestId,
        error: err?.message || `${kind} chart focus failed`,
      })
      logger.warn('botlens_evidence_focus_failed', {
        bot_id: bot?.id || null,
        run_id: activeRunId,
        symbol_key: selectedSymbolKey,
        evidence_kind: kind,
        evidence_id: evidenceId || null,
      }, err)
    } finally {
      finishChartRequest(request)
    }
  }, [activeRunId, beginChartRequest, bot?.id, finishChartRequest, isCurrentChartRequest, logger, selectedSummary?.timeframe, selectedSymbolKey, selectedSymbolMetadata?.timeframe])

  const focusTrade = useCallback((trade) => focusEvidence(trade, {
    kind: 'trade',
    focusValue: trade?.entry_time || trade?.opened_at || trade?.event_ts,
    endValue: trade?.exit_time || trade?.closed_at,
    evidenceId: trade?.trade_id || trade?.event_id,
  }), [focusEvidence])

  const focusDecision = useCallback((decision) => focusEvidence(decision, {
    kind: 'decision',
    focusValue: decision?.bar_time || decision?.known_at || decision?.event_ts,
    evidenceId: decision?.decision_id || decision?.event_id,
  }), [focusEvidence])

  const clearError = useCallback(() => {
    dispatch({ type: 'ui/error', error: null })
  }, [])

  const closeModal = useCallback(() => {
    abortChartRequests()
    dispatch({ type: 'session/reset', botId: bot?.id || null })
    bootstrapLoadRef.current.clear()
    snapshotRefreshLoadRef.current.clear()
    initialChartLoadRef.current.clear()
    forensicLoadRef.current.clear()
    durableEvidenceRequestRef.current = {}
    durableEvidenceStageRef.current = { stageKey: null, token: null }
    setForensicReplay(emptyForensicReplayState())
    setDurableEvidence(emptyDurableEvidenceState())
    onClose?.()
  }, [abortChartRequests, bot?.id, onClose])

  return {
    activeRunId,
    chartCandles,
    chartHistory,
    chartHistoryCacheCount,
    chartHistoryStatus,
    chartOverlays: selectedSymbolOverlays,
    chartTrades,
    recentTrades: selectedSymbolRecentTrades,
    clearError,
    closeModal,
    changeSelectedSymbol,
    error: state.ui.error,
    durableEvidence,
    loadDecisionEvidencePage,
    loadTradeEvidencePage,
    loadDiagnosticEvidencePage,
    focusDecision,
    focusTrade,
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
