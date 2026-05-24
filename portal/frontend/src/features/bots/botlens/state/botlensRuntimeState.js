import {
  RUN_FAULT_DELTA_TYPE,
  RUN_HEALTH_DELTA_TYPE,
  RUN_LIFECYCLE_DELTA_TYPE,
  RUN_OPEN_TRADES_DELTA_TYPE,
  RUN_SYMBOL_CATALOG_DELTA_TYPE,
  applyOpenTradesDelta,
  applyRunFaultDelta,
  applyRunHealthDelta,
  applyRunLifecycleDelta,
  applyRunSymbolCatalogDelta,
  applySelectedSymbolBootstrap,
  applyTypedSymbolDelta,
  createRunStore,
  isTypedSymbolDeltaMessage,
  mergeCanonicalCandles,
  normalizeSeriesKey,
  selectSymbol,
} from '../../../../components/bots/botlensProjection.js'

function buildInitialUiState() {
  return {
    statusMessage: '',
    error: null,
  }
}

function buildInitialRetrievalState() {
  return {
    chartHistoryBySymbol: {},
    forensics: {
      events: {},
      signals: {},
    },
  }
}

export function createInitialBotLensState({ botId = null } = {}) {
  return {
    status: 'idle',
    botId: botId || null,
    runState: null,
    selectedSymbolKey: null,
    symbolBootstrapStatusByKey: {},
    live: {
      connectionState: 'idle',
      sessionId: null,
      lastStreamSeq: 0,
      subscribedSymbolKey: null,
      reconnectAttempt: 0,
    },
    retrieval: buildInitialRetrievalState(),
    ui: buildInitialUiState(),
  }
}

function splitProjectionStore(projectionStore) {
  if (!projectionStore) {
    return {
      runState: null,
      selectedSymbolKey: null,
      live: {
        sessionId: null,
        lastStreamSeq: 0,
      },
    }
  }
  const {
    streamSessionId,
    lastStreamSeq,
    selectedSymbolKey,
    transportEligible,
    live,
    ...rest
  } = projectionStore

  return {
    runState: {
      ...rest,
      transportEligible: Boolean(transportEligible ?? live),
    },
    selectedSymbolKey: normalizeSeriesKey(selectedSymbolKey || '') || null,
    live: {
      sessionId: streamSessionId ? String(streamSessionId) : null,
      lastStreamSeq: Number(lastStreamSeq || 0) || 0,
    },
  }
}

export function getBotLensProjectionStore(state) {
  if (!state?.runState) return null
  return {
    ...state.runState,
    transportEligible: Boolean(state.runState.transportEligible),
    selectedSymbolKey: normalizeSeriesKey(state.selectedSymbolKey || '') || null,
    streamSessionId: state.live?.sessionId || null,
    lastStreamSeq: Number(state.live?.lastStreamSeq || 0) || 0,
  }
}

function commitProjectionStore(state, projectionStore) {
  const projected = splitProjectionStore(projectionStore)
  return {
    ...state,
    runState: projected.runState,
    selectedSymbolKey: projected.selectedSymbolKey,
    live: {
      ...state.live,
      sessionId: projected.live.sessionId,
      lastStreamSeq: projected.live.lastStreamSeq,
    },
  }
}

function updateChartHistoryCache(cache, { symbolKey, candles, range }) {
  const existing = cache?.[symbolKey] || null
  return {
    ...(cache || {}),
    [symbolKey]: {
      symbolKey,
      status: 'ready',
      error: null,
      candles: mergeCanonicalCandles(candles || [], existing?.candles || []),
      range: range && typeof range === 'object' ? { ...range } : existing?.range || null,
    },
  }
}

function matchesActiveRun(state, runId) {
  const requestedRunId = String(runId || '').trim()
  if (!requestedRunId) return true
  const activeRunId = String(state?.runState?.runMeta?.run_id || '').trim()
  if (!activeRunId) return true
  return activeRunId === requestedRunId
}

function applyLiveProjectionMessage(state, message) {
  const projectionStore = getBotLensProjectionStore(state)
  if (!projectionStore) return state

  let nextProjectionStore = projectionStore
  switch (String(message?.type || '')) {
    case RUN_LIFECYCLE_DELTA_TYPE:
      nextProjectionStore = applyRunLifecycleDelta(projectionStore, message)
      break
    case RUN_HEALTH_DELTA_TYPE:
      nextProjectionStore = applyRunHealthDelta(projectionStore, message)
      break
    case RUN_FAULT_DELTA_TYPE:
      nextProjectionStore = applyRunFaultDelta(projectionStore, message)
      break
    case RUN_SYMBOL_CATALOG_DELTA_TYPE:
      nextProjectionStore = applyRunSymbolCatalogDelta(projectionStore, message)
      break
    case RUN_OPEN_TRADES_DELTA_TYPE:
      nextProjectionStore = applyOpenTradesDelta(projectionStore, message)
      break
    default:
      if (isTypedSymbolDeltaMessage(message)) {
        nextProjectionStore = applyTypedSymbolDelta(projectionStore, message)
      }
      break
  }

  return commitProjectionStore(state, nextProjectionStore)
}

export function reduceBotLensState(state, action) {
  switch (action.type) {
    case 'session/reset':
      return createInitialBotLensState({ botId: action.botId || null })

    case 'run/bootstrapStarted':
      return {
        ...createInitialBotLensState({ botId: action.botId || state.botId || null }),
        status: 'bootstrapping',
        ui: {
          statusMessage: action.statusMessage || 'Bootstrapping BotLens run...',
          error: null,
        },
      }

    case 'run/bootstrapPending':
      return {
        ...state,
        status: 'bootstrapping',
        ui: {
          statusMessage: action.statusMessage || state.ui.statusMessage,
          error: null,
        },
      }

    case 'run/bootstrapUnavailable':
      return {
        ...state,
        status: 'idle',
        runState: null,
        selectedSymbolKey: null,
        live: {
          ...state.live,
          connectionState: 'idle',
          sessionId: null,
          lastStreamSeq: 0,
          subscribedSymbolKey: null,
          reconnectAttempt: 0,
        },
        retrieval: buildInitialRetrievalState(),
        ui: {
          statusMessage: action.statusMessage || 'BotLens run bootstrap unavailable.',
          error: null,
        },
      }

    case 'run/bootstrapReady': {
      const projectionStore = createRunStore(action.runBootstrap)
      const projected = splitProjectionStore(projectionStore)
      const selectedSymbolKey = projected.selectedSymbolKey
      const selectedSymbolReady = Boolean(
        selectedSymbolKey
        && projected.runState?.symbolStates?.[selectedSymbolKey]?.readiness?.snapshot_ready,
      )
      return {
        ...state,
        status: 'ready',
        runState: projected.runState,
        selectedSymbolKey: projected.selectedSymbolKey,
        symbolBootstrapStatusByKey: selectedSymbolKey && selectedSymbolReady
          ? { [selectedSymbolKey]: 'ready' }
          : {},
        live: {
          ...state.live,
          connectionState: projected.runState?.transportEligible ? 'connecting' : 'historical',
          sessionId: projected.live.sessionId,
          lastStreamSeq: projected.live.lastStreamSeq,
          subscribedSymbolKey: null,
          reconnectAttempt: 0,
        },
        retrieval: buildInitialRetrievalState(),
        ui: {
          statusMessage: action.statusMessage || 'BotLens bootstrap ready.',
          error: null,
        },
      }
    }

    case 'run/bootstrapFailed':
      return {
        ...state,
        status: 'error',
        runState: null,
        selectedSymbolKey: null,
        live: {
          ...state.live,
          connectionState: 'error',
          sessionId: null,
          lastStreamSeq: 0,
          subscribedSymbolKey: null,
        },
        retrieval: buildInitialRetrievalState(),
        ui: {
          statusMessage: action.statusMessage || 'BotLens bootstrap failed.',
          error: action.error || 'BotLens bootstrap failed',
        },
      }

    case 'selection/requested': {
      const normalizedSymbolKey = normalizeSeriesKey(action.symbolKey || '')
      if (!normalizedSymbolKey) return state
      const currentSelectedSymbolKey = normalizeSeriesKey(state.selectedSymbolKey || '')
      if (currentSelectedSymbolKey === normalizedSymbolKey) {
        return state
      }
      const projectionStore = getBotLensProjectionStore(state)
      if (!projectionStore) return state
      const selectedProjectionStore = selectSymbol(projectionStore, normalizedSymbolKey)
      const hasCachedSymbolState = Boolean(
        selectedProjectionStore?.symbolStates?.[normalizedSymbolKey]?.readiness?.snapshot_ready,
      )
      return {
        ...commitProjectionStore(state, selectedProjectionStore),
        symbolBootstrapStatusByKey: {
          ...state.symbolBootstrapStatusByKey,
          [normalizedSymbolKey]: hasCachedSymbolState ? 'ready' : 'idle',
        },
        ui: {
          ...state.ui,
          error: null,
        },
      }
    }

    case 'selection/bootstrapStarted': {
      const normalizedSymbolKey = normalizeSeriesKey(action.symbolKey || '')
      if (!normalizedSymbolKey) return state
      if (normalizeSeriesKey(state.selectedSymbolKey || '') !== normalizedSymbolKey) {
        return state
      }
      return {
        ...state,
        symbolBootstrapStatusByKey: {
          ...state.symbolBootstrapStatusByKey,
          [normalizedSymbolKey]: 'loading',
        },
        ui: {
          ...state.ui,
          error: null,
          statusMessage: action.statusMessage || state.ui.statusMessage,
        },
      }
    }

    case 'selection/bootstrapPending': {
      const normalizedSymbolKey = normalizeSeriesKey(action.symbolKey || '')
      if (!normalizedSymbolKey) return state
      if (normalizeSeriesKey(state.selectedSymbolKey || '') !== normalizedSymbolKey) {
        return state
      }
      return {
        ...state,
        symbolBootstrapStatusByKey: {
          ...state.symbolBootstrapStatusByKey,
          [normalizedSymbolKey]: 'loading',
        },
        ui: {
          ...state.ui,
          error: null,
          statusMessage: action.statusMessage || state.ui.statusMessage,
        },
      }
    }

    case 'selection/bootstrapReady': {
      if (!matchesActiveRun(state, action.runId || action.bootstrapPayload?.scope?.run_id)) return state
      const requestedSymbolKey = normalizeSeriesKey(
        action.symbolKey
          || action.bootstrapPayload?.selection?.selected_symbol_key
          || action.bootstrapPayload?.scope?.symbol_key
          || '',
      )
      if (requestedSymbolKey && requestedSymbolKey !== normalizeSeriesKey(state.selectedSymbolKey || '')) {
        return state
      }
      const projectionStore = getBotLensProjectionStore(state)
      if (!projectionStore) return state
      const nextState = commitProjectionStore(
        state,
        applySelectedSymbolBootstrap(projectionStore, action.bootstrapPayload),
      )
      const resolvedSymbolKey = normalizeSeriesKey(
        action.bootstrapPayload?.selection?.selected_symbol_key
          || action.bootstrapPayload?.scope?.symbol_key
          || nextState.selectedSymbolKey,
      )
      return {
        ...nextState,
        symbolBootstrapStatusByKey: resolvedSymbolKey
          ? {
              ...nextState.symbolBootstrapStatusByKey,
              [resolvedSymbolKey]: 'ready',
            }
          : nextState.symbolBootstrapStatusByKey,
        ui: {
          ...nextState.ui,
          error: null,
          statusMessage: action.statusMessage || nextState.ui.statusMessage,
        },
      }
    }

    case 'selection/bootstrapFailed': {
      const normalizedSymbolKey = normalizeSeriesKey(action.symbolKey || '')
      if (!normalizedSymbolKey) return state
      if (normalizeSeriesKey(state.selectedSymbolKey || '') !== normalizedSymbolKey) {
        return state
      }
      return {
        ...state,
        symbolBootstrapStatusByKey: {
          ...state.symbolBootstrapStatusByKey,
          [normalizedSymbolKey]: 'error',
        },
        ui: {
          ...state.ui,
          statusMessage: action.statusMessage || state.ui.statusMessage,
          error: action.error || state.ui.error,
        },
      }
    }

    case 'selection/bootstrapUnavailable': {
      const normalizedSymbolKey = normalizeSeriesKey(action.symbolKey || '')
      if (!normalizedSymbolKey) return state
      if (normalizeSeriesKey(state.selectedSymbolKey || '') !== normalizedSymbolKey) {
        return state
      }
      return {
        ...state,
        symbolBootstrapStatusByKey: {
          ...state.symbolBootstrapStatusByKey,
          [normalizedSymbolKey]: 'unavailable',
        },
        ui: {
          ...state.ui,
          error: null,
          statusMessage: action.statusMessage || state.ui.statusMessage,
        },
      }
    }

    case 'live/connectionStateChanged':
      return {
        ...state,
        live: {
          ...state.live,
          connectionState: action.connectionState,
        },
      }

    case 'live/connected':
      return {
        ...state,
        live: {
          ...state.live,
          connectionState: 'open',
          sessionId: String(action.message?.stream_session_id || '').trim() || state.live.sessionId,
          lastStreamSeq: Number(action.message?.stream_seq || 0) || state.live.lastStreamSeq,
        },
      }

    case 'live/reconnectAttempt':
      return {
        ...state,
        live: {
          ...state.live,
          reconnectAttempt: Math.max(0, Number(action.attempt || 0) || 0),
        },
      }

    case 'live/subscribedSymbol':
      return {
        ...state,
        live: {
          ...state.live,
          subscribedSymbolKey: normalizeSeriesKey(action.symbolKey || '') || null,
        },
      }

    case 'live/messageReceived':
      return applyLiveProjectionMessage(state, action.message)

    case 'retrieval/chartRequest': {
      if (!matchesActiveRun(state, action.runId)) return state
      const normalizedSymbolKey = normalizeSeriesKey(action.symbolKey || '')
      if (!normalizedSymbolKey) return state
      return {
        ...state,
        retrieval: {
          ...state.retrieval,
          chartHistoryBySymbol: {
            ...state.retrieval.chartHistoryBySymbol,
            [normalizedSymbolKey]: {
              ...(state.retrieval.chartHistoryBySymbol?.[normalizedSymbolKey] || {}),
              symbolKey: normalizedSymbolKey,
              status: 'loading',
              error: null,
            },
          },
        },
      }
    }

    case 'retrieval/chartSuccess': {
      if (!matchesActiveRun(state, action.runId)) return state
      const normalizedSymbolKey = normalizeSeriesKey(action.symbolKey || '')
      if (!normalizedSymbolKey) return state
      return {
        ...state,
        retrieval: {
          ...state.retrieval,
          chartHistoryBySymbol: updateChartHistoryCache(
            state.retrieval.chartHistoryBySymbol,
            {
              symbolKey: normalizedSymbolKey,
              candles: action.candles,
              range: action.range,
            },
          ),
        },
      }
    }

    case 'retrieval/chartFailed': {
      if (!matchesActiveRun(state, action.runId)) return state
      const normalizedSymbolKey = normalizeSeriesKey(action.symbolKey || '')
      if (!normalizedSymbolKey) return state
      return {
        ...state,
        retrieval: {
          ...state.retrieval,
          chartHistoryBySymbol: {
            ...state.retrieval.chartHistoryBySymbol,
            [normalizedSymbolKey]: {
              ...(state.retrieval.chartHistoryBySymbol?.[normalizedSymbolKey] || {}),
              symbolKey: normalizedSymbolKey,
              status: 'error',
              error: action.error || 'Chart retrieval failed',
            },
          },
        },
      }
    }

    case 'ui/statusMessage':
      return {
        ...state,
        ui: {
          ...state.ui,
          statusMessage: action.statusMessage || '',
        },
      }

    case 'ui/error':
      return {
        ...state,
        ui: {
          ...state.ui,
          error: action.error || null,
        },
      }

    default:
      return state
  }
}
