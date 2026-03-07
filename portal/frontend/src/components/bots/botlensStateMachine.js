export const BOTLENS_PHASES = {
  BOOTSTRAPPING: 'bootstrapping',
  WAITING_FOR_RUN: 'waiting_for_run',
  WAITING_FOR_SNAPSHOT: 'waiting_for_snapshot',
  CONNECTING: 'connecting_live',
  LIVE: 'live',
  HISTORICAL: 'historical',
  PAGING_HISTORY: 'paging_history',
  RESYNCING: 'resyncing',
  STALE: 'stale',
}

export const initialBotLensState = {
  phase: BOTLENS_PHASES.BOOTSTRAPPING,
  runId: null,
  seriesKey: null,
  seq: 0,
  candles: [],
}

function dedupeCandles(candles) {
  const byTime = new Map()
  ;(Array.isArray(candles) ? candles : []).forEach((candle) => {
    const t = Number(candle?.time)
    if (!Number.isFinite(t)) return
    byTime.set(t, candle)
  })
  return Array.from(byTime.entries())
    .sort((a, b) => a[0] - b[0])
    .map((entry) => entry[1])
}

export function botlensReducer(state, action) {
  switch (action.type) {
    case 'BOOTSTRAP_START':
      return {
        ...state,
        phase: BOTLENS_PHASES.BOOTSTRAPPING,
      }
    case 'WAITING_FOR_RUN':
      return {
        ...state,
        phase: BOTLENS_PHASES.WAITING_FOR_RUN,
      }
    case 'WAITING_FOR_SNAPSHOT':
      return {
        ...state,
        phase: BOTLENS_PHASES.WAITING_FOR_SNAPSHOT,
      }
    case 'LIVE_CONNECTING':
      return {
        ...state,
        phase: BOTLENS_PHASES.CONNECTING,
      }
    case 'BOOTSTRAP_SUCCESS': {
      return {
        ...state,
        phase: action.live === false ? BOTLENS_PHASES.HISTORICAL : BOTLENS_PHASES.LIVE,
        runId: action.runId,
        seriesKey: action.seriesKey,
        seq: Number(action.seq || 0),
        candles: dedupeCandles(action.candles || []),
      }
    }
    case 'LIVE_BAR_APPEND': {
      const nextSeq = Number(action.seq || 0)
      if (nextSeq <= Number(state.seq || 0)) return state
      return {
        ...state,
        phase: BOTLENS_PHASES.LIVE,
        seq: nextSeq,
        candles: dedupeCandles([...(state.candles || []), action.bar]),
      }
    }
    case 'LIVE_BAR_UPDATE': {
      const nextSeq = Number(action.seq || 0)
      if (nextSeq <= Number(state.seq || 0)) return state
      const current = Array.isArray(state.candles) ? state.candles.slice() : []
      const nextTime = Number(action.bar?.time)
      if (current.length && Number(current[current.length - 1]?.time) === nextTime) {
        current[current.length - 1] = action.bar
      } else {
        current.push(action.bar)
      }
      return {
        ...state,
        phase: BOTLENS_PHASES.LIVE,
        seq: nextSeq,
        candles: dedupeCandles(current),
      }
    }
    case 'HISTORY_PAGE_SUCCESS': {
      return {
        ...state,
        phase: state.phase === BOTLENS_PHASES.HISTORICAL ? BOTLENS_PHASES.HISTORICAL : BOTLENS_PHASES.LIVE,
        candles: dedupeCandles([...(action.candles || []), ...(state.candles || [])]),
      }
    }
    case 'HISTORY_PAGE_START':
      return {
        ...state,
        phase: BOTLENS_PHASES.PAGING_HISTORY,
      }
    case 'SEQ_GAP':
      return {
        ...state,
        phase: BOTLENS_PHASES.RESYNCING,
      }
    case 'STREAM_STALE':
      return {
        ...state,
        phase: BOTLENS_PHASES.STALE,
      }
    case 'LIVE_CONNECTED':
      return {
        ...state,
        phase: BOTLENS_PHASES.LIVE,
      }
    default:
      return state
  }
}
