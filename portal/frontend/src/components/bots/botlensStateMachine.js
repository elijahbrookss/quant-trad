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
      }
    }
    case 'LIVE_BAR_APPEND': {
      const nextSeq = Number(action.seq || 0)
      if (nextSeq <= Number(state.seq || 0)) return state
      return {
        ...state,
        phase: BOTLENS_PHASES.LIVE,
        seq: nextSeq,
      }
    }
    case 'LIVE_BAR_UPDATE': {
      const nextSeq = Number(action.seq || 0)
      if (nextSeq <= Number(state.seq || 0)) return state
      return {
        ...state,
        phase: BOTLENS_PHASES.LIVE,
        seq: nextSeq,
      }
    }
    case 'HISTORY_PAGE_SUCCESS': {
      return {
        ...state,
        phase: state.phase === BOTLENS_PHASES.HISTORICAL ? BOTLENS_PHASES.HISTORICAL : BOTLENS_PHASES.LIVE,
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
