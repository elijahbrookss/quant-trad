import { createLogger } from '../utils/logger.js'

const log = createLogger('RealtimeAdapter')
const API_BASE = import.meta.env.REACT_APP_API_BASE_URL || 'http://localhost:8000'

function normalizeBase(base = API_BASE) {
  return String(base || '').trim() || 'http://localhost:8000'
}

export function toWebSocketBase(base = API_BASE) {
  const normalized = normalizeBase(base)
  if (normalized.startsWith('https://')) return normalized.replace('https://', 'wss://')
  if (normalized.startsWith('http://')) return normalized.replace('http://', 'ws://')
  return normalized
}

export function createRealtimeUrl(path, { transport = 'http', base } = {}) {
  const resolvedBase = transport === 'ws' ? toWebSocketBase(base) : normalizeBase(base)
  return new URL(path, resolvedBase)
}

export function openSse(path, { withCredentials = false, base } = {}) {
  try {
    const url = createRealtimeUrl(path, { transport: 'http', base })
    return new EventSource(url, { withCredentials })
  } catch (err) {
    log.warn('realtime_sse_open_failed', { path }, err)
    return null
  }
}

export function openWebSocket(path, { protocols, base } = {}) {
  try {
    const url = createRealtimeUrl(path, { transport: 'ws', base })
    return new WebSocket(url, protocols)
  } catch (err) {
    log.warn('realtime_ws_open_failed', { path }, err)
    return null
  }
}
