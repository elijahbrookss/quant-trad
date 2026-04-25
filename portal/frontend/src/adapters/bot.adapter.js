import { createLogger } from '../utils/logger.js'
import { API_ORIGIN } from '../config/appConfig.js'
import { openSse, openWebSocket } from './realtime.adapter.js'

const BASE = API_ORIGIN
const log = createLogger('BotAdapter')

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    method: 'GET',
    headers: { 'Content-Type': 'application/json' },
    mode: 'cors',
    ...options,
  })
  if (!res.ok) {
    let detail = null
    const contentType = res.headers.get('content-type') || ''
    try {
      if (contentType.includes('application/json')) {
        const payload = await res.json()
        detail = payload?.detail || payload?.message || null
      } else {
        detail = await res.text()
      }
    } catch (err) {
      log.warn('bot_request_parse_failed', { path, status: res.status }, err)
    }
    const message = detail || res.statusText || 'Bot request failed'
    throw new Error(message)
  }
  if (res.status === 204) return null
  return res.json()
}

export async function listBots() {
  return request('/api/bots/')
}

export async function fetchBotRuntimeCapacity() {
  return request('/api/bots/runtime-capacity')
}

export async function createBot(payload) {
  log.info('create_bot', payload)
  return request('/api/bots/', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

export async function updateBot(botId, payload) {
  return request(`/api/bots/${botId}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  })
}

export async function deleteBot(botId) {
  return request(`/api/bots/${botId}`, { method: 'DELETE' })
}

export async function startBot(botId) {
  return request(`/api/bots/${botId}/start`, { method: 'POST' })
}

export async function stopBot(botId) {
  return request(`/api/bots/${botId}/stop`, { method: 'POST' })
}



export function openBotsStream() {
  const source = openSse('/api/bots/stream', { withCredentials: false, base: BASE })
  if (!source) {
    log.warn('bots_stream_init_failed')
  }
  return source
}

export async function fetchBotActiveRun(botId) {
  return request(`/api/bots/${encodeURIComponent(botId)}/active-run`)
}

export async function fetchBotRuns(botId, { limit = 25 } = {}) {
  const params = new URLSearchParams()
  params.set('limit', String(Math.max(1, Number(limit) || 25)))
  return request(`/api/bots/${encodeURIComponent(botId)}/runs?${params.toString()}`)
}

export async function fetchBotLensRunBootstrap(botId) {
  return request(`/api/bots/${encodeURIComponent(botId)}/botlens/bootstrap/run`)
}



export async function fetchBotRunLifecycleEvents(botId, runId) {
  return request(`/api/bots/${encodeURIComponent(botId)}/runs/${encodeURIComponent(runId)}/lifecycle-events`)
}


export async function fetchBotLensSelectedSymbolSnapshot(runId, seriesKey, { limit = 320 } = {}) {
  const params = new URLSearchParams()
  params.set('limit', String(Math.max(1, Number(limit) || 320)))
  return request(`/api/bots/runs/${encodeURIComponent(runId)}/series/${encodeURIComponent(seriesKey)}/snapshot?${params.toString()}`)
}

export const fetchBotLensSelectedSymbolBootstrap = fetchBotLensSelectedSymbolSnapshot
export const fetchBotLensSelectedSymbolVisual = fetchBotLensSelectedSymbolSnapshot

export async function fetchBotLensChartHistory(runId, seriesKey, { startTime, endTime, limit = 320 } = {}) {
  const params = new URLSearchParams()
  if (startTime) params.set('start_time', String(startTime))
  if (endTime) params.set('end_time', String(endTime))
  params.set('limit', String(Math.max(1, Number(limit) || 320)))
  return request(`/api/bots/runs/${encodeURIComponent(runId)}/series/${encodeURIComponent(seriesKey)}/chart?${params.toString()}`)
}

export function openBotLensLiveStream(botId, {
  resumeFromSeq = 0,
  streamSessionId = null,
  selectedSymbolKey = null,
} = {}) {
  const params = new URLSearchParams()
  params.set('resume_from_seq', String(Math.max(0, Number(resumeFromSeq) || 0)))
  if (streamSessionId) params.set('stream_session_id', String(streamSessionId))
  if (selectedSymbolKey) params.set('selected_symbol_key', String(selectedSymbolKey))
  const query = params.toString()
  const path = `/api/bots/ws/${encodeURIComponent(botId)}/botlens/live${query ? `?${query}` : ''}`
  return openWebSocket(path, { base: BASE })
}
