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
  return request('/api/bots')
}

export async function fetchBotRuntimeCapacity() {
  return request('/api/bots/runtime-capacity')
}

export async function createBot(payload) {
  log.info('create_bot', payload)
  return request('/api/bots', {
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



export async function fetchBotSettingsCatalog() {
  return request('/api/bots/settings-catalog')
}


export async function fetchBotActiveRun(botId) {
  return request(`/api/bots/${encodeURIComponent(botId)}/active-run`)
}

export async function fetchBotRuns(botId, { limit = 25 } = {}) {
  const params = new URLSearchParams()
  params.set('limit', String(Math.max(1, Number(limit) || 25)))
  return request(`/api/bots/${encodeURIComponent(botId)}/runs?${params.toString()}`)
}

export async function fetchBotLensSeriesCatalog(runId) {
  return request(`/api/bots/runs/${encodeURIComponent(runId)}/series`)
}



export async function fetchBotRunLedgerEvents(botId, runId, { afterSeq = 0, limit = 500, eventNames } = {}) {
  const params = new URLSearchParams()
  params.set('after_seq', String(Math.max(0, Number(afterSeq) || 0)))
  params.set('limit', String(Math.max(1, Number(limit) || 500)))
  if (Array.isArray(eventNames)) {
    eventNames.forEach((name) => {
      if (name === undefined || name === null) return
      const normalized = String(name).trim()
      if (!normalized) return
      params.append('event_name', normalized)
    })
  }
  const query = params.toString()
  return request(
    `/api/bots/${encodeURIComponent(botId)}/runs/${encodeURIComponent(runId)}/events${query ? `?${query}` : ''}`,
  )
}


export async function fetchBotLensSeriesWindow(runId, seriesKey, { to = 'now', limit = 320 } = {}) {
  const params = new URLSearchParams()
  if (to) params.set('to', String(to))
  params.set('limit', String(Math.max(1, Number(limit) || 320)))
  return request(`/api/bots/runs/${encodeURIComponent(runId)}/series/${encodeURIComponent(seriesKey)}/window?${params.toString()}`)
}

export async function fetchBotLensSeriesHistory(runId, seriesKey, { beforeTs, limit = 320 } = {}) {
  const params = new URLSearchParams()
  if (beforeTs) params.set('before_ts', String(beforeTs))
  params.set('limit', String(Math.max(1, Number(limit) || 320)))
  return request(`/api/bots/runs/${encodeURIComponent(runId)}/series/${encodeURIComponent(seriesKey)}/history?${params.toString()}`)
}

export function openBotLensSeriesLiveStream(runId, seriesKey, { limit = 320 } = {}) {
  const params = new URLSearchParams()
  params.set('limit', String(Math.max(1, Number(limit) || 320)))
  const path = `/api/bots/ws/runs/${encodeURIComponent(runId)}/series/${encodeURIComponent(seriesKey)}/live?${params.toString()}`
  return openWebSocket(path, { base: BASE })
}
