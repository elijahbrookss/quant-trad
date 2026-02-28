import { createLogger } from '../utils/logger.js'
import { openSse, openWebSocket } from './realtime.adapter.js'

const BASE = import.meta.env.REACT_APP_API_BASE_URL || 'http://localhost:8000'
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

export async function fetchBotLensBootstrap(botId, { runId } = {}) {
  const query = runId ? `?run_id=${encodeURIComponent(runId)}` : ''
  return request(`/api/bots/${encodeURIComponent(botId)}/lens/bootstrap${query}`)
}

export function openBotLensStream(botId, { runId, sinceSeq = 0 } = {}) {
  const params = new URLSearchParams()
  if (runId) params.set('run_id', String(runId))
  params.set('since_seq', String(Number(sinceSeq) || 0))
  const qs = params.toString()
  const path = `/api/bots/ws/${encodeURIComponent(botId)}${qs ? `?${qs}` : ''}`
  const socket = openWebSocket(path, { base: BASE })
  if (!socket) {
    log.warn('bot_lens_stream_init_failed', { bot_id: botId, run_id: runId, since_seq: sinceSeq })
  }
  return socket
}
