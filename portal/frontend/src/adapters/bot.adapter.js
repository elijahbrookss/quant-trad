import { createLogger } from '../utils/logger.js'

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

export async function pauseBot(botId) {
  return request(`/api/bots/${botId}/pause`, { method: 'POST' })
}

export async function resumeBot(botId) {
  return request(`/api/bots/${botId}/resume`, { method: 'POST' })
}

export async function fetchBotStatus(botId) {
  return request(`/api/bots/${botId}/status`)
}

export async function fetchBotPerformance(botId) {
  return request(`/api/bots/${botId}/performance`)
}

export async function fetchBotLogs(botId, limit = 200) {
  const params = new URLSearchParams()
  if (limit) params.set('limit', String(limit))
  const query = params.toString() ? `?${params.toString()}` : ''
  return request(`/api/bots/${botId}/logs${query}`)
}
