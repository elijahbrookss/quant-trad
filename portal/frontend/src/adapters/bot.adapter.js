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
    const text = await res.text()
    const message = text || res.statusText || 'Bot request failed'
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

export async function fetchBotPerformance(botId) {
  return request(`/api/bots/${botId}/performance`)
}
