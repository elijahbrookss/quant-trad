import { createLogger } from '../utils/logger.js'

const BASE = import.meta.env.REACT_APP_API_BASE_URL || 'http://localhost:8000'
const log = createLogger('ProviderAdapter')

async function handleResponse(res) {
  if (res.ok) {
    return res.status === 204 ? null : res.json()
  }

  let payload = null
  const contentType = res.headers.get('content-type') || ''
  try {
    payload = contentType.includes('application/json') ? await res.json() : await res.text()
  } catch (err) {
    log.warn('provider_response_parse_failed', { status: res.status, url: res.url }, err)
  }

  const detail =
    (payload && typeof payload === 'object' && (payload.detail || payload.message)) ||
    (typeof payload === 'string' ? payload : null)
  const error = new Error(detail || res.statusText || `Request failed with status ${res.status}`)
  error.status = res.status
  if (payload && typeof payload === 'object') {
    error.payload = payload
  }
  throw error
}

export async function fetchProviders() {
  const res = await fetch(`${BASE}/api/providers/`, { mode: 'cors' })
  return handleResponse(res)
}

export async function validateProviderSelection(payload) {
  const res = await fetch(`${BASE}/api/providers/validate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function fetchTickMetadata(payload) {
  const res = await fetch(`${BASE}/api/providers/tick-metadata`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}
