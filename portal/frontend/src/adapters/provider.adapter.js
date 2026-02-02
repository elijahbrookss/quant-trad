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
  log.debug('providers_fetch_request')
  const res = await fetch(`${BASE}/api/providers/`, { mode: 'cors' })
  log.debug('providers_fetch_response', { status: res.status })
  return handleResponse(res)
}

export async function validateProviderSelection(payload) {
  log.debug('provider_validate_request', payload)
  const res = await fetch(`${BASE}/api/providers/validate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function fetchTickMetadata(payload) {
  log.debug('provider_tick_metadata_request', payload)
  const res = await fetch(`${BASE}/api/providers/tick-metadata`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
    })
  return handleResponse(res)
}

export async function saveProviderCredentials(payload) {
  const res = await fetch(`${BASE}/api/providers/credentials`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}
