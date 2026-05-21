import { createLogger } from '../utils/logger.js'

import { API_ORIGIN as BASE } from '../config/appConfig.js'
const log = createLogger('InstrumentAdapter')

async function handleResponse(res) {
  if (res.ok) {
    return res.status === 204 ? null : res.json()
  }

  let payload = null
  const contentType = res.headers.get('content-type') || ''
  try {
    if (contentType.includes('application/json')) {
      payload = await res.json()
    } else {
      payload = await res.text()
    }
  } catch (err) {
    log.warn('instrument_response_parse_failed', { status: res.status, url: res.url }, err)
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

export async function fetchInstruments() {
  const res = await fetch(`${BASE}/api/instruments/`, { mode: 'cors' })
  return handleResponse(res)
}

export async function createInstrument(payload) {
  const res = await fetch(`${BASE}/api/instruments/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function updateInstrument(instrumentId, payload) {
  const res = await fetch(`${BASE}/api/instruments/${instrumentId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function deleteInstrument(instrumentId) {
  const res = await fetch(`${BASE}/api/instruments/${instrumentId}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function resolveInstrument(payload) {
  const res = await fetch(`${BASE}/api/instruments/resolve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}
