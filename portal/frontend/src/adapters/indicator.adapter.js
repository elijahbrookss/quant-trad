import { createLogger } from '../utils/logger.js';

const BASE = import.meta.env.REACT_APP_API_BASE_URL || 'http://localhost:8000'
const adapterLogger = createLogger('IndicatorAdapter');

async function handleResponse(res) {
  if (res.ok) {
    return res.status === 204 ? null : res.json()
  }

  const contentType = res.headers.get('content-type') || ''
  let payload = null

  try {
    if (contentType.includes('application/json')) {
      payload = await res.json()
    } else {
      const text = await res.text()
      payload = text || null
    }
  } catch (err) {
    adapterLogger.warn('error_response_parse_failed', {
      status: res.status,
      url: res.url,
      contentType,
    }, err)
  }

  const detail =
    (payload && typeof payload === 'object' && (payload.detail || payload.message)) ||
    (typeof payload === 'string' ? payload : null)

  const message = detail || res.statusText || `Request failed with status ${res.status}`
  const error = new Error(message)
  error.status = res.status
  if (payload && typeof payload === 'object') {
    error.payload = payload
  }

  throw error
}

export async function fetchIndicators() {
  const res = await fetch(`${BASE}/api/indicators/`, { mode: 'cors' })
  return handleResponse(res)
}

export async function fetchIndicatorTypes() {
  const res = await fetch(`${BASE}/api/indicators-types/`, { mode: 'cors' })
  return handleResponse(res)
}

export async function fetchIndicatorType(id) {
    const res = await fetch(`${BASE}/api/indicators-types/${id}`, { mode: 'cors' })
    return handleResponse(res)
}

export async function createIndicator({ type, name, params }) {
  adapterLogger.debug('create_indicator_request', {
    type,
    hasName: Boolean(name),
    paramKeys: Object.keys(params || {}),
  })
  const res = await fetch(`${BASE}/api/indicators/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type, name, params }),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function updateIndicator(id, { type, name, params }) {
  const res = await fetch(`${BASE}/api/indicators/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type, name, params }),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function deleteIndicator(id) {
  const res = await fetch(`${BASE}/api/indicators/${id}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function fetchIndicatorOverlays(id, { start, end, interval, symbol }) {
  adapterLogger.debug('fetch_indicator_overlays_request', { id, start, end, interval, symbol })
  const res = await fetch(`${BASE}/api/indicators/${id}/overlays`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ start, end, interval, symbol }),
    mode: 'cors',
  });
  return handleResponse(res);
}

export async function generateIndicatorSignals(
  id,
  { start, end, interval, symbol, config } = {},
) {
  const payload = {
    start,
    end,
    interval,
  };

  if (symbol) payload.symbol = symbol;

  const cfgEntries = Object.entries(config || {}).filter(([, v]) => v !== undefined && v !== null);
  if (cfgEntries.length) {
    payload.config = Object.fromEntries(cfgEntries);
  }

  const res = await fetch(`${BASE}/api/indicators/${id}/signals`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  });

  return handleResponse(res);
}
