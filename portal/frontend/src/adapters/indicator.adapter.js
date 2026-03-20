import { createLogger } from '../utils/logger.js';

import { API_BASE_URL } from '../config/appConfig.js'
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
  const res = await fetch(`${API_BASE_URL}/indicators/`, { mode: 'cors' })
  return handleResponse(res)
}

export async function fetchIndicatorTypes() {
  const res = await fetch(`${API_BASE_URL}/indicators/types`, { mode: 'cors' })
  return handleResponse(res)
}

export async function fetchIndicatorType(id) {
    const res = await fetch(`${API_BASE_URL}/indicators/types/${id}`, { mode: 'cors' })
    return handleResponse(res)
}

export async function fetchIndicator(id) {
  const res = await fetch(`${API_BASE_URL}/indicators/${id}`, { mode: 'cors' })
  return handleResponse(res)
}

export async function createIndicator({ type, name, params, dependencies = [], color }) {
  adapterLogger.debug('create_indicator_request', {
    type,
    hasName: Boolean(name),
    paramKeys: Object.keys(params || {}),
  })
  const body = { type, name, params, dependencies }
  if (color !== undefined) {
    body.color = color
  }
  const res = await fetch(`${API_BASE_URL}/indicators/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function updateIndicator(id, { type, name, params, dependencies = [], color }) {
  const body = { type, name, params, dependencies }
  if (color !== undefined) {
    body.color = color
  }
  const res = await fetch(`${API_BASE_URL}/indicators/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function deleteIndicator(id) {
  const res = await fetch(`${API_BASE_URL}/indicators/${id}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function setIndicatorEnabled(id, enabled) {
  const res = await fetch(`${API_BASE_URL}/indicators/${id}/enabled`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled }),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function bulkToggleIndicators(ids = [], enabled) {
  const res = await fetch(`${API_BASE_URL}/indicators/bulk/toggle`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ids, enabled }),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function bulkDeleteIndicators(ids = []) {
  const res = await fetch(`${API_BASE_URL}/indicators/bulk/delete`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ids }),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function duplicateIndicator(id, { name } = {}) {
  const payload = {}
  if (typeof name === 'string' && name.trim()) {
    payload.name = name.trim()
  }
  const hasBody = Object.keys(payload).length > 0
  const headers = hasBody ? { 'Content-Type': 'application/json' } : undefined
  const body = hasBody ? JSON.stringify(payload) : undefined
  const res = await fetch(`${API_BASE_URL}/indicators/${id}/duplicate`, {
    method: 'POST',
    headers,
    body,
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function fetchIndicatorOverlays(id, { start, end, interval, symbol, datasource, exchange, instrument_id }) {
  adapterLogger.debug('fetch_indicator_overlays_request', {
    id,
    start,
    end,
    interval,
    symbol,
    datasource,
    exchange,
    instrument_id,
  })
  const res = await fetch(`${API_BASE_URL}/indicators/${id}/overlays`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ start, end, interval, symbol, datasource, exchange, instrument_id }),
    mode: 'cors',
  });
  return handleResponse(res);
}

export async function generateIndicatorSignals(
  id,
  { start, end, interval, symbol, datasource, exchange, config } = {},
) {
  const payload = {
    start,
    end,
    interval,
  };

  if (symbol) payload.symbol = symbol;
  if (datasource) payload.datasource = datasource;
  if (exchange) payload.exchange = exchange;

  const cfgEntries = Object.entries(config || {}).filter(([, v]) => v !== undefined && v !== null);
  if (cfgEntries.length) {
    payload.config = Object.fromEntries(cfgEntries);
  }

  const res = await fetch(`${API_BASE_URL}/indicators/${id}/signals`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  });

  return handleResponse(res);
}

export async function fetchIndicatorStrategies(id) {
  const res = await fetch(`${API_BASE_URL}/indicators/${id}/strategies`, { mode: 'cors' })
  return handleResponse(res)
}
