import { createLogger } from '../utils/logger.js';

import { API_BASE_URL } from '../config/appConfig.js'
const adapterLogger = createLogger('IndicatorAdapter');

export function normalizeIndicatorRead(payload) {
  if (!payload || typeof payload !== 'object') {
    return null
  }

  const instance = payload.instance && typeof payload.instance === 'object' ? payload.instance : {}
  const manifest = payload.manifest && typeof payload.manifest === 'object' ? payload.manifest : {}
  const outputs = payload.outputs && typeof payload.outputs === 'object' ? payload.outputs : {}
  const capabilities =
    payload.capabilities && typeof payload.capabilities === 'object' ? payload.capabilities : {}

  return {
    ...instance,
    manifest,
    outputs,
    capabilities,
    typed_outputs: Array.isArray(outputs.typed) ? outputs.typed : [],
    overlay_outputs: Array.isArray(outputs.overlays) ? outputs.overlays : [],
    runtime_supported: Boolean(capabilities.runtime_supported),
    compute_supported: Boolean(capabilities.compute_supported),
  }
}

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
  const res = await fetch(`${API_BASE_URL}/indicators/`, { mode: 'cors', cache: 'no-store' })
  return handleResponse(res)
}

export async function fetchIndicatorTypes() {
  const res = await fetch(`${API_BASE_URL}/indicators/types`, { mode: 'cors', cache: 'no-store' })
  return handleResponse(res)
}

export async function fetchIndicatorType(id) {
    const res = await fetch(`${API_BASE_URL}/indicators/types/${id}`, { mode: 'cors', cache: 'no-store' })
    return handleResponse(res)
}

export async function fetchIndicator(id) {
  const res = await fetch(`${API_BASE_URL}/indicators/${id}`, { mode: 'cors', cache: 'no-store' })
  return normalizeIndicatorRead(await handleResponse(res))
}

export async function createIndicator({ type, name, params, dependencies = [], output_prefs = {}, color, color_palette }) {
  adapterLogger.debug('create_indicator_request', {
    type,
    hasName: Boolean(name),
    paramKeys: Object.keys(params || {}),
  })
  const body = { type, name, params, dependencies, output_prefs }
  if (color !== undefined) {
    body.color = color
  }
  if (color_palette !== undefined) {
    body.color_palette = color_palette
  }
  const res = await fetch(`${API_BASE_URL}/indicators/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    mode: 'cors',
  })
  return normalizeIndicatorRead(await handleResponse(res))
}

export async function updateIndicator(id, { type, name, params, dependencies = [], output_prefs = {}, color, color_palette }) {
  const body = { type, name, params, dependencies, output_prefs }
  adapterLogger.debug('update_indicator_request', {
    id,
    type,
    dependencyCount: dependencies.length,
    outputPrefs: output_prefs,
  })
  if (color !== undefined) {
    body.color = color
  }
  if (color_palette !== undefined) {
    body.color_palette = color_palette
  }
  const res = await fetch(`${API_BASE_URL}/indicators/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    mode: 'cors',
  })
  const payload = normalizeIndicatorRead(await handleResponse(res))
  adapterLogger.debug('update_indicator_response', {
    id,
    outputPrefs: payload?.output_prefs || null,
    typedOutputs: Array.isArray(payload?.typed_outputs)
      ? payload.typed_outputs
          .filter((entry) => entry?.type === 'signal')
          .map((entry) => ({ name: entry?.name, enabled: entry?.enabled !== false }))
      : null,
  })
  return payload
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
  return normalizeIndicatorRead(await handleResponse(res))
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
  return normalizeIndicatorRead(await handleResponse(res))
}

export async function fetchIndicatorOverlays(
  id,
  {
    start,
    end,
    interval,
    symbol,
    datasource,
    exchange,
    instrument_id,
    cursor_epoch,
    cursor_time,
  },
) {
  adapterLogger.debug('fetch_indicator_overlays_request', {
    id,
    start,
    end,
    interval,
    symbol,
    datasource,
    exchange,
    instrument_id,
    cursor_epoch,
    cursor_time,
  })
  const payload = { start, end, interval, symbol, datasource, exchange, instrument_id }
  if (cursor_epoch !== undefined && cursor_epoch !== null) {
    payload.cursor_epoch = cursor_epoch
  }
  if (cursor_time) {
    payload.cursor_time = cursor_time
  }
  const res = await fetch(`${API_BASE_URL}/indicators/${id}/overlays`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  });
  return handleResponse(res);
}

export async function generateIndicatorSignals(
  id,
  { start, end, interval, symbol, datasource, exchange, instrument_id, config } = {},
) {
  const payload = {
    start,
    end,
    interval,
  };

  if (symbol) payload.symbol = symbol;
  if (datasource) payload.datasource = datasource;
  if (exchange) payload.exchange = exchange;
  if (instrument_id) payload.instrument_id = instrument_id;

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
