import { createLogger } from '../utils/logger.js'

import { API_ORIGIN as BASE } from '../config/appConfig.js'
const adapterLogger = createLogger('StrategyAdapter')

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
    adapterLogger.warn(
      'strategy_adapter_response_parse_failed',
      {
        status: res.status,
        url: res.url,
        contentType,
      },
      err,
    )
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

/** Fetch all strategy records. */
export async function fetchStrategies() {
  const res = await fetch(`${BASE}/api/strategies/`, { mode: 'cors' })
  return handleResponse(res)
}

/** Create a new strategy. */
export async function createStrategy(payload) {
  const res = await fetch(`${BASE}/api/strategies/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Update strategy metadata. */
export async function updateStrategy(strategyId, payload) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Delete a strategy. */
export async function deleteStrategy(strategyId) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Attach an indicator instance to a strategy. */
export async function attachStrategyIndicator(strategyId, indicatorId) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/indicators/${indicatorId}`, {
    method: 'POST',
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Detach an indicator instance from a strategy. */
export async function detachStrategyIndicator(strategyId, indicatorId) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/indicators/${indicatorId}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Create a rule for a strategy. */
export async function createStrategyRule(strategyId, payload) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/rules`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Update an existing strategy rule. */
export async function updateStrategyRule(strategyId, ruleId, payload) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/rules/${ruleId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Delete a strategy rule. */
export async function deleteStrategyRule(strategyId, ruleId) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/rules/${ruleId}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Run a rule-logic preview for a strategy over the requested window. */
export async function runStrategyPreview(strategyId, payload) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/preview`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function fetchSymbolPresets() {
  const res = await fetch(`${BASE}/api/strategies/presets/symbols`, { mode: 'cors' })
  return handleResponse(res)
}

export async function saveSymbolPreset(preset) {
  const res = await fetch(`${BASE}/api/strategies/presets/symbols`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(preset),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function deleteSymbolPreset(presetId) {
  const res = await fetch(`${BASE}/api/strategies/presets/symbols/${presetId}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function fetchATMTemplates() {
  const res = await fetch(`${BASE}/api/strategies/atm-templates`, { mode: 'cors' })
  return handleResponse(res)
}

export async function saveATMTemplate(payload) {
  const res = await fetch(`${BASE}/api/strategies/atm-templates`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}
