import { createLogger } from '../utils/logger.js'

import { API_ORIGIN as BASE } from '../config/appConfig.js'
const adapterLogger = createLogger('StrategyAdapter')

const normalizeStrategyCore = (strategy = {}) => ({
  id: strategy?.id ?? null,
  name: strategy?.name ?? '',
  description: strategy?.description ?? null,
  timeframe: strategy?.timeframe ?? '',
  datasource: strategy?.datasource ?? '',
  exchange: strategy?.exchange ?? '',
  provider_id: strategy?.provider_id ?? null,
  venue_id: strategy?.venue_id ?? null,
  atm_template_id: strategy?.atm_template_id ?? null,
  atm_template:
    strategy?.atm_template && typeof strategy.atm_template === 'object'
      ? { ...strategy.atm_template }
      : {},
  risk_config:
    strategy?.risk_config && typeof strategy.risk_config === 'object'
      ? { ...strategy.risk_config }
      : {},
  created_at: strategy?.created_at ?? null,
  updated_at: strategy?.updated_at ?? null,
})

const normalizeStrategyBindings = (bindings = {}) => ({
  symbols: Array.isArray(bindings?.symbols) ? bindings.symbols : [],
  instrument_slots: Array.isArray(bindings?.instrument_slots) ? bindings.instrument_slots : [],
  instruments: Array.isArray(bindings?.instruments) ? bindings.instruments : [],
  indicator_ids: Array.isArray(bindings?.indicator_ids) ? bindings.indicator_ids : [],
  indicators: Array.isArray(bindings?.indicators) ? bindings.indicators : [],
})

export function normalizeStrategySummary(payload) {
  if (!payload || typeof payload !== 'object') {
    return null
  }

  const strategy = normalizeStrategyCore(payload.strategy)
  const bindings = normalizeStrategyBindings(payload.bindings)

  return {
    ...strategy,
    ...bindings,
    strategy,
    bindings,
  }
}

export function normalizeStrategyDetail(payload) {
  const summary = normalizeStrategySummary(payload)
  if (!summary) {
    return null
  }

  const decision = {
    rules: Array.isArray(payload?.decision?.rules) ? payload.decision.rules : [],
  }
  const read_context = {
    missing_indicators: Array.isArray(payload?.read_context?.missing_indicators)
      ? payload.read_context.missing_indicators
      : [],
    instrument_messages: Array.isArray(payload?.read_context?.instrument_messages)
      ? payload.read_context.instrument_messages
      : [],
  }
  const variants = Array.isArray(payload?.variants) ? payload.variants : []

  return {
    ...summary,
    rules: decision.rules,
    missing_indicators: read_context.missing_indicators,
    instrument_messages: read_context.instrument_messages,
    variants,
    decision,
    read_context,
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
  const payload = await handleResponse(res)
  const list = Array.isArray(payload) ? payload : []
  return list.map(normalizeStrategySummary).filter(Boolean)
}

/** Fetch a single strategy detail record. */
export async function fetchStrategy(strategyId) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}`, { mode: 'cors' })
  return normalizeStrategyDetail(await handleResponse(res))
}

/** Create a new strategy. */
export async function createStrategy(payload) {
  const res = await fetch(`${BASE}/api/strategies/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return normalizeStrategyDetail(await handleResponse(res))
}

/** Create a saved strategy variant. */
export async function createStrategyVariant(strategyId, payload) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/variants`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Update a saved strategy variant. */
export async function updateStrategyVariant(strategyId, variantId, payload) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/variants/${variantId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

/** Delete a saved non-default strategy variant. */
export async function deleteStrategyVariant(strategyId, variantId) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/variants/${variantId}`, {
    method: 'DELETE',
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
  return normalizeStrategyDetail(await handleResponse(res))
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
  return normalizeStrategyDetail(await handleResponse(res))
}

/** Detach an indicator instance from a strategy. */
export async function detachStrategyIndicator(strategyId, indicatorId) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/indicators/${indicatorId}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return normalizeStrategyDetail(await handleResponse(res))
}

/** Create a rule for a strategy. */
export async function createStrategyRule(strategyId, payload) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/rules`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return normalizeStrategyDetail(await handleResponse(res))
}

/** Update an existing strategy rule. */
export async function updateStrategyRule(strategyId, ruleId, payload) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/rules/${ruleId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return normalizeStrategyDetail(await handleResponse(res))
}

/** Delete a strategy rule. */
export async function deleteStrategyRule(strategyId, ruleId) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/rules/${ruleId}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return normalizeStrategyDetail(await handleResponse(res))
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
