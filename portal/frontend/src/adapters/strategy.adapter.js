import { createLogger } from '../utils/logger.js'

import { API_ORIGIN as BASE } from '../config/appConfig.js'
const adapterLogger = createLogger('StrategyAdapter')

export const STRATEGY_AUTHORING_DISABLED_MESSAGE =
  'Strategy authoring is CLI-owned while the frontend strategy workspace is dormant.'

export class StrategyAuthoringDisabledError extends Error {
  constructor() {
    super(STRATEGY_AUTHORING_DISABLED_MESSAGE)
    this.name = 'StrategyAuthoringDisabledError'
    this.code = 'strategy_authoring_disabled'
  }
}

const authoringDisabled = () => {
  throw new StrategyAuthoringDisabledError()
}

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

const uniqueStrings = (values = []) => {
  if (!Array.isArray(values)) return []
  const seen = new Set()
  const result = []
  for (const value of values) {
    const text = String(value || '').trim()
    if (!text) continue
    const key = text.toUpperCase()
    if (seen.has(key)) continue
    seen.add(key)
    result.push(text)
  }
  return result
}

const normalizeStrategyBindings = (bindings = {}) => ({
  symbols: uniqueStrings(bindings?.symbols),
  instrument_slots: Array.isArray(bindings?.instrument_slots) ? bindings.instrument_slots : [],
  instruments: Array.isArray(bindings?.instruments) ? bindings.instruments : [],
  indicator_ids: Array.isArray(bindings?.indicator_ids) ? bindings.indicator_ids : [],
  indicators: Array.isArray(bindings?.indicators) ? bindings.indicators : [],
})

export function normalizeStrategySummary(payload) {
  if (!payload || typeof payload !== 'object') {
    return null
  }

  if (payload.schema_version && payload.schema_version !== 'strategy_inventory_item.v1') {
    return null
  }

  const strategy = normalizeStrategyCore(payload)
  const symbols = uniqueStrings(payload.symbols)
  const counts = {
    instrument_count: Number(payload?.instrument_count || 0),
    indicator_count: Number(payload?.indicator_count || 0),
    rule_count: Number(payload?.rule_count || 0),
    variant_count: Number(payload?.variant_count || 0),
  }

  return {
    ...strategy,
    ...counts,
    symbols,
    readiness:
      payload?.readiness && typeof payload.readiness === 'object'
        ? { ...payload.readiness }
        : {},
    strategy,
    counts,
  }
}

export function normalizeStrategyDetail(definition, sections = {}) {
  if (
    !definition
    || typeof definition !== 'object'
    || definition.schema_version !== 'strategy_definition.v1'
  ) {
    return null
  }

  const strategy = normalizeStrategyCore(definition.strategy)
  const read_context = {
    missing_indicators: Array.isArray(definition?.read_context?.missing_indicators)
      ? definition.read_context.missing_indicators
      : [],
    instrument_messages: Array.isArray(definition?.read_context?.instrument_messages)
      ? definition.read_context.instrument_messages
      : [],
  }
  const bindingsDoc = sections?.bindings
  const rulesDoc = sections?.rules
  const variantsDoc = sections?.variants
  const bindings = normalizeStrategyBindings(bindingsDoc?.bindings)
  const rules = Array.isArray(rulesDoc?.rules) ? rulesDoc.rules : []
  const variants = Array.isArray(variantsDoc?.variants) ? variantsDoc.variants : []
  const counts = {
    ...(definition?.counts && typeof definition.counts === 'object' ? definition.counts : {}),
    instrument_count: bindings.instruments.length,
    indicator_count: bindings.indicators.length,
    rule_count: rules.length,
    variant_count: variants.length,
  }
  const decision = { rules }

  return {
    ...strategy,
    ...bindings,
    counts,
    rules,
    missing_indicators: read_context.missing_indicators,
    instrument_messages: read_context.instrument_messages,
    variants,
    strategy,
    bindings,
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

function ensureContract(payload, schemaVersion, label) {
  if (!payload || typeof payload !== 'object' || payload.schema_version !== schemaVersion) {
    throw new Error(`${label} returned an unexpected contract`)
  }
  return payload
}

/** Fetch all strategy records. */
export async function fetchStrategies() {
  const res = await fetch(`${BASE}/api/strategies/`, { mode: 'cors' })
  const payload = ensureContract(await handleResponse(res), 'strategy_inventory.v1', 'Strategy inventory')
  const list = Array.isArray(payload.items) ? payload.items : []
  return list.map(normalizeStrategySummary).filter(Boolean)
}

/** Fetch a single strategy detail record. */
export async function fetchStrategy(strategyId) {
  const [definitionRes, bindingsRes, rulesRes, variantsRes] = await Promise.all([
    fetch(`${BASE}/api/strategies/${strategyId}`, { mode: 'cors' }),
    fetch(`${BASE}/api/strategies/${strategyId}/bindings`, { mode: 'cors' }),
    fetch(`${BASE}/api/strategies/${strategyId}/rules`, { mode: 'cors' }),
    fetch(`${BASE}/api/strategies/${strategyId}/variants`, { mode: 'cors' }),
  ])
  const definition = ensureContract(
    await handleResponse(definitionRes),
    'strategy_definition.v1',
    'Strategy definition',
  )
  const bindings = ensureContract(
    await handleResponse(bindingsRes),
    'strategy_bindings.v1',
    'Strategy bindings',
  )
  const rules = ensureContract(await handleResponse(rulesRes), 'strategy_rules.v1', 'Strategy rules')
  const variants = ensureContract(
    await handleResponse(variantsRes),
    'strategy_variants.v1',
    'Strategy variants',
  )
  return normalizeStrategyDetail(definition, { bindings, rules, variants })
}

/** Create a new strategy. */
export async function createStrategy() {
  return authoringDisabled()
}

/** Create a saved strategy variant. */
export async function createStrategyVariant() {
  return authoringDisabled()
}

/** Update a saved strategy variant. */
export async function updateStrategyVariant() {
  return authoringDisabled()
}

/** Delete a saved non-default strategy variant. */
export async function deleteStrategyVariant() {
  return authoringDisabled()
}

/** Update strategy metadata. */
export async function updateStrategy() {
  return authoringDisabled()
}

/** Delete a strategy. */
export async function deleteStrategy() {
  return authoringDisabled()
}

/** Attach an indicator instance to a strategy. */
export async function attachStrategyIndicator() {
  return authoringDisabled()
}

/** Detach an indicator instance from a strategy. */
export async function detachStrategyIndicator() {
  return authoringDisabled()
}

/** Create a rule for a strategy. */
export async function createStrategyRule() {
  return authoringDisabled()
}

/** Update an existing strategy rule. */
export async function updateStrategyRule() {
  return authoringDisabled()
}

/** Delete a strategy rule. */
export async function deleteStrategyRule() {
  return authoringDisabled()
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
