import { createLogger } from '../utils/logger.js'

const BASE = import.meta.env.REACT_APP_API_BASE_URL || 'http://localhost:8000'
const logger = createLogger('StrategyAdapter')

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
    logger.warn('response_parse_failed', { status: res.status, url: res.url, contentType }, err)
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

/**
 * Fetch all saved strategy definitions.
 */
export async function fetchStrategies() {
  const res = await fetch(`${BASE}/api/strategies/`, { mode: 'cors' })
  return handleResponse(res)
}

/**
 * Persist a strategy definition to the backend.
 */
export async function saveStrategy(payload) {
  logger.info('save_strategy_request', { hasId: Boolean(payload?.strategy_id) })
  const res = await fetch(`${BASE}/api/strategies/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    mode: 'cors',
  })
  return handleResponse(res)
}

/**
 * Upload YAML metadata for a strategy.
 */
export async function uploadStrategyYaml(strategyId, yamlText) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/yaml`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ yaml_text: yamlText }),
    mode: 'cors',
  })
  return handleResponse(res)
}

/**
 * Request synthesized order signals for a strategy.
 */
export async function fetchStrategyOrderSignals(strategyId) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/order-signals`, { mode: 'cors' })
  return handleResponse(res)
}

/**
 * Trigger a placeholder backtest run for the strategy.
 */
export async function requestStrategyBacktest(strategyId, params) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/backtest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params || {}),
    mode: 'cors',
  })
  return handleResponse(res)
}

/**
 * Trigger a placeholder launch request for the strategy.
 */
export async function launchStrategy(strategyId, payload) {
  const res = await fetch(`${BASE}/api/strategies/${strategyId}/launch`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {}),
    mode: 'cors',
  })
  return handleResponse(res)
}

