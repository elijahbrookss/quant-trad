import { createLogger } from '../utils/logger.js'
import { API_ORIGIN } from '../config/appConfig.js'
import { openSse } from './realtime.adapter.js'

const BASE = API_ORIGIN
const log = createLogger('MarketDataAdapter')

// Collector reads and mutations are confined to the canonical operational
// contract. The UI cannot define collectors, register schemas, mutate provider
// credentials, or invoke unrestricted acquisition.

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    method: 'GET',
    mode: 'cors',
    ...options,
  })
  if (!res.ok) {
    let detail = null
    const contentType = res.headers.get('content-type') || ''
    try {
      if (contentType.includes('application/json')) {
        const payload = await res.json()
        detail = payload?.detail || payload?.message || null
      } else {
        detail = await res.text()
      }
    } catch (err) {
      log.warn('market_data_request_parse_failed', { path, status: res.status }, err)
    }
    const message = detail || res.statusText || 'Market data request failed'
    throw new Error(message)
  }
  if (res.status === 204) return null
  return res.json()
}

function buildQuery(params = {}) {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined || value === '') return
    query.append(key, value)
  })
  const text = query.toString()
  return text ? `?${text}` : ''
}

function collectorPath(collectorKind, collectorId, suffix = '') {
  if (!collectorKind || !collectorId) throw new Error('Collector kind and ID are required')
  return '/api/market-data/operations/collectors/'
    + encodeURIComponent(collectorKind)
    + '/'
    + encodeURIComponent(collectorId)
    + suffix
}

export async function fetchCollectorOperationsSnapshot({ attemptLimit = 5 } = {}) {
  return request('/api/market-data/operations/collectors/snapshot' + buildQuery({ attempt_limit: attemptLimit }))
}

export function openCollectorOperationsStream({ attemptLimit = 5 } = {}) {
  return openSse(
    '/api/market-data/operations/collectors/stream' + buildQuery({ attempt_limit: attemptLimit }),
    { withCredentials: false, base: BASE },
  )
}

export async function fetchCollectorOperationsDetail(collectorKind, collectorId, { limit = 100 } = {}) {
  return request(collectorPath(collectorKind, collectorId) + buildQuery({ limit }))
}

export async function fetchCollectorDiagnostics(collectorKind, collectorId) {
  return request(collectorPath(collectorKind, collectorId, '/diagnostics'))
}

export async function fetchCollectorEvents(collectorKind, collectorId, { limit = 100 } = {}) {
  return request(collectorPath(collectorKind, collectorId, '/events') + buildQuery({ limit }))
}

export async function fetchCollectorGaps(collectorKind, collectorId, { limit = 100 } = {}) {
  return request(collectorPath(collectorKind, collectorId, '/gaps') + buildQuery({ limit }))
}

export async function fetchMarketDataPlaneSnapshot() {
  return request('/api/market-data/operations/data-plane')
}

export async function executeCollectorAction(
  collectorKind,
  collectorId,
  action,
  { actorId = 'frontend-v2:local-operator', reason = '', confirmation = null } = {},
) {
  const requestId = globalThis.crypto?.randomUUID?.()
    || `frontend-v2-${Date.now()}-${Math.random().toString(16).slice(2)}`
  return request(collectorPath(collectorKind, collectorId, '/actions/' + encodeURIComponent(action)), {
    method: 'POST',
    body: JSON.stringify({
      request_id: requestId,
      actor_id: actorId,
      requested_at: new Date().toISOString(),
      confirmation,
      context: { surface: 'frontend_v2', reason: String(reason || '').trim() || null },
    }),
  })
}

export async function listInstruments() {
  const payload = await request('/api/instruments/')
  return Array.isArray(payload) ? payload : []
}

// required=false always: the backend's default (required=true) raises a 400
// on stale/missing data, which would take down the whole lens. v2 renders
// "unavailable" instead, so it always asks for the graceful response shape.
export async function fetchLatestOpenInterest({ instrumentId, decisionTime, maxStalenessSeconds }) {
  return request(
    `/api/market-data/open-interest/latest${buildQuery({
      instrument_id: instrumentId,
      decision_time: decisionTime,
      max_staleness_seconds: maxStalenessSeconds,
      required: false,
    })}`,
  )
}

export async function fetchLatestFundingRate({ instrumentId, decisionTime, maxStalenessSeconds }) {
  return request(
    `/api/market-data/funding-rate/latest${buildQuery({
      instrument_id: instrumentId,
      decision_time: decisionTime,
      max_staleness_seconds: maxStalenessSeconds,
      required: false,
    })}`,
  )
}
