import { createLogger } from '../utils/logger.js'
import { API_ORIGIN } from '../config/appConfig.js'

const BASE = API_ORIGIN
const log = createLogger('MarketDataAdapter')

// Deliberately read-only: this adapter wraps GET collector, market-structure,
// normalization-spec, and latest-fact projections. Mutation endpoints are
// intentionally absent; v2 cannot operate collectors or materialize data.

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    method: 'GET',
    headers: { 'Content-Type': 'application/json' },
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

export async function listCollectorDefinitions({ definitionId } = {}) {
  const payload = await request(`/api/market-data/collectors${buildQuery({ definition_id: definitionId })}`)
  return Array.isArray(payload?.definitions) ? payload.definitions : []
}

export async function fetchCollectorAttempts(definitionId, { limit } = {}) {
  if (!definitionId) return []
  const payload = await request(
    `/api/market-data/collectors/${encodeURIComponent(definitionId)}/attempts${buildQuery({ limit })}`,
  )
  return Array.isArray(payload?.attempts) ? payload.attempts : []
}

export async function listMarketStructureDefinitions({ definitionId } = {}) {
  const payload = await request(
    `/api/market-data/market-structure/definitions${buildQuery({
      definition_id: definitionId,
    })}`,
  )
  return Array.isArray(payload?.definitions) ? payload.definitions : []
}

export async function listMarketStructureSessions({ definitionId, limit = 100 } = {}) {
  const payload = await request(
    `/api/market-data/market-structure/sessions${buildQuery({
      definition_id: definitionId,
      limit,
    })}`,
  )
  return Array.isArray(payload?.sessions) ? payload.sessions : []
}

export async function fetchMarketStructureStatus(definitionId) {
  if (!definitionId) return null
  return request(
    `/api/market-data/market-structure/definitions/${encodeURIComponent(definitionId)}/status`,
  )
}

export async function listMarketNormalizationSpecs() {
  const payload = await request(
    '/api/market-data/market-structure/normalization/specs',
  )
  return Array.isArray(payload?.specs) ? payload.specs : []
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
