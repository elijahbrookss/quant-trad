import { API_ORIGIN } from '../config/appConfig.js'
import { createResearchReadCache } from './researchReadCache.js'

const BASE = API_ORIGIN
const reads = createResearchReadCache()
function buildQuery(params = {}) {
  const query = new URLSearchParams()
  Object.entries(params).sort(([a], [b]) => a.localeCompare(b)).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== '') query.set(key, String(value))
  })
  return query.size ? `?${query}` : ''
}
async function request(path, { signal, refresh = false, cache = false, ...options } = {}) {
  return reads.read(path, async physicalSignal => {
    const response = await fetch(`${BASE}${path}`, { method: 'GET', mode: 'cors', ...options, signal: physicalSignal })
    if (!response.ok) {
      let detail
      try { const body = await response.json(); detail = body?.detail || body?.message } catch { detail = null }
      throw new Error(typeof detail === 'string' ? detail : response.statusText || 'Research read failed')
    }
    return response.status === 204 ? null : response.json()
  }, { signal, refresh, cache })
}
const itemPath = id => `/api/research/items/${encodeURIComponent(id)}`
export async function listResearchItems(params = {}, options = {}) {
  const payload = await request(`/api/research/items${buildQuery(params)}`, options)
  return Array.isArray(payload?.items) ? payload.items : []
}
export function fetchResearchActivity(params = {}, options = {}) {
  return request(`/api/research/activity${buildQuery(params)}`, { cache: true, ...options })
}
export function fetchOverviewActivity(type, options = {}) {
  return type === 'backtests_completed'
    ? request('/api/reports/activity?days=182&type=backtest', { cache: true, ...options })
    : fetchResearchActivity({ type, days: 182 }, options)
}
export function fetchResearchItem(itemId, options = {}) { return request(itemPath(itemId), options) }
export function fetchResearchRecord(itemId, options = {}) { return request(`${itemPath(itemId)}/record`, options) }
export function fetchResearchTrail(itemId, options = {}) { return request(`${itemPath(itemId)}/trail`, options) }
export function fetchResearchInventory(params = {}, options = {}) {
  return request(`/api/research/browse${buildQuery(params)}`, { cache: true, ...options })
}
export function fetchResearchOverview(options = {}) { return request('/api/research/overview', { cache: true, ...options }) }
export function fetchResearchSummary(itemId, params = {}, options = {}) {
  return request(`${itemPath(itemId)}/summary${buildQuery(params)}`, { cache: true, ...options })
}
export function fetchResearchRelationships(itemId, options = {}) {
  return request(`${itemPath(itemId)}/relationships`, { cache: true, ...options })
}
export async function fetchResearchImages(itemId, options = {}) {
  const payload = await request(`${itemPath(itemId)}/images`, { cache: true, ...options })
  if (!Array.isArray(payload?.items)) throw new Error('Research image response invalid')
  return payload
}
export function researchImageContentUrl(itemId, imageId) {
  return `${BASE}${itemPath(itemId)}/images/${encodeURIComponent(imageId)}/content`
}
export function researchRecordUrl(itemId) { return `${BASE}${itemPath(itemId)}/record` }
export function fetchStudyReports(itemId, options = {}) { return request(`${itemPath(itemId)}/reports`, options) }
export function studyReportUrl(itemId, reportId, source = false) {
  return `${BASE}${itemPath(itemId)}/reports/${encodeURIComponent(reportId)}/${source ? 'source' : 'content'}`
}
export function fetchResearchExportPreflight(itemId, params = {}, options = {}) {
  return request(`${itemPath(itemId)}/export/preflight${buildQuery(params)}`, options)
}
export function researchExportUrl(itemId, params = {}) { return `${BASE}${itemPath(itemId)}/export/content${buildQuery(params)}` }
export async function downloadResearchExport(itemId, params, { signal } = {}) {
  const response = await fetch(researchExportUrl(itemId, params), { method: 'GET', signal, mode: 'cors' })
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    throw new Error(body?.detail || 'Evidence export unavailable')
  }
  const blob = await response.blob()
  if (signal?.aborted) throw new DOMException('Export download cancelled', 'AbortError')
  return blob
}
export function fetchRunResearchEvidence(runId, options = {}) {
  return request(`/api/research/runs/${encodeURIComponent(runId)}/evidence`, options)
}

export function fetchResearchComparison(left, right, options = {}) {
  return request(`/api/research/browse/compare${buildQuery({ left, right })}`, options)
}

export function fetchActivityRange(type, range, options = {}) {
  return request(`/api/research/activity/range${buildQuery({ type, ...range })}`, { cache: true, ...options })
}
