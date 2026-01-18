import { createLogger } from '../utils/logger.js'

const BASE = import.meta.env.REACT_APP_API_BASE_URL || 'http://localhost:8000'
const log = createLogger('ReportAdapter')

const buildQuery = (params = {}) => {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined || value === '') return
    query.append(key, value)
  })
  const text = query.toString()
  return text ? `?${text}` : ''
}

async function handleResponse(res) {
  if (res.ok) {
    return res.status === 204 ? null : res.json()
  }

  let payload = null
  const contentType = res.headers.get('content-type') || ''
  try {
    payload = contentType.includes('application/json') ? await res.json() : await res.text()
  } catch (err) {
    log.warn('report_response_parse_failed', { status: res.status, url: res.url }, err)
  }

  const detail =
    (payload && typeof payload === 'object' && (payload.detail || payload.message)) ||
    (typeof payload === 'string' ? payload : null)
  const error = new Error(detail || res.statusText || `Request failed with status ${res.status}`)
  error.status = res.status
  if (payload && typeof payload === 'object') {
    error.payload = payload
  }
  throw error
}

export async function listReports(params = {}) {
  const query = buildQuery(params)
  const res = await fetch(`${BASE}/api/reports${query}`, { mode: 'cors' })
  return handleResponse(res)
}

export async function getReport(runId) {
  const res = await fetch(`${BASE}/api/reports/${runId}`, { mode: 'cors' })
  return handleResponse(res)
}

export async function compareReports(runIds = []) {
  const res = await fetch(`${BASE}/api/reports/compare`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ run_ids: runIds }),
    mode: 'cors',
  })
  return handleResponse(res)
}
