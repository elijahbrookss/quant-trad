import { createLogger } from '../utils/logger.js'

import { API_ORIGIN as BASE } from '../config/appConfig.js'
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

export async function exportReport(runId, options = {}) {
  const res = await fetch(`${BASE}/api/reports/${runId}/export`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(options || {}),
    mode: 'cors',
  })
  if (!res.ok) {
    await handleResponse(res)
  }

  const blob = await res.blob()
  const disposition = res.headers.get('content-disposition') || ''
  const match = disposition.match(/filename="?([^";]+)"?/i)
  const filename = match?.[1] || `run_${runId}_llm_export.zip`
  return { blob, filename }
}
