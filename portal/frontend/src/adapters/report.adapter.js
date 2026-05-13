import { createLogger } from '../utils/logger.js'

import { API_ORIGIN as BASE } from '../config/appConfig.js'
const log = createLogger('ReportAdapter')
const REPORT_GET_TTL_MS = 15_000
const reportGetCache = new Map()
const reportGetInflight = new Map()

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

async function getReportJson(path, options = {}) {
  const url = `${BASE}${path}`
  const key = url
  if (options.force) {
    reportGetCache.delete(key)
  } else {
    const cached = reportGetCache.get(key)
    if (cached && Date.now() - cached.storedAt < REPORT_GET_TTL_MS) {
      return cached.payload
    }
  }

  const existing = reportGetInflight.get(key)
  if (existing) return existing

  const request = fetch(url, { mode: 'cors' })
    .then(handleResponse)
    .then((payload) => {
      reportGetCache.set(key, { storedAt: Date.now(), payload })
      return payload
    })
    .finally(() => {
      reportGetInflight.delete(key)
    })
  reportGetInflight.set(key, request)
  return request
}

export async function listReports(params = {}) {
  const query = buildQuery(params)
  const res = await fetch(`${BASE}/api/reports/${query}`, { mode: 'cors' })
  return handleResponse(res)
}

export async function getReport(runId, options = {}) {
  return getReportJson(`/api/reports/${runId}`, options)
}

export async function getReportReadiness(runId, options = {}) {
  return getReportJson(`/api/reports/${runId}/readiness`, options)
}

export async function getReportSummary(runId, options = {}) {
  return getReportJson(`/api/reports/${runId}/summary`, options)
}

export async function getReportSections(runId, options = {}) {
  return getReportJson(`/api/reports/${runId}/sections`, options)
}

export async function getTradeDataset(runId, params = {}, options = {}) {
  const query = buildQuery(params)
  return getReportJson(`/api/reports/${runId}/trades${query}`, options)
}

export async function getDecisionDataset(runId, params = {}, options = {}) {
  const query = buildQuery(params)
  return getReportJson(`/api/reports/${runId}/decisions${query}`, options)
}

export async function getSignalDataset(runId, params = {}, options = {}) {
  const query = buildQuery(params)
  return getReportJson(`/api/reports/${runId}/signals${query}`, options)
}

export async function getTimeseriesDataset(runId, section, params = {}, options = {}) {
  const query = buildQuery(params)
  return getReportJson(`/api/reports/${runId}/timeseries/${encodeURIComponent(section)}${query}`, options)
}

export async function getContextDataset(runId, params = {}, options = {}) {
  const query = buildQuery(params)
  return getReportJson(`/api/reports/${runId}/context${query}`, options)
}

export async function getCandleCatalog(runId, options = {}) {
  return getReportJson(`/api/reports/${runId}/candles/catalog`, options)
}

export async function getReportDiagnostics(runId, options = {}) {
  return getReportJson(`/api/reports/${runId}/diagnostics`, options)
}

export async function getReportMetrics(runId, options = {}) {
  return getReportJson(`/api/reports/${runId}/metrics`, options)
}

export async function getOperationalHealth(runId, options = {}) {
  return getReportJson(`/api/reports/${runId}/operational-health`, options)
}

export async function getExportManifest(runId, options = {}) {
  const { includeCandles, force } = options || {}
  const query = buildQuery({ include_candles: includeCandles })
  return getReportJson(`/api/reports/${runId}/export/manifest${query}`, { force })
}

export async function explainMetric(runId, metricName, options = {}) {
  return getReportJson(`/api/reports/${runId}/metrics/${encodeURIComponent(metricName)}/explanation`, options)
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
  const filename = match?.[1] || `run_${runId}_report_export.zip`
  return { blob, filename }
}
