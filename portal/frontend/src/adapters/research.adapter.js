import { API_ORIGIN } from '../config/appConfig.js'

const BASE = API_ORIGIN

function buildQuery(params = {}) {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined || value === '') return
    query.append(key, String(value))
  })
  const text = query.toString()
  return text ? `?${text}` : ''
}

async function request(path) {
  const response = await fetch(`${BASE}${path}`, {
    method: 'GET',
    headers: { 'Content-Type': 'application/json' },
    mode: 'cors',
  })
  if (!response.ok) {
    let detail = null
    try {
      const payload = await response.json()
      detail = payload?.detail || payload?.message || null
    } catch {
      detail = null
    }
    throw new Error(detail || response.statusText || 'Research read failed')
  }
  return response.status === 204 ? null : response.json()
}

export async function listResearchItems(params = {}) {
  const payload = await request(`/api/research/items${buildQuery(params)}`)
  return Array.isArray(payload?.items) ? payload.items : []
}

export async function fetchResearchActivity(params = {}) {
  return request(`/api/research/activity${buildQuery(params)}`)
}

export async function fetchResearchItem(itemId) {
  return request(`/api/research/items/${encodeURIComponent(itemId)}`)
}

export async function fetchResearchTrail(itemId) {
  return request(`/api/research/items/${encodeURIComponent(itemId)}/trail`)
}

export async function fetchRunResearchEvidence(runId) {
  return request(`/api/research/runs/${encodeURIComponent(runId)}/evidence`)
}
