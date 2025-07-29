const BASE = import.meta.env.REACT_APP_API_BASE_URL || 'http://localhost:8000'

async function handleResponse(res) {
  if (!res.ok) {
    const txt = await res.text()
    throw new Error(txt || res.statusText)
  }
  return res.status === 204 ? null : res.json()
}

export async function fetchIndicators() {
  const res = await fetch(`${BASE}/api/indicators/`, { mode: 'cors' })
  return handleResponse(res)
}

export async function fetchIndicatorTypes() {
  const res = await fetch(`${BASE}/api/indicators-types/`, { mode: 'cors' })
  return handleResponse(res)
}

export async function createIndicator({ type, name, params }) {
  const res = await fetch(`${BASE}/api/indicators/`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type, name, params }),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function updateIndicator(id, { type, name, params }) {
  const res = await fetch(`${BASE}/api/indicators/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type, name, params }),
    mode: 'cors',
  })
  return handleResponse(res)
}

export async function deleteIndicator(id) {
  const res = await fetch(`${BASE}/api/indicators/${id}`, {
    method: 'DELETE',
    mode: 'cors',
  })
  return handleResponse(res)
}
