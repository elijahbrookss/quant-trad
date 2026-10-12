import { API_BASE_URL } from '../config/appConfig.js'

async function storageRequest(path = '', { signal, body } = {}) {
  const response = await fetch(`${API_BASE_URL}/storage${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
  })
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    const detail = typeof payload?.detail === 'string' ? payload.detail : 'Storage request failed'
    throw new Error(`${detail} (${response.status})`)
  }
  if (!payload || typeof payload !== 'object') throw new Error('Storage response unavailable')
  return payload
}

export const readStorage = (signal) => storageRequest('', { signal })
export const registerStorageTarget = (targetId) => storageRequest('/targets', { body: { target_id: targetId } })
export const reviewStorageChange = (policy, revision, requestId) => storageRequest('/plans', {
  body: { policy, base_revision: revision, request_id: requestId },
})
export const applyStorageChange = (plan) => storageRequest(`/plans/${encodeURIComponent(plan.id)}/apply`, {
  body: { policy_hash: plan.policy_hash },
})
