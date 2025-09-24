import { createLogger } from '../utils/logger.js'

const BASE = import.meta.env.REACT_APP_API_BASE_URL || 'http://localhost:8000'
const log = createLogger('HealthAdapter')

export async function pingApi({ timeoutMs = 5000 } = {}) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const res = await fetch(`${BASE}/api/health`, { mode: 'cors', signal: controller.signal })
    if (!res.ok) {
      const message = res.statusText || `Health check failed with status ${res.status}`
      throw new Error(message)
    }
    const data = await res.json().catch(() => ({}))
    log.debug('api_health_ok', { status: data?.status || res.status, timestamp: data?.timestamp })
    return data
  } catch (error) {
    log.warn('api_health_failed', { message: error?.message })
    throw error
  } finally {
    clearTimeout(timer)
  }
}
