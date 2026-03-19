function normalizeApiBase(baseUrl) {
  if (!baseUrl) return '/api'
  const trimmed = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl
  if (trimmed.startsWith('http')) {
    return trimmed.endsWith('/api') ? trimmed : `${trimmed}/api`
  }
  return trimmed
}

function resolveApiBase() {
  const configured = import.meta.env?.VITE_API_BASE_URL
  if (configured) return normalizeApiBase(configured)

  if (typeof window !== 'undefined') {
    const { protocol, hostname, port } = window.location
    if (port && Number(port) === 5173) {
      return `${protocol}//${hostname}:8000/api`
    }
  }

  return '/api'
}

function readPositiveNumber(name, fallback) {
  const raw = import.meta.env?.[name]
  if (raw === undefined || raw === null || raw === '') return fallback
  const parsed = Number(raw)
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback
  return parsed
}

function readPositiveInt(name, fallback) {
  return Math.max(1, Math.floor(readPositiveNumber(name, fallback)))
}

export const API_BASE_URL = normalizeApiBase(resolveApiBase())
export const API_ORIGIN = API_BASE_URL.endsWith('/api') ? API_BASE_URL.slice(0, -4) || '' : API_BASE_URL

export const BOTLENS_CONFIG = Object.freeze({
  debug: import.meta.env?.VITE_BOTLENS_DEBUG === 'true',
  autoFitOverlayExtents: String(import.meta.env?.VITE_BOTLENS_AUTO_FIT_OVERLAY_EXTENTS || '').trim().toLowerCase() === 'true',
  targetRenderLagMs: readPositiveNumber('VITE_BOTLENS_TARGET_RENDER_LAG_MS', 120),
  catchupRenderLagMs: readPositiveNumber('VITE_BOTLENS_CATCHUP_RENDER_LAG_MS', 1200),
  catchupSeqBehind: readPositiveInt('VITE_BOTLENS_CATCHUP_SEQ_BEHIND', 6),
  catchupQueueDepth: readPositiveInt('VITE_BOTLENS_CATCHUP_QUEUE_DEPTH', 8),
  normalApplyIntervalMs: readPositiveNumber('VITE_BOTLENS_NORMAL_APPLY_INTERVAL_MS', 33),
  catchupApplyIntervalMs: readPositiveNumber('VITE_BOTLENS_CATCHUP_APPLY_INTERVAL_MS', 12),
  maxCatchupBatch: readPositiveInt('VITE_BOTLENS_MAX_CATCHUP_BATCH', 2),
  metricsPublishMs: readPositiveNumber('VITE_BOTLENS_METRICS_PUBLISH_MS', 120),
  snapCandlesBehind: readPositiveInt('VITE_BOTLENS_SNAP_CANDLES_BEHIND', 30),
  ledgerPollMs: readPositiveInt('VITE_BOTLENS_LEDGER_POLL_MS', 800),
  ledgerPollLimit: readPositiveInt('VITE_BOTLENS_LEDGER_POLL_LIMIT', 500),
  ledgerMaxEvents: readPositiveInt('VITE_BOTLENS_LEDGER_MAX_EVENTS', 3000),
  liveResubscribeLimit: readPositiveInt('VITE_BOTLENS_LIVE_RESUBSCRIBE_LIMIT', 3),
  liveResubscribeWindowMs: readPositiveInt('VITE_BOTLENS_LIVE_RESUBSCRIBE_WINDOW_MS', 30000),
})

export { normalizeApiBase }
