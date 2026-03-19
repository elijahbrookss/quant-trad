export function consumeRetryBudget(history, nowMs, { limit = 3, windowMs = 30000 } = {}) {
  const boundedLimit = Math.max(1, Number(limit) || 3)
  const boundedWindowMs = Math.max(1, Number(windowMs) || 30000)
  const currentMs = Number(nowMs) || 0
  const cutoff = currentMs - boundedWindowMs
  const nextHistory = (Array.isArray(history) ? history : [])
    .map((entry) => Number(entry))
    .filter((entry) => Number.isFinite(entry) && entry >= cutoff)

  nextHistory.push(currentMs)

  return {
    history: nextHistory,
    attemptCount: nextHistory.length,
    blocked: nextHistory.length > boundedLimit,
    limit: boundedLimit,
    windowMs: boundedWindowMs,
  }
}
