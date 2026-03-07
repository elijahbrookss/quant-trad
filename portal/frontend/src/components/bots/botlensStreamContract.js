export function shouldForceResyncForSeqGap({ previousSeq, nextSeq, maxAllowedGap = 1 }) {
  const prev = Number(previousSeq || 0)
  const next = Number(nextSeq || 0)
  const allowed = Math.max(1, Number(maxAllowedGap || 1))
  if (!Number.isFinite(prev) || !Number.isFinite(next)) return false
  if (prev <= 0 || next <= 0) return false
  return next - prev > allowed
}
