import { CameraIntents } from './hooks/useViewportController.js'

export const resolveCandleUpdateCameraIntent = ({ previous = [], next = [] } = {}) => {
  const prevLast = previous[previous.length - 1]
  const nextLast = next[next.length - 1]
  const prevLastTime = prevLast?.time
  const nextLastTime = nextLast?.time
  const historyRewound =
    Number.isFinite(prevLastTime) && Number.isFinite(nextLastTime) && (next.length < previous.length || nextLastTime < prevLastTime)
  const longJump = previous.length > 0 && next.length > previous.length + 1
  if (!next.length) return null
  if (!previous.length) return { intent: CameraIntents.FOLLOW_LATEST, reason: 'initial-load' }
  if (historyRewound || longJump) return { intent: CameraIntents.FOLLOW_LATEST, reason: 'series-reset' }
  return null
}
