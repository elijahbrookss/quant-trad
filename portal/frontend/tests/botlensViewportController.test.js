import test from 'node:test'
import assert from 'node:assert/strict'

import {
  computeFollowRange,
  isLogicalRangePinnedToLatest,
} from '../src/components/bots/hooks/useViewportController.js'

function makeCandles(count = 200, start = 1_700_000_000, spacing = 60) {
  return Array.from({ length: count }, (_, index) => {
    const time = start + index * spacing
    return {
      time,
      open: 100 + index,
      high: 101 + index,
      low: 99 + index,
      close: 100 + index,
    }
  })
}

test('follow range preserves the tracked span while anchoring the latest candle at the front edge', () => {
  const candles = makeCandles()
  const follow = computeFollowRange(candles, 60, { lookbackBars: 72, forwardPadBars: 1.25 })

  assert.equal(Number(follow.logicalRange.from.toFixed(2)), 127)
  assert.equal(Number(follow.logicalRange.to.toFixed(2)), 200.25)
  assert.equal(follow.range.to, candles[candles.length - 1].time + 75)
  assert.equal(follow.range.from, follow.range.to - (60 * 73.25))
})

test('live-edge detection stays engaged when the user zooms while remaining at the front edge', () => {
  const candles = makeCandles()
  const liveLogicalRange = computeFollowRange(candles, 60).logicalRange

  assert.equal(
    isLogicalRangePinnedToLatest({ from: 82, to: 198.5 }, liveLogicalRange),
    true,
  )
})

test('live-edge detection disengages once the viewport is panned away from the front edge', () => {
  const candles = makeCandles()
  const liveLogicalRange = computeFollowRange(candles, 60).logicalRange

  assert.equal(
    isLogicalRangePinnedToLatest({ from: 40, to: 189.5 }, liveLogicalRange),
    false,
  )
})
