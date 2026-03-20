import test from 'node:test'
import assert from 'node:assert/strict'

import { applyIndicatorColors } from '../src/components/indicatorSignals.js'

test('applyIndicatorColors preserves explicit polyline colors', () => {
  const overlays = [
    {
      ind_id: 'candle-stats',
      color: '#60a5fa',
      payload: {
        polylines: [
          { color: '#22c55e', points: [{ time: 1, price: 10 }] },
          { color: '#ef4444', points: [{ time: 1, price: 11 }] },
        ],
      },
    },
  ]

  const result = applyIndicatorColors(overlays, { 'candle-stats': '#60a5fa' })

  assert.equal(result[0].payload.polylines[0].color, '#22c55e')
  assert.equal(result[0].payload.polylines[1].color, '#ef4444')
})

test('applyIndicatorColors still tints polylines with no explicit color', () => {
  const overlays = [
    {
      ind_id: 'generic',
      color: '#60a5fa',
      payload: {
        polylines: [
          { points: [{ time: 1, price: 10 }] },
        ],
      },
    },
  ]

  const result = applyIndicatorColors(overlays, { generic: '#60a5fa' })

  assert.match(result[0].payload.polylines[0].color, /^rgba\(/)
})

