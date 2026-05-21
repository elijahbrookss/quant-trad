import test from 'node:test'
import assert from 'node:assert/strict'

import { buildPolylineSeriesData } from '../src/chart/paneViews/polylinePaneView.js'

test('buildPolylineSeriesData computes autoscale bounds across multiple lines', () => {
  const rows = buildPolylineSeriesData([
    {
      points: [
        { time: 1, price: 100 },
        { time: 2, price: 110 },
      ],
    },
    {
      points: [
        { time: 1, price: 90 },
        { time: 2, price: 120 },
      ],
    },
  ])

  assert.deepEqual(rows, [
    { time: 1, low: 90, high: 100, value: 90, originalData: {} },
    { time: 2, low: 110, high: 120, value: 120, originalData: {} },
  ])
})

