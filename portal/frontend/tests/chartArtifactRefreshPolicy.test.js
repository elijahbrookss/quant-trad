import assert from 'node:assert/strict'
import test from 'node:test'

import {
  DEFAULT_ARTIFACT_REFRESH_BARS,
  resolveChartArtifactRefreshKey,
} from '../src/components/bots/chartArtifactRefreshPolicy.js'

const hourlyCandles = (start, count) => Array.from({ length: count }, (_, index) => ({
  time: start + index * 3600,
  open: 1,
  high: 2,
  low: 0,
  close: 1,
}))

test('live chart artifacts refresh at a bounded bar cadence', () => {
  const start = 3600
  const first = hourlyCandles(start, 320)
  const withinBucket = hourlyCandles(start, 320 + DEFAULT_ARTIFACT_REFRESH_BARS - 1).slice(-320)
  const nextBucket = hourlyCandles(start, 320 + DEFAULT_ARTIFACT_REFRESH_BARS).slice(-320)

  const firstKey = resolveChartArtifactRefreshKey({ candles: first, timeframe: '1h' })
  assert.equal(
    resolveChartArtifactRefreshKey({ candles: withinBucket, timeframe: '1h' }),
    firstKey,
  )
  assert.notEqual(
    resolveChartArtifactRefreshKey({ candles: nextBucket, timeframe: '1h' }),
    firstKey,
  )
})

test('history updates force an artifact refresh inside the same live bucket', () => {
  const candles = hourlyCandles(1_700_000_000, 320)
  const before = resolveChartArtifactRefreshKey({
    candles,
    timeframe: '1h',
    dataUpdateToken: 'history-page-1',
  })
  const after = resolveChartArtifactRefreshKey({
    candles,
    timeframe: '1h',
    dataUpdateToken: 'history-page-2',
  })

  assert.notEqual(after, before)
})
