import test from 'node:test'
import assert from 'node:assert/strict'

import {
  applyHistoryPage,
  applyLiveTail,
  assessLiveContinuity,
  buildProjectionFromWindow,
  canonicalSeriesKey,
  mergeCanonicalCandles,
  normalizeSeriesKey,
} from '../src/components/bots/botlensProjection.js'

test('buildProjectionFromWindow preserves canonical series identity and projection fields', () => {
  const projection = buildProjectionFromWindow({
    runId: 'run-1',
    seq: 20,
    seriesKey: 'btc|1M',
    window: {
      projection: {
        series: [
          {
            symbol: 'btc',
            timeframe: '1M',
            candles: [
              { time: '2026-01-01T00:00:00Z', open: 1, high: 1, low: 1, close: 1 },
            ],
            overlays: [{ type: 'regime_overlay', payload: { state: 'risk_on' } }],
            stats: { total_trades: 2 },
          },
        ],
        trades: [{ trade_id: 't-1', symbol: 'BTC' }],
        runtime: { status: 'running' },
        warnings: ['warning'],
      },
    },
  })

  assert.equal(projection.run_id, 'run-1')
  assert.equal(projection.seq, 20)
  assert.equal(projection.series_key, 'BTC|1m')
  assert.equal(projection.series[0].series_key, 'BTC|1m')
  assert.equal(projection.series[0].overlays[0].overlay_id, 'type:regime_overlay')
  assert.equal(projection.trades[0].trade_id, 't-1')
})

test('mergeCanonicalCandles normalizes time identity and replaces duplicates', () => {
  const merged = mergeCanonicalCandles(
    [{ time: '2026-01-01T00:00:00Z', open: 1, high: 1, low: 1, close: 1 }],
    [{ time: 1767225600, open: 2, high: 2, low: 2, close: 2 }],
    [{ time: 1767225660, open: 3, high: 3, low: 3, close: 3 }],
  )

  assert.deepEqual(merged.map((row) => row.time), [1767225600, 1767225660])
  assert.equal(merged[0].close, 2)
})

test('history paging prepends overlapping candles without duplication', () => {
  const seeded = buildProjectionFromWindow({
    runId: 'run-1',
    seq: 5,
    seriesKey: 'BTC|1m',
    window: {
      projection: {
        series: [
          {
            symbol: 'BTC',
            timeframe: '1m',
            candles: [
              { time: 1767225600, open: 1, high: 1, low: 1, close: 1 },
              { time: 1767225660, open: 2, high: 2, low: 2, close: 2 },
            ],
          },
        ],
      },
    },
  })

  const next = applyHistoryPage({
    projection: seeded,
    seriesKey: canonicalSeriesKey('BTC', '1m'),
    candles: [
      { time: 1767225540, open: 0, high: 0, low: 0, close: 0 },
      { time: 1767225600, open: 1.5, high: 1.5, low: 1.5, close: 1.5 },
    ],
  })

  assert.deepEqual(next.series[0].candles.map((row) => row.time), [1767225540, 1767225600, 1767225660])
  assert.equal(next.series[0].candles[1].close, 1)
})

test('applyLiveTail materializes typed series_delta payloads incrementally', () => {
  const seeded = buildProjectionFromWindow({
    runId: 'run-1',
    seq: 10,
    seriesKey: 'BTC|1m',
    window: {
      projection: {
        series: [
          {
            symbol: 'BTC',
            timeframe: '1m',
            candles: [{ time: 1767225600, open: 1, high: 1, low: 1, close: 1 }],
            overlays: [],
            stats: { total_trades: 0 },
          },
        ],
        runtime: { status: 'running' },
        trades: [],
        logs: [],
        decisions: [],
      },
    },
  })

  const next = applyLiveTail({
    projection: seeded,
    seriesKey: 'BTC|1m',
    message: {
      runId: 'run-1',
      seriesKey: 'BTC|1m',
      seq: 11,
      messageType: 'series_delta',
      payload: {
        runtime: { status: 'running', warnings: ['slow consumer'] },
        logs: [{ message: 'delta log' }],
        decisions: [{ event: 'decision' }],
        seriesDelta: {
          symbol: 'BTC',
          timeframe: '1m',
          candle: { time: 1767225660, open: 2, high: 2, low: 2, close: 2 },
          overlay_delta: {
            ops: [
              {
                op: 'upsert',
                key: 'overlay:regime',
                overlay: { type: 'regime_overlay', payload: { state: 'risk_on' } },
              },
            ],
          },
          stats: { total_trades: 1 },
          trades: [{ trade_id: 't-1', symbol: 'BTC' }],
        },
      },
    },
  })

  assert.equal(next.seq, 11)
  assert.deepEqual(next.series[0].candles.map((row) => row.time), [1767225600, 1767225660])
  assert.equal(next.series[0].overlays[0].overlay_id, 'overlay:regime')
  assert.equal(next.series[0].stats.total_trades, 1)
  assert.equal(next.trades[0].trade_id, 't-1')
  assert.equal(next.logs[0].message, 'delta log')
  assert.equal(next.decisions[0].event, 'decision')
  assert.deepEqual(next.warnings, ['slow consumer'])
  assert.equal(next.runtime.last_bar.time, 1767225660)
})

test('live continuity check forces resync on sequence gaps', () => {
  const seeded = buildProjectionFromWindow({
    runId: 'run-1',
    seq: 10,
    seriesKey: 'BTC|1m',
    window: {
      projection: {
        series: [{ symbol: 'BTC', timeframe: '1m', candles: [] }],
      },
    },
  })

  const continuity = assessLiveContinuity({
    projection: seeded,
    message: { runId: 'run-1', seriesKey: 'BTC|1m', seq: 13 },
    seriesKey: 'BTC|1m',
  })

  assert.equal(continuity.action, 'resync')
  assert.equal(continuity.reason, 'seq_gap')
})

test('series identity helpers reject non-canonical legacy keys', () => {
  assert.equal(normalizeSeriesKey('bot'), '')
  assert.equal(normalizeSeriesKey('BOT|'), '')
  assert.equal(canonicalSeriesKey('BTC', ''), '')
  assert.equal(canonicalSeriesKey('btc', '1M'), 'BTC|1m')
})
