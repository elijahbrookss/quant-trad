import test from 'node:test'
import assert from 'node:assert/strict'

import { getBotPerformanceTrace } from '../src/components/bots/botPerformanceTrace.js'

function buildBot(overrides = {}) {
  return {
    id: 'bot-trace-1',
    runtime: {
      stats: {},
    },
    last_run_artifact: {},
    ...overrides,
  }
}

test('uses runtime wallet balance trace when the fleet payload exposes it', () => {
  const trace = getBotPerformanceTrace(
    buildBot({
      runtime: {
        stats: {
          quote_currency: 'USD',
          balance_trace: [
            { timestamp: '2026-04-06T12:00:00Z', balance: 1000 },
            { timestamp: '2026-04-06T12:02:00Z', balance: 1012.5 },
            { timestamp: '2026-04-06T12:04:00Z', balance: 1008.25 },
          ],
        },
      },
    }),
    { statusKey: 'running' },
  )

  assert.equal(trace.kind, 'series')
  assert.equal(trace.source, 'wallet')
  assert.equal(trace.label, 'Wallet')
  assert.equal(trace.quoteCurrency, 'USD')
  assert.equal(trace.points.length, 3)
  assert.equal(trace.latestValue, 1008.25)
})

test('falls back to run artifact equity curve when runtime trace is absent', () => {
  const trace = getBotPerformanceTrace(
    buildBot({
      last_run_artifact: {
        charts: {
          equity_curve: [
            { time: '2026-04-06T12:00:00Z', value: 5000 },
            { time: '2026-04-06T12:03:00Z', value: 5088 },
          ],
        },
      },
    }),
    { statusKey: 'completed' },
  )

  assert.equal(trace.kind, 'series')
  assert.equal(trace.source, 'equity')
  assert.equal(trace.label, 'Equity')
  assert.equal(trace.latestValue, 5088)
})

test('returns an honest placeholder when no performance series is exposed', () => {
  const trace = getBotPerformanceTrace(buildBot(), { statusKey: 'starting' })

  assert.equal(trace.kind, 'placeholder')
  assert.equal(trace.label, 'Trace pending')
  assert.deepEqual(trace.points, [])
  assert.equal(trace.latestValue, null)
})
