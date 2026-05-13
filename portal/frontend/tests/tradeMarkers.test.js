import test from 'node:test'
import assert from 'node:assert/strict'

import { buildTradeMarkerArtifacts } from '../src/components/bots/hooks/useTradeMarkers.js'

const candles = [
  { time: 1_700_000_000, open: 100, high: 101, low: 99, close: 100 },
  { time: 1_700_000_060, open: 100, high: 106, low: 98, close: 104 },
  { time: 1_700_000_120, open: 104, high: 105, low: 103, close: 104 },
]

function candleLookup() {
  return new Map(candles.map((candle) => [candle.time, candle]))
}

function closedTrade(overrides = {}) {
  return {
    trade_id: 'trade-1',
    symbol: 'BTC',
    timeframe: '1m',
    status: 'closed',
    trade_state: 'closed',
    side: 'long',
    entry_time: '2023-11-14T22:13:20Z',
    entry_price: 100,
    exit_time: '2023-11-14T22:14:20Z',
    exit_price: 104,
    stop_price: 98,
    quantity: 1,
    gross_pnl: 4,
    fees_paid: 0.5,
    net_pnl: 3.5,
    close_reason: 'TARGET',
    legs: [
      {
        target_price: 104,
        exit_time: '2023-11-14T22:14:20Z',
        exit_price: 104,
        status: 'target',
        contracts: 1,
        pnl: 4,
      },
    ],
    ...overrides,
  }
}

function openTrade(overrides = {}) {
  return {
    ...closedTrade({
      status: 'open',
      trade_state: 'open',
      exit_time: null,
      exit_price: null,
      close_reason: null,
      net_pnl: null,
      legs: [
        {
          target_price: 106,
          status: 'open',
          contracts: 1,
        },
      ],
    }),
    ...overrides,
  }
}

test('closed trade artifacts keep markers and spans without persistent price lines', () => {
  const artifacts = buildTradeMarkerArtifacts([closedTrade()], candleLookup(), candles)

  assert.equal(artifacts.markers.some((marker) => marker.kind === 'entry'), true)
  assert.equal(artifacts.markers.some((marker) => marker.kind === 'target'), true)
  assert.equal(artifacts.regions.length, 1)
  assert.equal(artifacts.segments.length, 1)
  assert.deepEqual(artifacts.priceLines, [])
  assert.equal(artifacts.tooltips.some((tooltip) => tooltip.entries.some((line) => line === 'Fees: +0.50')), true)
  assert.equal(artifacts.tooltips.some((tooltip) => tooltip.entries.some((line) => line === 'Net PnL: +3.50')), true)
})

test('closed trade marker uses top-level exit price and close reason when legs are absent', () => {
  const trade = closedTrade({ legs: [] })
  const artifacts = buildTradeMarkerArtifacts([trade], candleLookup(), candles)

  const entry = artifacts.markers.find((marker) => marker.kind === 'entry')
  const exit = artifacts.markers.find((marker) => marker.kind === 'target')

  assert.equal(Boolean(entry), true)
  assert.equal(Boolean(exit), true)
  assert.equal(exit.price, 104)
  assert.equal(
    artifacts.tooltips.some((tooltip) => tooltip.kind === 'target' && tooltip.entries.includes('Close Reason: TARGET')),
    true,
  )
  assert.equal(trade.exit_price, 104)
})

test('open trade artifacts render active entry, stop, and target price lines', () => {
  const artifacts = buildTradeMarkerArtifacts([openTrade()], candleLookup(), candles)

  assert.equal(artifacts.markers.some((marker) => marker.kind === 'entry'), true)
  assert.equal(artifacts.markers.some((marker) => marker.kind === 'target'), false)
  assert.equal(artifacts.segments.length, 1)
  assert.equal(artifacts.segments[0].y1, 100)
  assert.equal(artifacts.segments[0].y2, 104)
  assert.equal(artifacts.regions.length, 1)
  assert.equal(artifacts.regions[0].y1, 98)
  assert.equal(artifacts.regions[0].y2, 106)
  assert.equal(
    [...artifacts.segments.flatMap((segment) => [segment.y1, segment.y2]), ...artifacts.regions.flatMap((region) => [region.y1, region.y2])].includes(0),
    false,
  )
  assert.equal(artifacts.priceLines.some((line) => line.source === 'active_trade_entry' && line.price === 100), true)
  assert.equal(artifacts.priceLines.some((line) => line.source === 'active_trade_sl' && line.price === 98), true)
  assert.equal(artifacts.priceLines.some((line) => line.source === 'active_trade_tp' && line.price === 106), true)
})

test('open trade artifacts ignore zero-valued missing exit and target prices', () => {
  const artifacts = buildTradeMarkerArtifacts([
    openTrade({
      exit_price: 0,
      legs: [
        {
          target_price: 0,
          status: 'open',
          contracts: 1,
        },
      ],
    }),
  ], candleLookup(), candles)

  assert.equal(artifacts.segments.length, 1)
  assert.equal(artifacts.segments[0].y2, 104)
  assert.equal(artifacts.regions.length, 1)
  assert.deepEqual([artifacts.regions[0].y1, artifacts.regions[0].y2], [98, 104])
  assert.equal(artifacts.priceLines.some((line) => line.source === 'active_trade_tp' && line.price === 0), false)
})

test('BACKTEST_END close renders as terminal exit marker with reason in tooltip', () => {
  const artifacts = buildTradeMarkerArtifacts([
    closedTrade({
      close_reason: 'BACKTEST_END',
      legs: [
        {
          target_price: 106,
          exit_time: '2023-11-14T22:14:20Z',
          exit_price: 104,
          status: 'backtest_end',
          contracts: 1,
          pnl: 4,
        },
      ],
    }),
  ], candleLookup(), candles)

  const terminal = artifacts.markers.find((marker) => marker.kind === 'backtest_end')
  assert.equal(Boolean(terminal), true)
  assert.equal(terminal.text.startsWith('END'), true)
  assert.equal(
    artifacts.tooltips.some((tooltip) => tooltip.kind === 'backtest_end' && tooltip.entries.includes('Close Reason: BACKTEST_END')),
    true,
  )
})
