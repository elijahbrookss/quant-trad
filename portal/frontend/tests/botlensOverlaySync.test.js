import test from 'node:test'
import assert from 'node:assert/strict'

import {
  isLegacyTradeOverlay,
  suppressLegacyTradeOverlays,
} from '../src/components/bots/botlensOverlayFilters.js'

test('legacy bot trade ray overlays are suppressed before BotLens chart projection', () => {
  const overlays = [
    {
      type: 'bot_trade_rays',
      payload: {
        segments: [
          { x1: 1_700_000_000, x2: 1_700_000_060, y1: 90_000, y2: 0 },
        ],
      },
    },
    { type: 'regime_overlay', payload: { boxes: [{ x1: 1, x2: 2, y1: 10, y2: 20 }] } },
    { type: 'market_profile', payload: { price_lines: [{ price: 91_000 }] } },
  ]

  assert.equal(isLegacyTradeOverlay(overlays[0]), true)
  assert.deepEqual(
    suppressLegacyTradeOverlays(overlays).map((overlay) => overlay.type),
    ['regime_overlay', 'market_profile'],
  )
})

test('legacy trade overlay suppression tolerates missing or non-array payloads', () => {
  assert.deepEqual(suppressLegacyTradeOverlays(null), [])
  assert.deepEqual(suppressLegacyTradeOverlays(undefined), [])
  assert.deepEqual(suppressLegacyTradeOverlays({ type: 'bot_trade_rays' }), [])
})
