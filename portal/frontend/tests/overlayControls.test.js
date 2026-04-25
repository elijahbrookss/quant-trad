import test from 'node:test'
import assert from 'node:assert/strict'

import { resolveOverlayGroup } from '../src/components/bots/hooks/useOverlayControls.js'

test('resolveOverlayGroup splits chart overlays into scan-friendly buckets', () => {
  assert.equal(resolveOverlayGroup({ type: 'market_profile' }), 'market')
  assert.equal(resolveOverlayGroup({ type: 'regime_overlay' }), 'regime')
  assert.equal(resolveOverlayGroup({ type: 'regime_markers' }), 'regime')
  assert.equal(resolveOverlayGroup({ type: 'session_context' }), 'indicator')
  assert.equal(resolveOverlayGroup({ ui: { group: 'regime' }, type: 'custom_overlay' }), 'regime')
  assert.equal(resolveOverlayGroup({ group: 'context', type: 'custom_overlay' }), 'indicator')
})

test('resolveOverlayGroup still keeps trade overlays separate', () => {
  assert.equal(resolveOverlayGroup({ type: 'bot_trade_rays' }), 'trade')
  assert.equal(resolveOverlayGroup({ ui: { group: 'trade' }, type: 'custom_overlay' }), 'trade')
})
