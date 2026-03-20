import test from 'node:test'
import assert from 'node:assert/strict'

import {
  replaceIndicatorOverlaySlice,
  retainActiveIndicatorOverlays,
} from '../src/components/indicatorOverlaySlices.js'

test('retainActiveIndicatorOverlays keeps active indicator slices and non-indicator overlays', () => {
  const overlays = [
    { ind_id: 'a', overlay_id: 'a.1' },
    { ind_id: 'b', overlay_id: 'b.1' },
    { overlay_id: 'system.1', source: 'system' },
  ]

  const result = retainActiveIndicatorOverlays(overlays, new Set(['a']))

  assert.deepEqual(result, [
    { ind_id: 'a', overlay_id: 'a.1' },
    { overlay_id: 'system.1', source: 'system' },
  ])
})

test('replaceIndicatorOverlaySlice replaces one indicator slice and preserves others', () => {
  const overlays = [
    { ind_id: 'a', overlay_id: 'a.old' },
    { ind_id: 'b', overlay_id: 'b.old' },
    { overlay_id: 'system.1', source: 'system' },
  ]

  const result = replaceIndicatorOverlaySlice(overlays, {
    indicatorId: 'a',
    nextSlice: [{ ind_id: 'a', overlay_id: 'a.new' }],
    activeIndicatorIds: new Set(['a', 'b']),
  })

  assert.deepEqual(result, [
    { ind_id: 'b', overlay_id: 'b.old' },
    { overlay_id: 'system.1', source: 'system' },
    { ind_id: 'a', overlay_id: 'a.new' },
  ])
})

test('replaceIndicatorOverlaySlice removes stale overlays for a skipped indicator', () => {
  const overlays = [
    { ind_id: 'a', overlay_id: 'a.old' },
    { ind_id: 'b', overlay_id: 'b.old' },
  ]

  const result = replaceIndicatorOverlaySlice(overlays, {
    indicatorId: 'a',
    nextSlice: [],
    activeIndicatorIds: new Set(['a', 'b']),
  })

  assert.deepEqual(result, [{ ind_id: 'b', overlay_id: 'b.old' }])
})

