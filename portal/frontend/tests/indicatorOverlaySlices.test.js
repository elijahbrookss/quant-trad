import test from 'node:test'
import assert from 'node:assert/strict'

import {
  replaceIndicatorArtifactSlice,
  retainIndicatorArtifacts,
} from '../src/components/indicatorOverlaySlices.js'

test('retainIndicatorArtifacts keeps entries allowed by source policy and non-indicator overlays', () => {
  const overlays = [
    { ind_id: 'a', overlay_id: 'a.1', source: 'indicator' },
    { ind_id: 'b', overlay_id: 'b.1', source: 'signal' },
    { ind_id: 'c', overlay_id: 'c.1', source: 'indicator' },
    { overlay_id: 'system.1', source: 'system' },
  ]

  const result = retainIndicatorArtifacts(overlays, {
    indicator: new Set(['a']),
    signal: new Set(['b']),
  })

  assert.deepEqual(result, [
    { ind_id: 'a', overlay_id: 'a.1', source: 'indicator' },
    { ind_id: 'b', overlay_id: 'b.1', source: 'signal' },
    { overlay_id: 'system.1', source: 'system' },
  ])
})

test('replaceIndicatorArtifactSlice replaces one source-scoped indicator slice and preserves others', () => {
  const overlays = [
    { ind_id: 'a', overlay_id: 'a.overlay', source: 'indicator' },
    { ind_id: 'a', overlay_id: 'a.signal.old', source: 'signal' },
    { ind_id: 'b', overlay_id: 'b.old', source: 'indicator' },
    { overlay_id: 'system.1', source: 'system' },
  ]

  const result = replaceIndicatorArtifactSlice(overlays, {
    indicatorId: 'a',
    source: 'signal',
    nextSlice: [{ ind_id: 'a', overlay_id: 'a.signal.new', source: 'signal' }],
    retainBySource: {
      indicator: new Set(['a', 'b']),
      signal: new Set(['a']),
    },
  })

  assert.deepEqual(result, [
    { ind_id: 'a', overlay_id: 'a.overlay', source: 'indicator' },
    { ind_id: 'b', overlay_id: 'b.old', source: 'indicator' },
    { overlay_id: 'system.1', source: 'system' },
    { ind_id: 'a', overlay_id: 'a.signal.new', source: 'signal' },
  ])
})

test('replaceIndicatorArtifactSlice removes stale entries when a source slice is cleared', () => {
  const overlays = [
    { ind_id: 'a', overlay_id: 'a.signal.old', source: 'signal' },
    { ind_id: 'b', overlay_id: 'b.signal.old', source: 'signal' },
  ]

  const result = replaceIndicatorArtifactSlice(overlays, {
    indicatorId: 'a',
    source: 'signal',
    nextSlice: [],
    retainBySource: {
      signal: new Set(['a', 'b']),
    },
  })

  assert.deepEqual(result, [{ ind_id: 'b', overlay_id: 'b.signal.old', source: 'signal' }])
})
