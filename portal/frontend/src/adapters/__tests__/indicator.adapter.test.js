import test from 'node:test'
import assert from 'node:assert/strict'

import { normalizeIndicatorRead } from '../indicator.adapter.js'

test('normalizeIndicatorRead flattens whole-indicator document for UI consumers', () => {
  const payload = {
    instance: {
      id: 'ind-1',
      type: 'rsi',
      name: 'RSI Fast',
      params: { period: 14 },
      dependencies: [],
      enabled: true,
      output_prefs: { crossover_up: { enabled: false } },
    },
    manifest: {
      label: 'Relative Strength Index',
      description: 'Momentum oscillator',
    },
    outputs: {
      typed: [{ name: 'crossover_up', type: 'signal', enabled: true }],
      overlays: [{ name: 'rsi_line', type: 'line' }],
    },
    capabilities: {
      runtime_supported: true,
      compute_supported: true,
    },
  }

  const normalized = normalizeIndicatorRead(payload)

  assert.equal(normalized.id, 'ind-1')
  assert.equal(normalized.type, 'rsi')
  assert.deepEqual(normalized.manifest, payload.manifest)
  assert.deepEqual(normalized.typed_outputs, payload.outputs.typed)
  assert.deepEqual(normalized.overlay_outputs, payload.outputs.overlays)
  assert.equal(normalized.runtime_supported, true)
  assert.equal(normalized.compute_supported, true)
  assert.deepEqual(normalized.capabilities, payload.capabilities)
})
