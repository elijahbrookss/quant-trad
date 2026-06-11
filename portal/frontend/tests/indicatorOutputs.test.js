import test from 'node:test'
import assert from 'node:assert/strict'

import {
  getAuthorableOutputsByType,
  getIndicatorOutputsByType,
  indicatorHasAuthorableOutputs,
} from '../src/utils/indicatorOutputs.js'

test('getIndicatorOutputsByType returns every typed output for the requested type', () => {
  const indicator = {
    typed_outputs: [
      { name: 'breakout', label: 'Breakout', type: 'signal' },
      { name: 'retest', label: 'Retest', type: 'signal' },
      { name: 'regime', label: 'Regime', type: 'context' },
    ],
  }

  assert.deepEqual(
    getIndicatorOutputsByType(indicator, 'signal').map((entry) => entry.name),
    ['breakout', 'retest'],
  )
})

test('authorable outputs are catalog outputs, not preference-filtered outputs', () => {
  const indicator = {
    typed_outputs: [
      { name: 'breakout', label: 'Breakout', type: 'signal', enabled: false },
      { name: 'retest', label: 'Retest', type: 'signal' },
      { name: 'regime', label: 'Regime', type: 'context' },
    ],
  }

  assert.deepEqual(
    getAuthorableOutputsByType(indicator, 'signal').map((entry) => entry.name),
    ['breakout', 'retest'],
  )
  assert.equal(indicatorHasAuthorableOutputs(indicator, 'signal'), true)
  assert.deepEqual(
    getAuthorableOutputsByType(indicator, 'context').map((entry) => entry.name),
    ['regime'],
  )
})
