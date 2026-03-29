import test from 'node:test'
import assert from 'node:assert/strict'

import {
  applySignalOutputPrefs,
  buildSignalOutputEnabledMap,
  buildSignalOutputPrefs,
  enabledSignalOutputNames,
  getAuthorableOutputsByType,
  isSignalOutputEnabled,
} from '../src/utils/indicatorOutputs.js'

test('buildSignalOutputEnabledMap respects typed output flags and stored prefs', () => {
  const enabledMap = buildSignalOutputEnabledMap({
    typed_outputs: [
      { name: 'breakout', type: 'signal', enabled: true },
      { name: 'retest', type: 'signal', enabled: false },
      { name: 'context_state', type: 'context' },
    ],
    output_prefs: {
      breakout: { enabled: false },
    },
  })

  assert.deepEqual(enabledMap, {
    breakout: false,
    retest: false,
  })
})

test('buildSignalOutputPrefs persists only disabled signal outputs', () => {
  const outputPrefs = buildSignalOutputPrefs(
    {
      outputs: [
        { name: 'breakout', type: 'signal' },
        { name: 'retest', type: 'signal' },
        { name: 'regime', type: 'context' },
      ],
    },
    {
      breakout: true,
      retest: false,
    },
  )

  assert.deepEqual(outputPrefs, {
    retest: { enabled: false },
  })
})

test('enabledSignalOutputNames and isSignalOutputEnabled reflect stored prefs', () => {
  const indicator = {
    typed_outputs: [
      { name: 'breakout', type: 'signal', enabled: true },
      { name: 'retest', type: 'signal', enabled: true },
    ],
    output_prefs: {
      retest: { enabled: false },
    },
  }

  assert.deepEqual(enabledSignalOutputNames(indicator), ['breakout'])
  assert.equal(isSignalOutputEnabled(indicator, 'breakout'), true)
  assert.equal(isSignalOutputEnabled(indicator, 'retest'), false)
})

test('applySignalOutputPrefs updates signal enabled flags for local UI state', () => {
  const next = applySignalOutputPrefs(
    {
      typed_outputs: [
        { name: 'breakout', type: 'signal', enabled: true },
        { name: 'retest', type: 'signal', enabled: true },
        { name: 'context_state', type: 'context' },
      ],
    },
    {
      retest: { enabled: false },
    },
  )

  assert.deepEqual(
    next.typed_outputs,
    [
      { name: 'breakout', type: 'signal', enabled: true },
      { name: 'retest', type: 'signal', enabled: false },
      { name: 'context_state', type: 'context' },
    ],
  )
})

test('getAuthorableOutputsByType hides disabled signal outputs unless already selected', () => {
  const indicator = {
    typed_outputs: [
      { name: 'breakout', label: 'Breakout', type: 'signal', enabled: true },
      { name: 'retest', label: 'Retest', type: 'signal', enabled: false },
      { name: 'regime', label: 'Regime', type: 'context' },
    ],
  }

  assert.deepEqual(
    getAuthorableOutputsByType(indicator, 'signal').map((entry) => entry.name),
    ['breakout'],
  )

  assert.deepEqual(
    getAuthorableOutputsByType(indicator, 'signal', { selectedOutputName: 'retest' }).map((entry) => entry.name),
    ['breakout', 'retest'],
  )

  assert.deepEqual(
    getAuthorableOutputsByType(indicator, 'context').map((entry) => entry.name),
    ['regime'],
  )
})
