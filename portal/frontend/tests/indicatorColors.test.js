import test from 'node:test'
import assert from 'node:assert/strict'

import { applyIndicatorColors } from '../src/components/indicatorSignals.js'
import { normalizeIndicatorArtifactResponse } from '../src/components/indicatorArtifacts.js'
import {
  getIndicatorColorMode,
  getIndicatorColorPalettes,
  getIndicatorSignalColor,
  getSelectedIndicatorPalette,
  getPaletteOverlayColor,
  supportsCustomIndicatorColor,
  supportsIndicatorPaletteSelection,
  usesIndicatorPalette,
} from '../src/utils/indicatorColors.js'

test('indicator color mode defaults to single when not declared', () => {
  assert.equal(getIndicatorColorMode({}), 'single')
  assert.equal(supportsCustomIndicatorColor({}), true)
  assert.equal(usesIndicatorPalette({}), false)
})

test('indicator color mode honors palette declarations', () => {
  const indicator = {
    color_mode: 'palette',
    manifest: { color_mode: 'single' },
    color_palettes: [
      {
        key: 'ocean',
        label: 'Ocean',
        signal_color: '#0ea5e9',
        overlay_colors: { candle_stats_atr_short: '#0ea5e9' },
      },
    ],
  }

  assert.equal(getIndicatorColorMode(indicator), 'palette')
  assert.equal(supportsCustomIndicatorColor(indicator), false)
  assert.equal(usesIndicatorPalette(indicator), true)
  assert.equal(supportsIndicatorPaletteSelection(indicator), true)
  assert.equal(getIndicatorColorPalettes(indicator).length, 1)
  assert.equal(getSelectedIndicatorPalette(indicator)?.key, 'ocean')
  assert.equal(getIndicatorSignalColor(indicator), '#0ea5e9')
  assert.equal(getPaletteOverlayColor(indicator, 'candle_stats_atr_short'), '#0ea5e9')
})

test('normalizeIndicatorArtifactResponse keeps overlay color sets for multi-overlay indicators', () => {
  const indicator = {
    id: 'candle-stats',
    type: 'candle_stats',
    color: '#60a5fa',
    color_mode: 'palette',
    color_palette: 'ocean',
    color_palettes: [
      {
        key: 'ocean',
        label: 'Ocean',
        signal_color: '#0ea5e9',
        overlay_colors: {
          candle_stats_atr_short: '#0ea5e9',
        },
      },
    ],
  }

  const result = normalizeIndicatorArtifactResponse(indicator, [
    {
      type: 'candle_stats_atr_short',
      payload: { polylines: [{ points: [{ time: 1, price: 10 }] }] },
      ui: { color: '#ef4444' },
    },
  ])

  assert.equal(result[0].color, '#0ea5e9')
  assert.equal(result[0].ui.color, '#0ea5e9')
  assert.equal(result[0].ui.color_policy, 'overlay')
})

test('applyIndicatorColors preserves explicit polyline colors', () => {
  const overlays = [
    {
      ind_id: 'candle-stats',
      color: '#60a5fa',
      payload: {
        polylines: [
          { color: '#22c55e', points: [{ time: 1, price: 10 }] },
          { color: '#ef4444', points: [{ time: 1, price: 11 }] },
        ],
      },
    },
  ]

  const result = applyIndicatorColors(overlays, { 'candle-stats': '#60a5fa' })

  assert.equal(result[0].payload.polylines[0].color, '#22c55e')
  assert.equal(result[0].payload.polylines[1].color, '#ef4444')
})

test('applyIndicatorColors preserves overlay-set colors for multi-overlay indicators', () => {
  const overlays = [
    {
      ind_id: 'candle-stats',
      color: '#ef4444',
      ui: {
        color: '#ef4444',
        color_policy: 'overlay',
      },
      payload: {
        polylines: [
          { points: [{ time: 1, price: 10 }] },
        ],
      },
    },
  ]

  const result = applyIndicatorColors(overlays, { 'candle-stats': '#60a5fa' })

  assert.equal(result[0].color, '#ef4444')
  assert.equal(result[0].ui.color, '#ef4444')
  assert.match(result[0].payload.polylines[0].color, /^rgba\(239,68,68,/)
})

test('applyIndicatorColors applies selected palette colors to indicator and signal overlays', () => {
  const overlays = [
    {
      ind_id: 'candle-stats',
      type: 'candle_stats_atr_short',
      source: 'indicator',
      color: '#ef4444',
      ui: {
        color: '#ef4444',
        color_policy: 'overlay',
      },
      payload: {
        polylines: [
          { color: '#ef4444', points: [{ time: 1, price: 10 }] },
        ],
      },
    },
    {
      ind_id: 'candle-stats',
      type: 'indicator_signal',
      source: 'signal',
      color: '#38bdf8',
      ui: {
        color: '#38bdf8',
        color_policy: 'indicator',
      },
      payload: {
        bubbles: [{ time: 1, price: 10, label: 'Breakout' }],
      },
    },
  ]

  const indicators = [
    {
      id: 'candle-stats',
      color_mode: 'palette',
      color_palette: 'slate',
      color_palettes: [
        {
          key: 'slate',
          label: 'Slate',
          signal_color: '#94a3b8',
          overlay_colors: {
            candle_stats_atr_short: '#64748b',
          },
        },
      ],
    },
  ]

  const result = applyIndicatorColors(overlays, {}, indicators)

  assert.equal(result[0].color, '#64748b')
  assert.match(result[0].payload.polylines[0].color, /^rgba\(100,116,139,/)
  assert.equal(result[1].color, '#94a3b8')
  assert.equal(result[1].ui.color, '#94a3b8')
})

test('applyIndicatorColors still tints polylines with no explicit color', () => {
  const overlays = [
    {
      ind_id: 'generic',
      color: '#60a5fa',
      payload: {
        polylines: [
          { points: [{ time: 1, price: 10 }] },
        ],
      },
    },
  ]

  const result = applyIndicatorColors(overlays, { generic: '#60a5fa' })

  assert.match(result[0].payload.polylines[0].color, /^rgba\(/)
})
