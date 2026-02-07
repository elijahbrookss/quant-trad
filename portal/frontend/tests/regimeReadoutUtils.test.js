import { buildRegimeSnapshots, glyphForAxisState, nearestSnapshot } from '../src/components/bots/regimeReadoutUtils.js'

describe('regimeReadoutUtils', () => {
  it('maps glyphs by axis and state', () => {
    expect(glyphForAxisState('volatility', 'high')).toBe('↑')
    expect(glyphForAxisState('volatility', 'low')).toBe('↓')
    expect(glyphForAxisState('liquidity', 'thin')).toBe('○')
    expect(glyphForAxisState('liquidity', 'normal')).toBe('◐')
    expect(glyphForAxisState('expansion', 'expanding')).toBe('↗')
    expect(glyphForAxisState('expansion', 'compressing')).toBe('↘')
    expect(glyphForAxisState('structure', 'transition')).toBe('?')
  })

  it('selects the nearest snapshot by timestamp', () => {
    const snapshots = buildRegimeSnapshots([
      { time: 100, structure: { state: 'trend' }, confidence: 0.6 },
      { time: 200, structure: { state: 'range' }, confidence: 0.6 },
      { time: 400, structure: { state: 'transition' }, confidence: 0.6 },
    ])
    expect(nearestSnapshot(snapshots, 100)?.ts).toBe(100)
    expect(nearestSnapshot(snapshots, 180)?.ts).toBe(200)
    expect(nearestSnapshot(snapshots, 350)?.ts).toBe(400)
    expect(nearestSnapshot(snapshots, 10)?.ts).toBe(100)
    expect(nearestSnapshot(snapshots, 500)?.ts).toBe(400)
  })
})
