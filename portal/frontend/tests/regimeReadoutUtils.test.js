import { buildRegimeSnapshots, findSnapshotForTime, glyphForAxisState } from '../src/components/bots/regimeReadoutUtils.js'

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

  it('selects the active regime block snapshot by timestamp', () => {
    const snapshots = buildRegimeSnapshots([
      { x1: 100, x2: 199, known_at: 100, structure: { state: 'trend' }, confidence: 0.6 },
      { x1: 200, x2: 399, known_at: 220, structure: { state: 'range' }, confidence: 0.6 },
      { x1: 400, x2: 599, known_at: 400, structure: { state: 'transition' }, confidence: 0.6 },
    ])
    expect(findSnapshotForTime(snapshots, 120)?.structure?.state).toBe('trend')
    expect(findSnapshotForTime(snapshots, 210)?.structure?.state).toBe('trend')
    expect(findSnapshotForTime(snapshots, 250)?.structure?.state).toBe('range')
    expect(findSnapshotForTime(snapshots, 10)).toBe(null)
    expect(findSnapshotForTime(snapshots, 510)?.structure?.state).toBe('transition')
  })
})
