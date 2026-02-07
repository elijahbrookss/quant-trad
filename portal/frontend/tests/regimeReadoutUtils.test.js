import {
  buildCandleSnapshots,
  buildReadoutSnapshot,
  buildRegimeBlockSnapshots,
  findNearestCandleTime,
  getActiveRegimeBlock,
  getNearestCandleStats,
  glyphForAxisState,
} from '../src/components/bots/regimeReadoutUtils.js'

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

  it('selects the active regime block by timestamp', () => {
    const blocks = buildRegimeBlockSnapshots([
      { x1: 100, x2: 199, known_at: 100, structure: { state: 'trend' }, confidence: 0.6 },
      { x1: 200, x2: 399, known_at: 220, structure: { state: 'range' }, confidence: 0.6 },
      { x1: 400, x2: 599, known_at: 400, structure: { state: 'transition' }, confidence: 0.6 },
    ])
    expect(getActiveRegimeBlock(blocks, 120)?.structure?.state).toBe('trend')
    expect(getActiveRegimeBlock(blocks, 210)?.structure?.state).toBe('trend')
    expect(getActiveRegimeBlock(blocks, 250)?.structure?.state).toBe('range')
    expect(getActiveRegimeBlock(blocks, 10)).toBe(null)
    expect(getActiveRegimeBlock(blocks, 510)?.structure?.state).toBe('transition')
  })

  it('selects nearest candle stats by timestamp', () => {
    const candles = buildCandleSnapshots([
      { time: 100, volatility: { state: 'high' } },
      { time: 200, volatility: { state: 'low' } },
      { time: 260, volatility: { state: 'low' } },
      { time: 400, volatility: { state: 'normal' } },
    ])
    expect(getNearestCandleStats(candles, 100)?.volatility?.state).toBe('high')
    expect(getNearestCandleStats(candles, 240)?.volatility?.state).toBe('low')
    expect(getNearestCandleStats(candles, 380)?.volatility?.state).toBe('normal')
  })

  it('builds readout snapshot with block structure and candle modifiers', () => {
    const blocks = buildRegimeBlockSnapshots([
      { x1: 100, x2: 299, known_at: 100, structure: { state: 'trend' }, confidence: 0.7, block_id: 'b1', bars: 200 },
      { x1: 300, x2: 599, known_at: 300, structure: { state: 'range' }, confidence: 0.5, block_id: 'b2', bars: 300 },
    ])
    const candles = buildCandleSnapshots([
      { time: 120, volatility: { state: 'high' }, confidence: 0.4 },
      { time: 280, volatility: { state: 'low' }, confidence: 0.4 },
      { time: 320, volatility: { state: 'normal' }, confidence: 0.4 },
    ])
    const snapshot = buildReadoutSnapshot({ focusTs: 280, blocks, points: candles })
    expect(snapshot?.structure?.state).toBe('trend')
    expect(snapshot?.structure?.block_id).toBe('b1')
    expect(snapshot?.volatility?.state).toBe('low')
  })

  it('finds nearest candle time for focus', () => {
    const candleData = [{ time: 100 }, { time: 200 }, { time: 320 }]
    expect(findNearestCandleTime(candleData, 210)).toBe(200)
    expect(findNearestCandleTime(candleData, 330)).toBe(320)
  })
})
