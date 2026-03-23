import test from 'node:test'
import assert from 'node:assert/strict'

import { botlensReducer, initialBotLensState, BOTLENS_PHASES } from '../src/components/bots/botlensStateMachine.js'

test('bootstrap initializes bounded state for selected series', () => {
  const next = botlensReducer(initialBotLensState, {
    type: 'BOOTSTRAP_SUCCESS',
    runId: 'run-1',
    seriesKey: 'instrument-btc|1m',
    seq: 20,
  })
  assert.equal(next.phase, BOTLENS_PHASES.LIVE)
  assert.equal(next.seq, 20)
  assert.equal(next.seriesKey, 'instrument-btc|1m')
})

test('history page preserves live/historical phase without owning candle state', () => {
  const seeded = {
    ...initialBotLensState,
    phase: BOTLENS_PHASES.LIVE,
  }
  const next = botlensReducer(seeded, {
    type: 'HISTORY_PAGE_SUCCESS',
    candles: [{ time: 1 }, { time: 2 }],
  })
  assert.equal(next.phase, BOTLENS_PHASES.LIVE)
})

test('seq gap enters resyncing instead of replaying backlog', () => {
  const next = botlensReducer(initialBotLensState, { type: 'SEQ_GAP' })
  assert.equal(next.phase, BOTLENS_PHASES.RESYNCING)
})

test('continuity unavailable enters terminal live fault phase', () => {
  const next = botlensReducer(initialBotLensState, { type: 'CONTINUITY_UNAVAILABLE' })
  assert.equal(next.phase, BOTLENS_PHASES.CONTINUITY_UNAVAILABLE)
})


test('re-bootstrap replaces queued/live state instead of replaying backlog', () => {
  const live = {
    ...initialBotLensState,
    phase: BOTLENS_PHASES.LIVE,
    runId: 'run-1',
    seq: 50,
    candles: [{ time: 49 }, { time: 50 }],
  }
  const next = botlensReducer(live, {
    type: 'BOOTSTRAP_SUCCESS',
    runId: 'run-1',
    seriesKey: 'instrument-btc|1m',
    seq: 60,
  })
  assert.equal(next.seq, 60)
  assert.equal(next.seriesKey, 'instrument-btc|1m')
})
