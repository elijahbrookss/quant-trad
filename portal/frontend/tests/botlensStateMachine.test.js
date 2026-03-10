import test from 'node:test'
import assert from 'node:assert/strict'

import { botlensReducer, initialBotLensState, BOTLENS_PHASES } from '../src/components/bots/botlensStateMachine.js'

test('bootstrap initializes bounded state for selected series', () => {
  const next = botlensReducer(initialBotLensState, {
    type: 'BOOTSTRAP_SUCCESS',
    runId: 'run-1',
    seriesKey: 'BTC|1m',
    seq: 20,
    candles: [{ time: 1 }, { time: 2 }],
  })
  assert.equal(next.phase, BOTLENS_PHASES.LIVE)
  assert.equal(next.seq, 20)
  assert.equal(next.candles.length, 2)
})

test('history page prepends and dedupes overlaps', () => {
  const seeded = {
    ...initialBotLensState,
    phase: BOTLENS_PHASES.LIVE,
    candles: [{ time: 2 }, { time: 3 }],
  }
  const next = botlensReducer(seeded, {
    type: 'HISTORY_PAGE_SUCCESS',
    candles: [{ time: 1 }, { time: 2 }],
  })
  assert.deepEqual(next.candles.map((x) => x.time), [1, 2, 3])
})

test('seq gap enters resyncing instead of replaying backlog', () => {
  const next = botlensReducer(initialBotLensState, { type: 'SEQ_GAP' })
  assert.equal(next.phase, BOTLENS_PHASES.RESYNCING)
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
    seriesKey: 'BTC|1m',
    seq: 60,
    candles: [{ time: 58 }, { time: 59 }, { time: 60 }],
  })
  assert.deepEqual(next.candles.map((x) => x.time), [58, 59, 60])
  assert.equal(next.seq, 60)
})
