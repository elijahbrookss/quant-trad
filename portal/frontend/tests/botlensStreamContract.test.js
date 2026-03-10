import test from 'node:test'
import assert from 'node:assert/strict'

import { shouldForceResyncForSeqGap } from '../src/components/bots/botlensStreamContract.js'

test('shouldForceResyncForSeqGap returns true when seq jump exceeds allowed gap', () => {
  assert.equal(shouldForceResyncForSeqGap({ previousSeq: 10, nextSeq: 13, maxAllowedGap: 1 }), true)
})

test('shouldForceResyncForSeqGap returns false for contiguous sequence', () => {
  assert.equal(shouldForceResyncForSeqGap({ previousSeq: 10, nextSeq: 11, maxAllowedGap: 1 }), false)
})
