import test from 'node:test'
import assert from 'node:assert/strict'

import {
  mapBotsStreamReadyState,
  resolveBotsStreamMutation,
} from '../src/components/bots/useBotStream.js'

test('bots fleet stream treats reconnecting EventSource states as connecting instead of opening a second stream', () => {
  assert.equal(mapBotsStreamReadyState(0), 'connecting')
  assert.equal(mapBotsStreamReadyState(1), 'connecting')
})

test('bots fleet stream exposes a closed EventSource as an error state', () => {
  assert.equal(mapBotsStreamReadyState(2), 'error')
})

test('bots fleet stream treats snapshot events as authoritative fleet replacement', () => {
  const mutation = resolveBotsStreamMutation('snapshot', {
    type: 'snapshot',
    bots: [{ id: 'bot-1', name: 'Momentum Runner' }],
  })

  assert.deepEqual(mutation, {
    type: 'replace',
    bots: [{ id: 'bot-1', name: 'Momentum Runner' }],
    hydrated: true,
  })
})

test('bots fleet stream routes runtime deltas through the SSE runtime writer', () => {
  const mutation = resolveBotsStreamMutation('bot_runtime', {
    bot_id: 'bot-1',
    runtime: {
      status: 'running',
      worker_count: 2,
    },
  })

  assert.deepEqual(mutation, {
    type: 'runtime',
    botId: 'bot-1',
    runtime: {
      status: 'running',
      worker_count: 2,
    },
  })
})

test('bots fleet stream routes delete events through the SSE delete writer', () => {
  const mutation = resolveBotsStreamMutation('bot_deleted', {
    bot_id: 'bot-1',
  })

  assert.deepEqual(mutation, {
    type: 'remove',
    botId: 'bot-1',
  })
})
