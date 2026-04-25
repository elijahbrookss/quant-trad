import test from 'node:test'
import assert from 'node:assert/strict'

import {
  buildSelectedSymbolSubscriptionPayload,
  buildBotLensLiveTransportEpoch,
  shouldOpenBotLensLiveTransport,
  shouldSendBotLensSelectedSymbolSubscription,
} from '../src/features/bots/botlens/hooks/useBotLensLiveTransport.js'

test('live transport stays open for an active run even while the next symbol snapshot is loading', () => {
  assert.equal(
    shouldOpenBotLensLiveTransport({
      open: true,
      botId: 'bot-1',
      runId: 'run-1',
      transportEligible: true,
      selectedSymbolReady: false,
    }),
    true,
  )
  assert.equal(
    shouldOpenBotLensLiveTransport({
      open: true,
      botId: 'bot-1',
      runId: 'run-1',
      transportEligible: true,
      selectedSymbolReady: true,
    }),
    true,
  )
})

test('live transport stays closed when run-scoped prerequisites are missing', () => {
  assert.equal(
    shouldOpenBotLensLiveTransport({
      open: true,
      botId: 'bot-1',
      runId: null,
      transportEligible: true,
      selectedSymbolReady: true,
    }),
    false,
  )
  assert.equal(
    shouldOpenBotLensLiveTransport({
      open: false,
      botId: 'bot-1',
      runId: 'run-1',
      transportEligible: true,
      selectedSymbolReady: true,
    }),
    false,
  )
})

test('transport epoch stays stable across replay cursor updates and changes only when lifecycle changes', () => {
  const epoch = buildBotLensLiveTransportEpoch({
    open: true,
    botId: 'bot-1',
    runId: 'run-1',
    transportEligible: true,
    selectedSymbolReady: true,
    reconnectTick: 0,
  })

  assert.equal(
    epoch,
    buildBotLensLiveTransportEpoch({
      open: true,
      botId: 'bot-1',
      runId: 'run-1',
      transportEligible: true,
      selectedSymbolReady: true,
      reconnectTick: 0,
      resumeFromSeq: 25,
      streamSessionId: 'stream-2',
      selectedSymbolKey: 'instrument-eth|5m',
    }),
  )
  assert.notEqual(
    epoch,
    buildBotLensLiveTransportEpoch({
      open: true,
      botId: 'bot-1',
      runId: 'run-1',
      transportEligible: true,
      selectedSymbolReady: true,
      reconnectTick: 1,
    }),
  )
  assert.equal(
    buildBotLensLiveTransportEpoch({
      open: true,
      botId: 'bot-1',
      runId: 'run-1',
      transportEligible: true,
      selectedSymbolReady: false,
      reconnectTick: 0,
    }),
    epoch,
  )
})

test('selected-symbol subscription is sent once the websocket becomes open after bootstrap is ready', () => {
  assert.equal(
    shouldSendBotLensSelectedSymbolSubscription({
      socketReadyState: 1,
      selectedSymbolKey: 'instrument-btc|1m',
      selectedSymbolReady: true,
      subscribedSymbolKey: null,
      subscriptionSocketMatches: false,
    }),
    true,
  )
})

test('selected-symbol subscription is skipped when the same socket already owns that symbol', () => {
  assert.equal(
    shouldSendBotLensSelectedSymbolSubscription({
      socketReadyState: 1,
      selectedSymbolKey: 'instrument-btc|1m',
      selectedSymbolReady: true,
      subscribedSymbolKey: 'instrument-btc|1m',
      subscriptionSocketMatches: true,
    }),
    false,
  )
})

test('selected-symbol subscription waits for both socket readiness and symbol bootstrap readiness', () => {
  assert.equal(
    shouldSendBotLensSelectedSymbolSubscription({
      socketReadyState: 0,
      selectedSymbolKey: 'instrument-btc|1m',
      selectedSymbolReady: true,
      subscribedSymbolKey: null,
      subscriptionSocketMatches: false,
    }),
    false,
  )
  assert.equal(
    shouldSendBotLensSelectedSymbolSubscription({
      socketReadyState: 1,
      selectedSymbolKey: 'instrument-btc|1m',
      selectedSymbolReady: false,
      subscribedSymbolKey: null,
      subscriptionSocketMatches: false,
    }),
    false,
  )
})

test('selected-symbol subscription carries snapshot resume cursor for server replay', () => {
  assert.deepEqual(
    buildSelectedSymbolSubscriptionPayload({
      selectedSymbolKey: 'instrument-btc|1M',
      resumeFromSeq: 42,
      streamSessionId: 'stream-1',
    }),
    {
      type: 'set_selected_symbol',
      symbol_key: 'instrument-btc|1m',
      resume_from_seq: 42,
      stream_session_id: 'stream-1',
    },
  )
})
