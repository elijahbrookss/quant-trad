import test from 'node:test'
import assert from 'node:assert/strict'

import { buildBotsPageViewModel } from '../src/features/bots/page/buildBotsPageViewModel.js'

test('page view model keeps the fleet surface in a connecting state until the first SSE snapshot arrives', () => {
  const connectingState = buildBotsPageViewModel({
    botStreamState: 'connecting',
    filteredBots: [],
    hasFleetSnapshot: false,
    lensBot: null,
    refreshingFleet: false,
    search: '',
    sortedBots: [],
  })

  const waitingState = buildBotsPageViewModel({
    botStreamState: 'open',
    filteredBots: [],
    hasFleetSnapshot: false,
    lensBot: null,
    refreshingFleet: false,
    search: '',
    sortedBots: [],
  })

  assert.equal(connectingState.fleetState.mode, 'loading')
  assert.match(connectingState.fleetState.title, /Connecting to live fleet state/)
  assert.equal(waitingState.runtimeState.mode, 'loading')
  assert.match(waitingState.runtimeState.detail, /first snapshot/)
})

test('page view model exposes empty fleet states only after fleet hydration exists', () => {
  const emptyFilterState = buildBotsPageViewModel({
    botStreamState: 'open',
    filteredBots: [],
    hasFleetSnapshot: true,
    lensBot: null,
    refreshingFleet: false,
    search: 'btc',
    sortedBots: [{ id: 'bot-1' }],
  })

  assert.equal(emptyFilterState.fleetState.mode, 'empty')
  assert.match(emptyFilterState.fleetState.title, /No bots match/)
  assert.equal(emptyFilterState.runtimeState.mode, 'idle')
})

test('page view model distinguishes idle runtime workspace from selected runtime workspace', () => {
  const idleState = buildBotsPageViewModel({
    botStreamState: 'open',
    filteredBots: [{ id: 'bot-1' }],
    hasFleetSnapshot: true,
    lensBot: null,
    refreshingFleet: false,
    search: '',
    sortedBots: [{ id: 'bot-1' }],
  })
  const selectedState = buildBotsPageViewModel({
    botStreamState: 'open',
    filteredBots: [{ id: 'bot-1' }],
    hasFleetSnapshot: true,
    lensBot: { id: 'bot-1', name: 'Momentum Runner' },
    refreshingFleet: false,
    search: '',
    sortedBots: [{ id: 'bot-1' }],
  })

  assert.equal(idleState.runtimeState.mode, 'idle')
  assert.equal(selectedState.runtimeState.mode, 'selected')
  assert.equal(selectedState.runtimeState.title, 'Momentum Runner')
})
