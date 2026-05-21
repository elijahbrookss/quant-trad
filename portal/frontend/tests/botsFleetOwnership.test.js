import test from 'node:test'
import assert from 'node:assert/strict'

import {
  initializeBotsPageRuntime,
  mergeFleetBotRuntime,
  removeFleetBotRecord,
  replaceFleetBotsSnapshot,
  runBotStartAction,
  runBotStopAction,
  runManualFleetRefresh,
  upsertFleetBotRecord,
} from '../src/features/bots/page/useBotsPageController.js'

function createLoggerRecorder() {
  return {
    infoEvents: [],
    errorEvents: [],
    info(event, payload) {
      this.infoEvents.push({ event, payload })
    },
    error(event, payload) {
      this.errorEvents.push({ event, payload })
    },
  }
}

test('bots page initialization mounts without an automatic HTTP fleet bootstrap', async () => {
  const calls = []
  const logger = createLoggerRecorder()

  await initializeBotsPageRuntime({
    loadStrategies: async () => {
      calls.push('loadStrategies')
    },
    logger,
  })

  assert.deepEqual(calls, ['loadStrategies'])
  assert.deepEqual(logger.infoEvents.map((entry) => entry.event), ['bots_page_mounted'])
})

test('manual fleet refresh performs the intentional HTTP resync and replaces fleet state', async () => {
  const logger = createLoggerRecorder()
  const refreshing = []
  const errors = []
  const replacements = []
  let requestFleetSnapshotCalls = 0
  let runtimeCapacityCalls = 0
  let hydrated = false

  await runManualFleetRefresh({
    replaceFleetBots(bots) {
      replacements.push(bots)
    },
    requestFleetSnapshot: async () => {
      requestFleetSnapshotCalls += 1
      return [{ id: 'bot-1', name: 'Momentum Runner' }]
    },
    loadRuntimeCapacity: async () => {
      runtimeCapacityCalls += 1
    },
    logger,
    markFleetHydrated: () => {
      hydrated = true
    },
    setError: (value) => {
      errors.push(value)
    },
    setRefreshing: (value) => {
      refreshing.push(value)
    },
  })

  assert.equal(requestFleetSnapshotCalls, 1)
  assert.equal(runtimeCapacityCalls, 1)
  assert.equal(hydrated, true)
  assert.deepEqual(refreshing, [true, false])
  assert.deepEqual(errors, [null])
  assert.deepEqual(replacements, [[{ id: 'bot-1', name: 'Momentum Runner' }]])
  assert.deepEqual(
    logger.infoEvents.map((entry) => entry.event),
    ['bots_manual_refresh_start', 'bots_manual_refresh_success'],
  )
})

test('start and stop action helpers stay off the fleet snapshot path', async () => {
  const logger = createLoggerRecorder()
  const calls = []

  const startedBot = await runBotStartAction({
    bot: { id: 'bot-1', strategy_id: 'strategy-1' },
    botId: 'bot-1',
    logger,
    loadRuntimeCapacity: async () => {
      calls.push('capacity:start')
    },
    startBot: async (botId) => {
      calls.push(`start:${botId}`)
      return { id: botId, status: 'starting' }
    },
  })

  await runBotStopAction({
    botId: 'bot-1',
    logger,
    loadRuntimeCapacity: async () => {
      calls.push('capacity:stop')
    },
    stopBot: async (botId) => {
      calls.push(`stop:${botId}`)
    },
  })

  assert.deepEqual(startedBot, { id: 'bot-1', status: 'starting' })
  assert.deepEqual(calls, ['start:bot-1', 'capacity:start', 'stop:bot-1', 'capacity:stop'])
  assert.deepEqual(
    logger.infoEvents.map((entry) => entry.event),
    ['bot_start_requested', 'bot_stop_requested'],
  )
})

test('fleet record helpers keep replacement, runtime deltas, and deletion paths explicit', () => {
  const replaced = replaceFleetBotsSnapshot([
    { id: 'bot-1', name: 'Runner', runtime: { status: 'idle' } },
    null,
    { id: 'bot-2', name: 'Breakout', runtime: { status: 'running' } },
  ])

  const upserted = upsertFleetBotRecord(replaced, {
    id: 'bot-1',
    name: 'Runner',
    lifecycle: { status: 'running' },
    runtime: { seq: 7 },
  })

  const runtimeMerged = mergeFleetBotRuntime(upserted, 'bot-1', {
    status: 'running',
    worker_count: 2,
  })

  const removed = removeFleetBotRecord(runtimeMerged, 'bot-2')

  assert.equal(replaced.length, 2)
  assert.deepEqual(runtimeMerged.find((bot) => bot.id === 'bot-1')?.runtime, {
    status: 'running',
    seq: 7,
    worker_count: 2,
  })
  assert.equal(removed.some((bot) => bot.id === 'bot-2'), false)
})
