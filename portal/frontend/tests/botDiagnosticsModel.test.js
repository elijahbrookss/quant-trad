import test from 'node:test'
import assert from 'node:assert/strict'

import {
  buildBotDiagnosticsViewModel,
  copyDiagnosticsIdentifier,
  DIAGNOSTICS_COPY_RESET_MS,
} from '../src/features/bots/diagnostics/buildBotDiagnosticsViewModel.js'

function buildLifecycle(overrides = {}) {
  return {
    status: 'crashed',
    phase: 'crashed',
    message: 'Lifecycle fallback message.',
    ...overrides,
  }
}

function buildDiagnostics(overrides = {}) {
  return {
    run_status: 'crashed',
    summary: {
      run_status: 'crashed',
      current_phase: 'crashed',
      root_failure_phase: 'startup_failed',
      root_failure_owner: 'runtime',
      root_failure_message: 'worker exitcode 1',
      first_failure_at: '2026-04-08T23:02:05Z',
      last_successful_checkpoint: 'waiting_for_series_bootstrap',
      container_launched: true,
      container_booted: true,
      workers_planned: 3,
      workers_spawned: 3,
      workers_live: 0,
      workers_failed: 3,
      failed_symbols: ['BIP-20DEC30-CDE', 'ETP-20DEC30-CDE', 'XPP-20DEC30-CDE'],
      first_failed_worker_id: 'worker-1',
      first_failed_symbol: 'BIP-20DEC30-CDE',
      failed_worker_count: 3,
      any_worker_live: false,
      crash_before_any_series_live: true,
      final_observation: {
        phase: 'crashed',
        owner: 'watchdog',
        message: 'Container runtime supervision completed.',
        at: '2026-04-08T23:02:11Z',
        status: 'crashed',
      },
    },
    events: [
      {
        event_id: 'evt-1',
        seq: 1,
        owner: 'backend',
        phase: 'container_launched',
        status: 'starting',
        checkpoint_status: 'completed',
        message: 'Container launched.',
        checkpoint_at: '2026-04-08T23:01:50Z',
      },
      {
        event_id: 'evt-2',
        seq: 2,
        owner: 'runtime',
        phase: 'startup_failed',
        status: 'startup_failed',
        checkpoint_status: 'failed',
        message: 'Worker failed during warmup.',
        checkpoint_at: '2026-04-08T23:02:05Z',
        failure_details: {
          message: 'worker exitcode 1',
          worker_id: 'worker-1',
          symbol: 'BIP-20DEC30-CDE',
          exit_code: 1,
        },
      },
    ],
    ...overrides,
  }
}

function factMap(card) {
  return Object.fromEntries(card.facts.map((fact) => [fact.label, fact.value]))
}

test('root failure message wins over final observation in the header and primary failure card', () => {
  const model = buildBotDiagnosticsViewModel({
    botId: 'bot-c11d3435-b48d-47eb-8c80-8dde4a649fbc',
    runId: 'run-86f47532-d9e5-40a9-b647-f8f1dfc2c315',
    lifecycle: buildLifecycle(),
    diagnostics: buildDiagnostics(),
    loading: false,
  })

  assert.equal(model.header.statusLabel, 'Crashed')
  assert.equal(model.header.subtitle, 'worker exitcode 1')
  assert.deepEqual(model.header.quickFacts, [
    '3 workers failed',
    '0 live',
    'Container booted',
    'Last successful: Waiting for series bootstrap',
  ])
  assert.equal(model.primaryFailure.title, 'Startup Failed')
  assert.equal(model.primaryFailure.message, 'worker exitcode 1')
  assert.match(model.primaryFailure.contextLine, /Runtime/)
  assert.match(model.primaryFailure.contextLine, /First detected/)
  assert.deepEqual(model.primaryFailure.keyFacts, [
    {
      label: 'First failure',
      value: 'worker-1 • BIP-20DEC30-CDE',
      copyItems: [
        { label: 'Worker ID', key: 'root_failure_worker_id', value: 'worker-1', displayValue: 'worker-1' },
        { label: 'Symbol', key: 'root_failure_symbol', value: 'BIP-20DEC30-CDE', displayValue: 'BIP-20DEC30-CDE' },
      ],
    },
    { label: 'Last successful', value: 'Waiting for series bootstrap' },
    { label: 'Before any series live', value: 'Yes' },
  ])
})

test('header subtitle falls back to final observation before generic status text', () => {
  const model = buildBotDiagnosticsViewModel({
    botId: 'bot-1',
    runId: 'run-1',
    lifecycle: buildLifecycle({ status: 'crashed' }),
    diagnostics: buildDiagnostics({
      summary: {
        ...buildDiagnostics().summary,
        root_failure_message: null,
        final_observation: {
          phase: 'crashed',
          owner: 'watchdog',
          message: 'Watchdog observed the container exit.',
          at: '2026-04-08T23:02:11Z',
          status: 'crashed',
        },
      },
    }),
    loading: false,
  })

  assert.equal(model.header.subtitle, 'Watchdog observed the container exit.')
})

test('final state card renders the expected summary facts', () => {
  const model = buildBotDiagnosticsViewModel({
    botId: 'bot-1',
    runId: 'run-1',
    lifecycle: buildLifecycle(),
    diagnostics: buildDiagnostics(),
    loading: false,
  })

  const facts = factMap(model.finalState)
  assert.deepEqual(facts, {
    'Run status': 'Crashed',
    'Current phase': 'Crashed',
    'Container launched': 'Yes',
    'Container booted': 'Yes',
    'Workers': 'Planned 3 • Spawned 3 • Live 0 • Failed 3',
    'Before any series live': 'Yes',
  })
})

test('worker failure summary renders the first failed worker and symbol', () => {
  const model = buildBotDiagnosticsViewModel({
    botId: 'bot-1',
    runId: 'run-1',
    lifecycle: buildLifecycle(),
    diagnostics: buildDiagnostics(),
    loading: false,
  })

  const facts = factMap(model.workerFailureSummary)
  assert.equal(facts['Failed workers'], '3')
  assert.equal(facts['First failure'], 'worker-1 • BIP-20DEC30-CDE')
  assert.match(facts['Failed symbols'], /ETP-20DEC30-CDE/)
  assert.deepEqual(model.workerFailureSummary.entries, [
    {
      key: 'worker-1',
      summary: 'worker-1 • BIP-20DEC30-CDE • exit code 1',
      message: 'worker exitcode 1',
      copyItems: [
        { label: 'Event ID', key: 'worker-1-event_id', value: 'evt-2', displayValue: 'evt-2' },
        { label: 'Worker ID', key: 'worker-1-worker_id', value: 'worker-1', displayValue: 'worker-1' },
        { label: 'Symbol', key: 'worker-1-symbol', value: 'BIP-20DEC30-CDE', displayValue: 'BIP-20DEC30-CDE' },
      ],
    },
  ])
})

test('primary failure surfaces structured reason and exception details when available', () => {
  const model = buildBotDiagnosticsViewModel({
    botId: 'bot-1',
    runId: 'run-1',
    lifecycle: buildLifecycle(),
    diagnostics: buildDiagnostics({
      summary: {
        ...buildDiagnostics().summary,
        root_failure_reason_code: 'artifact_cleanup_race',
        root_failure_exception_type: 'OSError',
        root_failure: {
          message: 'Run artifact spool cleanup raced with another worker finalizer.',
          worker_id: 'worker-3',
          symbol: 'XPP-20DEC30-CDE',
          reason_code: 'artifact_cleanup_race',
          exception_type: 'OSError',
          component: 'report_artifacts',
          operation: 'spool_cleanup',
          path: 'indicators',
        },
      },
    }),
    loading: false,
  })

  assert.equal(model.primaryFailure.message, 'Run artifact spool cleanup raced with another worker finalizer.')
  assert.deepEqual(model.primaryFailure.keyFacts.slice(0, 3), [
    {
      label: 'First failure',
      value: 'worker-3 • XPP-20DEC30-CDE',
      copyItems: [
        { label: 'Worker ID', key: 'root_failure_worker_id', value: 'worker-3', displayValue: 'worker-3' },
        { label: 'Symbol', key: 'root_failure_symbol', value: 'XPP-20DEC30-CDE', displayValue: 'XPP-20DEC30-CDE' },
      ],
    },
    { label: 'Reason', value: 'Artifact Cleanup Race' },
    { label: 'Exception', value: 'OSError' },
  ])
})

test('runtime insights expose current state, progress timing, pressure, and recent transitions', () => {
  const model = buildBotDiagnosticsViewModel({
    botId: 'bot-1',
    runId: 'run-1',
    lifecycle: buildLifecycle(),
    diagnostics: buildDiagnostics({
      runtime: {
        state: 'degraded',
        progress_state: 'churning',
        last_useful_progress_at: '2026-04-08T23:02:03Z',
        degraded: {
          started_at: '2026-04-08T23:02:04Z',
          cleared_at: null,
        },
        churn: {
          detected_at: '2026-04-08T23:02:09Z',
        },
        top_pressure: {
          reason_code: 'telemetry_backpressure',
          value: 0.75,
          unit: 'ratio',
        },
        terminal: {
          actor: 'process_exit',
          reason: 'Container runtime supervision completed.',
        },
        recent_transitions: [
          {
            from_state: 'live',
            to_state: 'degraded',
            transition_reason: 'continuity_gap:subscriber_gap',
            source_component: 'worker_bridge',
            timestamp: '2026-04-08T23:02:04Z',
          },
        ],
      },
    }),
    loading: false,
  })

  const facts = factMap(model.runtimeInsights)
  assert.equal(facts['Runtime state'], 'Degraded')
  assert.equal(facts['Progress state'], 'Churning')
  assert.match(facts['Top pressure'], /Telemetry Backpressure/)
  assert.equal(model.runtimeInsights.transitions.length, 1)
  assert.equal(model.runtimeInsights.transitions[0].label, 'live -> degraded')
})

test('lifecycle trail remains available as supporting evidence with normalized row states', () => {
  const model = buildBotDiagnosticsViewModel({
    botId: 'bot-1',
    runId: 'run-1',
    lifecycle: buildLifecycle(),
    diagnostics: buildDiagnostics(),
    loading: false,
  })

  assert.equal(model.lifecycleTrail.title, 'Lifecycle Trail')
  assert.equal(model.lifecycleTrail.rows.length, 2)
  assert.equal(model.lifecycleTrail.rows[0].seq, 2)
  assert.equal(model.lifecycleTrail.rows[0].badgeLabel, 'Failed')
  assert.equal(model.lifecycleTrail.rows[0].message, 'Worker failed during warmup.')
  assert.deepEqual(model.lifecycleTrail.rows[0].identifiers, [
    { label: 'Event ID', key: 'evt-2-event_id', value: 'evt-2', displayValue: 'evt-2' },
    { label: 'Worker ID', key: 'evt-2-worker_id', value: 'worker-1', displayValue: 'worker-1' },
    { label: 'Symbol', key: 'evt-2-symbol', value: 'BIP-20DEC30-CDE', displayValue: 'BIP-20DEC30-CDE' },
  ])
  assert.deepEqual(model.lifecycleTrail.rows[0].details, [
    {
      label: 'Failure',
      tone: 'failure',
      value: `{
  "message": "worker exitcode 1",
  "worker_id": "worker-1",
  "symbol": "BIP-20DEC30-CDE",
  "exit_code": 1
}`,
      copyItem: {
        label: 'Failure JSON',
        key: 'evt-2-failure_json',
        value: `{
  "message": "worker exitcode 1",
  "worker_id": "worker-1",
  "symbol": "BIP-20DEC30-CDE",
  "exit_code": 1
}`,
        displayValue: 'JSON payload',
      },
    },
  ])
  assert.equal(model.lifecycleTrail.rows[1].seq, 1)
  assert.equal(model.lifecycleTrail.rows[1].badgeLabel, 'Completed')
})

test('copy helper copies the full bot id and toggles copied state temporarily', async () => {
  const writes = []
  const stateChanges = []
  let scheduled = null

  await copyDiagnosticsIdentifier({
    copyKey: 'bot_id',
    value: 'c11d3435-b48d-47eb-8c80-8dde4a649fbc',
    writeText: async (value) => writes.push(value),
    onCopiedChange: (key, copied) => stateChanges.push([key, copied]),
    scheduleReset: (reset, ms) => {
      scheduled = { reset, ms }
    },
  })

  assert.deepEqual(writes, ['c11d3435-b48d-47eb-8c80-8dde4a649fbc'])
  assert.deepEqual(stateChanges, [['bot_id', true]])
  assert.equal(scheduled.ms, DIAGNOSTICS_COPY_RESET_MS)
  scheduled.reset()
  assert.deepEqual(stateChanges, [['bot_id', true], ['bot_id', false]])
})

test('copy helper copies the full run id', async () => {
  const writes = []

  await copyDiagnosticsIdentifier({
    copyKey: 'run_id',
    value: '86f47532-d9e5-40a9-b647-f8f1dfc2c315',
    writeText: async (value) => writes.push(value),
    onCopiedChange: () => {},
    scheduleReset: () => {},
  })

  assert.deepEqual(writes, ['86f47532-d9e5-40a9-b647-f8f1dfc2c315'])
})

test('copied state stays isolated per identifier', async () => {
  const state = { bot_id: false, run_id: false }
  const resets = {}

  const onCopiedChange = (key, copied) => {
    state[key] = copied
  }

  await copyDiagnosticsIdentifier({
    copyKey: 'bot_id',
    value: 'bot-c11d3435-b48d-47eb-8c80-8dde4a649fbc',
    writeText: async () => {},
    onCopiedChange,
    scheduleReset: (reset) => {
      resets.bot = reset
    },
  })

  await copyDiagnosticsIdentifier({
    copyKey: 'run_id',
    value: 'run-86f47532-d9e5-40a9-b647-f8f1dfc2c315',
    writeText: async () => {},
    onCopiedChange,
    scheduleReset: (reset) => {
      resets.run = reset
    },
  })

  assert.deepEqual(state, { bot_id: true, run_id: true })
  resets.bot()
  assert.deepEqual(state, { bot_id: false, run_id: true })
  resets.run()
  assert.deepEqual(state, { bot_id: false, run_id: false })
})
