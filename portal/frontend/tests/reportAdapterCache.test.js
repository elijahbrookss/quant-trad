import test from 'node:test'
import assert from 'node:assert/strict'

import { getReportReadiness } from '../src/adapters/report.adapter.js'

function readinessResponse(runId) {
  return {
    ok: true,
    status: 200,
    headers: { get: () => 'application/json' },
    json: async () => ({
      schema_version: 'report_readiness.v1',
      run_id: runId,
      dataset_ready: true,
      results_ready: true,
      safe_to_compare: true,
      reason: 'ready',
      conditions: {},
      export_status: 'available',
      dataset_status: 'ready',
      caveats: [],
      diagnostics: {
        schema_version: 'report_diagnostics.v1',
        run_id: runId,
        items: [],
        summary: {},
      },
    }),
  }
}

test('report adapter coalesces in-flight GETs and honors cache expiry and force refresh', async () => {
  const originalFetch = globalThis.fetch
  const originalDateNow = Date.now
  const runId = 'cache-contract-run'
  let nowEpochMs = 1_000
  let calls = 0
  let releaseFirstFetch = null

  Date.now = () => nowEpochMs
  globalThis.fetch = () => {
    calls += 1
    if (calls === 1) {
      return new Promise((resolve) => {
        releaseFirstFetch = () => resolve(readinessResponse(runId))
      })
    }
    return Promise.resolve(readinessResponse(runId))
  }

  try {
    const firstRequest = getReportReadiness(runId)
    const secondRequest = getReportReadiness(runId)

    assert.equal(calls, 1)
    assert.equal(typeof releaseFirstFetch, 'function')
    releaseFirstFetch()

    const [first, second] = await Promise.all([firstRequest, secondRequest])
    assert.equal(first.run_id, runId)
    assert.equal(second.run_id, runId)

    const cached = await getReportReadiness(runId)
    assert.equal(cached.run_id, runId)
    assert.equal(calls, 1)

    nowEpochMs += 15_001
    const expired = await getReportReadiness(runId)
    assert.equal(expired.run_id, runId)
    assert.equal(calls, 2)

    const forced = await getReportReadiness(runId, { force: true })
    assert.equal(forced.run_id, runId)
    assert.equal(calls, 3)
  } finally {
    globalThis.fetch = originalFetch
    Date.now = originalDateNow
  }
})
