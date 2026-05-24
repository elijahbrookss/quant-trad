import test from 'node:test'
import assert from 'node:assert/strict'

import { getReportReadiness } from '../src/adapters/report.adapter.js'

test('report adapter reuses concurrent GET requests for the same contract URL', async () => {
  const originalFetch = globalThis.fetch
  const runId = `cache-${Date.now()}`
  let calls = 0
  globalThis.fetch = async () => {
    calls += 1
    await new Promise((resolve) => setTimeout(resolve, 1))
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

  try {
    const [first, second] = await Promise.all([
      getReportReadiness(runId),
      getReportReadiness(runId),
    ])

    assert.equal(calls, 1)
    assert.equal(first.run_id, runId)
    assert.equal(second.run_id, runId)
  } finally {
    globalThis.fetch = originalFetch
  }
})
