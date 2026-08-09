export function safeRunLensOrigin(value) {
  if (value === '/overview') return value
  if (String(value || '').startsWith('/operations')) return value
  return '/operations?tab=runs'
}

export function initialRunInspection(locationState, runId) {
  const run = locationState?.run
  if (!run || String(run.run_id || '') !== String(runId || '')) return null
  return {
    schema_version: 'navigation_hint.v1',
    run,
    definition: locationState?.definition || run.definition || {},
    observed_at: null,
  }
}

export function assertRunInspectionScope(inspection, runId) {
  if (String(inspection?.run?.run_id || '') !== String(runId || '')) {
    throw new Error('Authoritative run projection returned a mismatched identity.')
  }
  return inspection
}

export function projectRunAsBot(inspection) {
  const run = inspection?.run || {}
  const definition = inspection?.definition || {}
  return {
    ...definition,
    id: definition.id || run.bot_id,
    name: definition.name || run.bot_name || 'Run inspection',
    strategy_id: definition.strategy_id || run.strategy_id,
    strategy_variant_name: definition.strategy_variant_name || run.strategy_name,
    run_type: run.run_type || definition.run_type,
    execution_mode: run.execution_mode || definition.execution_mode,
    status: run.runtime_status || run.status || 'unknown',
    active_run_id: run.run_id,
    latest_run_id: run.run_id,
    lifecycle: run.lifecycle || {},
    runtime: {
      status: run.runtime_status || run.status || 'unknown',
      last_event_at: run.projection?.known_at || run.updated_at || null,
    },
    run,
  }
}
