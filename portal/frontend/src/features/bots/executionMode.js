export function normalizeExecutionMode(value, fallback = 'fast') {
  const normalized = String(value || '').trim().toLowerCase()
  if (normalized === 'full' || normalized === 'walk-forward' || normalized === 'walkforward') return 'full'
  if (normalized === 'fast' || normalized === 'instant') return 'fast'
  if (fallback === 'full') return 'full'
  if (fallback === 'fast') return 'fast'
  return ''
}

export function formatExecutionModeLabel(value) {
  return normalizeExecutionMode(value) === 'full' ? 'FULL (intrabar)' : 'FAST'
}

export function formatExecutionModeShort(value) {
  return normalizeExecutionMode(value) === 'full' ? 'FULL' : 'FAST'
}

export function executionModeDescription(value) {
  return normalizeExecutionMode(value) === 'full'
    ? 'Slower, more realistic execution'
    : 'Faster, conservative'
}

function readPath(source, path) {
  let cursor = source
  for (const segment of path) {
    if (!cursor || typeof cursor !== 'object' || !(segment in cursor)) return undefined
    cursor = cursor[segment]
  }
  return cursor
}

export function resolveExecutionMode(source = {}) {
  const candidates = [
    source?.execution_mode,
    source?.executionMode,
    readPath(source, ['risk', 'execution_mode']),
    readPath(source, ['run', 'execution_mode']),
    readPath(source, ['run_config', 'execution_mode']),
    readPath(source, ['config_snapshot', 'execution_mode']),
    readPath(source, ['last_run_artifact', 'execution_mode']),
    readPath(source, ['runtime', 'execution_mode']),
    readPath(source, ['runtime_metadata', 'execution_mode']),
    readPath(source, ['run', 'config_snapshot', 'execution_mode']),
    readPath(source, ['run', 'config_snapshot', 'bot', 'execution_mode']),
    readPath(source, ['run', 'config_snapshot', 'bot', 'risk', 'execution_mode']),
    readPath(source, ['lifecycle', 'telemetry', 'execution_mode']),
  ]
  for (const candidate of candidates) {
    const normalized = normalizeExecutionMode(candidate, '')
    if (normalized === 'fast' || normalized === 'full') return normalized
  }
  return 'fast'
}

export function executionModeUsesIntrabar(value) {
  return normalizeExecutionMode(value) === 'full'
}
