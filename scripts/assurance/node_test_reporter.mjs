/**
 * Stable JSON-lines transport for native `node --test` events.
 *
 * The attestation executor captures this stream as stdout and derives a
 * separately hashed, typed result summary. This reporter intentionally does
 * not decide PASS/FAIL: the assurance validator owns those semantics.
 */

export const TRANSPORT_SCHEMA_VERSION = 'qt.node_test_events.v1'

function normalize(value, seen = new WeakSet()) {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value
  if (typeof value === 'number') return Number.isFinite(value) ? value : String(value)
  if (typeof value === 'bigint') return value.toString()
  if (typeof value === 'undefined') return null
  if (typeof value === 'function' || typeof value === 'symbol') return String(value)

  if (seen.has(value)) return '[Circular]'
  seen.add(value)

  if (Array.isArray(value)) {
    const normalized = value.map((item) => normalize(item, seen))
    seen.delete(value)
    return normalized
  }

  const normalized = {}
  if (value instanceof Error) {
    for (const key of ['name', 'message', 'stack', 'code', 'failureType', 'cause']) {
      if (value[key] !== undefined) normalized[key] = normalize(value[key], seen)
    }
  }
  for (const key of Object.keys(value).sort()) {
    if (!(key in normalized)) normalized[key] = normalize(value[key], seen)
  }
  seen.delete(value)
  return normalized
}

export default async function* nodeTestEventReporter(source) {
  let sequence = 0
  for await (const event of source) {
    const envelope = {
      schema_version: TRANSPORT_SCHEMA_VERSION,
      sequence,
      event_type: String(event?.type ?? 'unknown'),
      data: normalize(event?.data),
    }
    sequence += 1
    yield `${JSON.stringify(envelope)}\n`
  }
}
