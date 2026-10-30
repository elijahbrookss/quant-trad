import test from 'node:test'
import assert from 'node:assert/strict'
import { METRICS, byteLabel, chartPoints, exactRange, grafanaUrl, lineSegments, responseStale, tableRows, validateMetrics } from './storageTrends.js'

const range = { from: '2026-10-25T00:00:00Z', to: '2026-10-26T00:00:00Z', bucket_seconds: 300 }
function response() {
  return { schema_version: 'qt.storage_metrics.v1', range, cadence_seconds: 300,
    generated_at: '2026-10-26T00:00:00Z',
    series: METRICS.map((metric) => ({ metric_id: metric.id, unit: 'bytes', basis: metric.basis, points: [] })),
    links: { grafana: { state: 'available', dashboard_url: 'https://metrics.example/d/quanttrad-capacity-growth/quanttrad-capacity-and-database-growth' } } }
}
test('absolute displayed Grafana bounds are stable across DST and later clock changes', () => {
  const payload = response()
  payload.range = { ...range, from: '2026-11-01T00:30:00-04:00', to: '2026-11-01T02:30:00-05:00' }
  const link = new URL(grafanaUrl(payload))
  assert.equal(link.searchParams.get('from'), String(Date.parse(payload.range.from)))
  assert.equal(link.searchParams.get('to'), String(Date.parse(payload.range.to)))
  assert.equal(Number(link.searchParams.get('to')) - Number(link.searchParams.get('from')), 3 * 3600_000)
  assert.equal(link.searchParams.get('timezone'), 'utc')
  assert.equal(grafanaUrl(payload), link.toString())
  payload.links.grafana.dashboard_url = 'javascript:alert(1)'
  assert.equal(grafanaUrl(payload), null)
  payload.links.grafana.dashboard_url = 'https://user:secret@metrics.example/'
  assert.equal(grafanaUrl(payload), null)
})
test('ranges use absolute UTC, enforce half-open positive span and thirty-day maximum', () => {
  const now = Date.parse('2026-10-26T00:00:00Z')
  const selected = exactRange('24h', {}, now)
  assert.equal(selected.from, '2026-10-25T00:00:00.000Z')
  assert.equal(selected.to, '2026-10-26T00:00:00.000Z')
  assert.throws(() => exactRange('custom', { from: '2026-01-01Z', to: '2026-03-01Z' }, now))
  assert.throws(() => exactRange('custom', { from: 'bad', to: selected.to }, now))
})
test('missing values stay whitespace and long gaps break lines at the displayed cadence', () => {
  const source = { points: [
    { time: '2026-10-25T00:00:00Z', value: 0, state: 'ready' },
    { time: '2026-10-25T00:05:00Z', value: null, state: 'unavailable' },
    { time: '2026-10-25T00:30:00Z', value: 80, state: 'ready' },
  ] }
  const points = chartPoints(source, range, 300)
  assert.equal(points[0].value, 0)
  assert.equal('value' in points[1], false)
  assert.equal(points[2].breakBefore, true)
  assert.equal(points[2].value, 80)
  assert.deepEqual(lineSegments(points).map((segment) => segment.map((point) => point.value)), [[0], [80]])
  assert.equal(byteLabel(null), 'Unavailable')
  const coarse = { ...range, bucket_seconds: 1800 }
  assert.equal(chartPoints({ points: [source.points[0], source.points[2]] }, coarse, 300).length, 2)
})
test('coarse stock charts keep actual sample clocks and raw gaps without fabricated points', () => {
  const source = { metric_id: 'database_allocated_bytes', points: [
    { time: '2026-10-25T00:00:00Z', observed_at: '2026-10-25T00:29:00Z', value: 50, state: 'ready', max_gap_seconds: 300 },
    { time: '2026-10-25T00:30:00Z', observed_at: '2026-10-25T00:59:00Z', value: 70, state: 'partial', max_gap_seconds: 700 },
  ] }
  const points = chartPoints(source, { ...range, bucket_seconds: 1800 }, 300)
  assert.equal(points.length, 2)
  assert.equal(points[0].time, Date.parse(source.points[0].observed_at) / 1000)
  assert.equal(lineSegments(points).length, 2)
})
test('fresh response age differs from historical event age and missing table cells stay unavailable', () => {
  const payload = response()
  assert.equal(responseStale(payload, true, Date.parse(payload.generated_at) + 901_000), true)
  assert.equal(responseStale(payload, false, Date.parse(payload.generated_at) + 901_000), false)
  payload.series[0].points = [{ time: range.from, value: 0, state: 'ready' }]
  payload.series[1].points = [{ time: range.from, value: null, state: 'unavailable' }]
  const row = tableRows(payload)[0]
  assert.equal(row.values.database_allocated_bytes, 0)
  assert.equal(row.values.market_allocated_bytes, null)
})
test('wrong byte bases and unbounded responses cannot replace a displayed chart', () => {
  assert.equal(validateMetrics(response()).schema_version, 'qt.storage_metrics.v1')
  const payload = response()
  payload.series[0].basis = 'gross_intake'
  assert.throws(() => validateMetrics(payload), /basis/)
  const oversized = response()
  oversized.series[0].points = Array.from({ length: 721 }, () => ({ time: range.from, value: 0 }))
  assert.throws(() => validateMetrics(oversized), /bound/)
})
