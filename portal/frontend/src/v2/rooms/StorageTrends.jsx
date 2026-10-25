import { useEffect, useRef, useState } from 'react'
import { createChart, HistogramSeries, LineSeries } from 'lightweight-charts'
import { readStorageMetrics } from '../../adapters/storage.adapter.js'
import { METRICS, byteLabel, chartPoints, exactRange, grafanaUrl, lineSegments, responseStale, seriesState, tableRows, validateMetrics } from './storageTrends.js'

const POLL_MS = 60_000
const REQUEST_TIMEOUT_MS = 20_000
const AGE_TICK_MS = 15_000
const utc = (time) => Number.isFinite(new Date(time).getTime()) ? new Date(time).toISOString().replace('T', ' ').replace('.000Z', ' UTC') : 'Unavailable time'
const localZone = () => Intl.DateTimeFormat().resolvedOptions().timeZone || 'local timezone'

function useMetrics(query) {
  const [displayed, setDisplayed] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [now, setNow] = useState(Date.now())
  // Freshness must advance even when the next read has not settled.
  useEffect(() => {
    if (!query.live) return undefined
    let timer
    const tick = () => setNow(Date.now())
    const visibility = () => {
      clearInterval(timer)
      if (document.visibilityState !== 'hidden') {
        tick()
        timer = setInterval(tick, AGE_TICK_MS)
      }
    }
    document.addEventListener('visibilitychange', visibility)
    visibility()
    return () => { clearInterval(timer); document.removeEventListener('visibilitychange', visibility) }
  }, [query.live])
  useEffect(() => {
    let active = true
    let timer
    let requestTimer
    let controller
    let startedAt = 0
    let running = false
    let retryOnVisible = false
    const schedule = (delay) => {
      clearTimeout(timer)
      if (active && query.live && document.visibilityState !== 'hidden') timer = setTimeout(load, delay)
    }
    async function load() {
      if (!active || running || document.visibilityState === 'hidden') return
      running = true
      startedAt = Date.now()
      setNow(startedAt)
      setLoading(true)
      const requestController = new AbortController()
      controller = requestController
      let timedOut = false
      requestTimer = setTimeout(() => {
        if (!active) return
        timedOut = true
        requestController.abort()
        setLoading(false)
        setError('Storage trends request timed out. Retry this range.')
      }, REQUEST_TIMEOUT_MS)
      try {
        const range = query.frozen || exactRange(query.preset, query.custom)
        const next = validateMetrics(await readStorageMetrics(range, requestController.signal))
        if (active && !requestController.signal.aborted) { setDisplayed(next); setError('') }
      } catch (failure) {
        if (active && !requestController.signal.aborted) setError(failure.message || 'Storage trends unavailable.')
        else if (active && timedOut) setError('Storage trends request timed out. Retry this range.')
      } finally {
        clearTimeout(requestTimer)
        running = false
        if (active) {
          setLoading(false)
          if (retryOnVisible && document.visibilityState !== 'hidden') {
            retryOnVisible = false
            load()
          } else schedule(Math.max(0, POLL_MS - (Date.now() - startedAt)))
        }
      }
    }
    const visibility = () => {
      clearTimeout(timer)
      if (document.visibilityState === 'hidden') {
        if (running) {
          retryOnVisible = true
          clearTimeout(requestTimer)
          controller?.abort()
        }
      } else if (retryOnVisible) {
        // If abort is still unwinding, finally performs the single retry.
        if (!running) { retryOnVisible = false; load() }
      } else if (query.live) schedule(Math.max(0, POLL_MS - (Date.now() - startedAt)))
      else if (!startedAt) load()
    }
    document.addEventListener('visibilitychange', visibility)
    load()
    return () => {
      active = false
      clearTimeout(timer)
      clearTimeout(requestTimer)
      controller?.abort()
      document.removeEventListener('visibilitychange', visibility)
    }
  }, [query])
  return { displayed, error, loading, now }
}

function MetricChart({ payload, metrics, bars = false, title }) {
  const host = useRef(null)
  useEffect(() => {
    const element = host.current
    if (!element) return undefined
    const chart = createChart(element, {
      width: element.clientWidth || 320, height: bars ? 110 : 230,
      layout: { background: { color: 'transparent' }, textColor: '#aaa8a3', fontSize: 11 },
      grid: { vertLines: { visible: false }, horzLines: { color: 'rgba(160,160,160,0.10)' } },
      rightPriceScale: { borderVisible: false, minimumWidth: 95 },
      timeScale: { timeVisible: true, secondsVisible: false, borderVisible: false },
      localization: { priceFormatter: (value) => byteLabel(value), timeFormatter: (time) => utc(time * 1000) },
      handleScroll: false, handleScale: false,
    })
    let hasPoints = false
    for (const metric of metrics) {
      const source = payload.series.find((entry) => entry.metric_id === metric.id)
      const points = chartPoints(source, payload.range, payload.cadence_seconds)
      hasPoints ||= points.length > 0
      const segments = bars ? [points] : lineSegments(points)
      for (const segment of segments.length ? segments : [[]]) {
        const series = chart.addSeries(bars ? HistogramSeries : LineSeries, {
          color: metric.color, lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
          pointMarkersVisible: !bars && segment.length === 1, title: metric.label,
        })
        series.setData(segment)
      }
    }
    if (hasPoints) chart.timeScale().setVisibleRange({ from: Date.parse(payload.range.from) / 1000, to: Date.parse(payload.range.to) / 1000 })
    const resize = () => chart.applyOptions({ width: element.clientWidth || 320 })
    const observer = typeof ResizeObserver === 'function' ? new ResizeObserver(resize) : null
    observer?.observe(element)
    window.addEventListener('resize', resize)
    return () => { observer?.disconnect(); window.removeEventListener('resize', resize); chart.remove() }
  }, [payload, metrics, bars])
  return <div className="qt2-storage-trend-chart" ref={host} role="img"
    aria-label={title + '. UTC time axis. Exact values are in the accessible data table below.'} />
}
const STOCKS = METRICS.filter((metric) => metric.kind === 'stock')
const FLOWS = METRICS.filter((metric) => metric.kind === 'flow')
const FLOW_SERIES = Object.fromEntries(FLOWS.map((metric) => [metric.id, [metric]]))

function MetricCaption({ payload, metric }) {
  const series = payload.series.find((entry) => entry.metric_id === metric.id)
  const state = seriesState(series)
  const summary = series?.summary
  return <div className="qt2-storage-trend-caption">
    <strong style={{ color: metric.color }}>{metric.label}</strong>
    <span>{state}{series?.reason ? ' · ' + series.reason.replaceAll('_', ' ') : ''}</span>
    {metric.kind === 'stock' ? <span>Net change {byteLabel(summary?.net_change_bytes, true)} · observed-span average {byteLabel(summary?.average_bytes_per_day, true)}/day</span>
      : <span>Recorded total {byteLabel(summary?.total_bytes)}</span>}
    {summary?.first_at && summary?.last_at && <small>Observed {utc(summary.first_at)} — {utc(summary.last_at)}</small>}
    {series?.coverage && <small>Coverage {series.coverage.complete ? 'complete within declared bounds' : 'partial'}
      {series.coverage.truncated ? ' · truncated' : ''}{Number.isFinite(series.coverage.missing_buckets) ? ' · ' + series.coverage.missing_buckets + ' missing buckets' : ''}</small>}
  </div>
}

export function StorageTrends() {
  const [query, setQuery] = useState({ preset: '24h', custom: {}, live: true })
  const [preset, setPreset] = useState('24h')
  const [custom, setCustom] = useState(() => ({
    from: new Date(Date.now() - 86_400_000).toISOString().slice(0, 16),
    to: new Date().toISOString().slice(0, 16),
  }))
  const [rangeError, setRangeError] = useState('')
  const { displayed, error, loading, now } = useMetrics(query)
  const stale = responseStale(displayed, query.live, now)
  const link = grafanaUrl(displayed)
  const rows = displayed ? tableRows(displayed) : []
  function choose(value) {
    setPreset(value)
    setRangeError('')
    if (value !== 'custom') setQuery({ preset: value, custom: {}, live: query.live })
    else setQuery({ ...query, live: false, frozen: displayed ? { ...displayed.range, resolution: 'auto' } : undefined })
  }
  function applyCustom(event) {
    event.preventDefault()
    try {
      const normalized = { from: custom.from + 'Z', to: custom.to + 'Z' }
      const frozen = exactRange('custom', normalized)
      setQuery({ preset: 'custom', custom: normalized, frozen, live: false })
      setRangeError('')
    } catch (failure) { setRangeError(failure.message) }
  }
  function toggleLive() {
    if (query.live) setQuery({ ...query, live: false, frozen: displayed ? { ...displayed.range, resolution: 'auto' } : undefined })
    else setQuery({ ...query, live: true, frozen: undefined })
  }
  return <section className="qt2-storage-trends" aria-labelledby="storage-trends-title">
    <div className="qt2-storage-section-heading">
      <div><span className="qt2-storage-eyebrow">History</span><h2 id="storage-trends-title">Storage trends</h2></div>
      {link ? <a href={link} target="_blank" rel="noopener noreferrer" onClick={() => setQuery({ ...query, live: false, frozen: { ...displayed.range, resolution: 'auto' } })}>Open in Grafana ↗</a>
        : <span className="qt2-storage-muted" title={displayed?.links?.grafana?.reason || 'No Grafana URL configured'}>Grafana unavailable</span>}
    </div>
    <div className="qt2-storage-trend-controls">
      <label>Range <select value={preset} onChange={(event) => choose(event.target.value)}>
        {['6h', '24h', '7d', '30d', 'custom'].map((value) => <option key={value} value={value}>{value === 'custom' ? 'Custom' : value}</option>)}
      </select></label>
      <label><input type="checkbox" checked={query.live} disabled={preset === 'custom'} onChange={toggleLive} /> Live · every 60 seconds</label>
    </div>
    {preset === 'custom' && <form className="qt2-storage-custom-range" onSubmit={applyCustom}>
      <label>From (UTC)<input type="datetime-local" required value={custom.from} onChange={(event) => setCustom({ ...custom, from: event.target.value })} /></label>
      <label>To exclusive (UTC)<input type="datetime-local" required value={custom.to} onChange={(event) => setCustom({ ...custom, to: event.target.value })} /></label>
      <button type="submit" disabled={loading}>Apply range</button>
      <small>Up to 30 days. UTC inputs avoid daylight-saving ambiguity.</small>
    </form>}
    <p className="qt2-storage-muted">Historical charts use UTC. Your local timezone: {localZone()}. Current cards above always show current status.</p>
    {rangeError && <p role="alert" className="qt2-storage-error">{rangeError}</p>}
    {error && <p role="alert" className="qt2-storage-error">Trends refresh failed: {error}{displayed ? ' Last successful range remains displayed.' : ''}</p>}
    {error && !loading && <button type="button" onClick={() => setQuery({ ...query })}>Retry trends</button>}
    {loading && <p role="status">Loading storage trends…{displayed ? ' Previous successful range remains displayed.' : ''}</p>}
    {stale && <p role="status" className="qt2-storage-error">Stale trends response. The displayed response is more than three sampling cadences old.</p>}
    {!displayed && !loading && <p>No historical metrics available.</p>}
    {displayed && <>
      <p className="qt2-storage-sample">Displayed [{utc(displayed.range.from)}, {utc(displayed.range.to)}) · {displayed.range.bucket_seconds / 60} minute buckets · response {utc(displayed.generated_at)}</p>
      <h3>Database footprint</h3>
      <p className="qt2-storage-muted">Allocated bytes at actual observation times when available. Market schema is a subset of the database; lines are not stacked. Net changes include allocation and reclamation.</p>
      <MetricChart payload={displayed} metrics={STOCKS} title="Database footprint" />
      <div className="qt2-storage-trend-summaries">{STOCKS.map((metric) => <MetricCaption key={metric.id} payload={displayed} metric={metric} />)}</div>
      <h3>Lifecycle bytes per bucket</h3>
      <p className="qt2-storage-muted">Separate byte bases and scales. These lanes are not intake or gross ingestion. Missing observations are gaps, never zero.</p>
      {FLOWS.map((metric) => <div className="qt2-storage-trend-lane" key={metric.id}>
        <MetricCaption payload={displayed} metric={metric} />
        <MetricChart payload={displayed} metrics={FLOW_SERIES[metric.id]} bars title={metric.label} />
      </div>)}
      <details className="qt2-storage-trend-data"><summary>Accessible data table · UTC · bytes</summary>
        <div className="qt2-storage-rate-wrap" tabIndex={0} role="region" aria-label="Scrollable storage trends data">
          <table><caption>Bucketed values with actual observation times where recorded. Unavailable values are not zero.</caption>
            <thead><tr><th scope="col">Bucket start (UTC)</th>{METRICS.map((metric) => <th scope="col" key={metric.id}>{metric.label} (bytes)</th>)}</tr></thead>
            <tbody>{rows.map((row) => <tr key={row.time}><th scope="row">{utc(row.time)}</th>{METRICS.map((metric) => <td key={metric.id}>{Number.isFinite(row.values[metric.id]) ? row.values[metric.id].toLocaleString() : 'Unavailable'}{row.observed[metric.id] && <small className="qt2-storage-observed-time">Observed {utc(row.observed[metric.id])}</small>}</td>)}</tr>)}</tbody>
          </table>
          {!rows.length && <p>No recorded points for this range.</p>}
        </div>
      </details>
    </>}
  </section>
}
