import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { StorageTrends } from './StorageTrends.jsx'
import { createChart } from 'lightweight-charts'
import { StorageRoom } from './StorageRoom.jsx'
import { METRICS } from './storageTrends.js'
import * as api from '../../adapters/storage.adapter.js'

vi.mock('../../adapters/storage.adapter.js', () => ({
  readStorage: vi.fn(), readStorageMetrics: vi.fn(), registerStorageTarget: vi.fn(),
  reviewStorageChange: vi.fn(), applyStorageChange: vi.fn(),
}))
vi.mock('lightweight-charts', () => ({
  LineSeries: 'line', HistogramSeries: 'histogram',
  createChart: vi.fn(() => ({
    addSeries: vi.fn(() => ({ setData: vi.fn() })),
    timeScale: () => ({ setVisibleRange: vi.fn() }),
    applyOptions: vi.fn(), remove: vi.fn(),
  })),
}))
function metrics(range, overrides = {}) {
  return { schema_version: 'qt.storage_metrics.v1', generated_at: new Date().toISOString(),
    range: { ...range, bucket_seconds: 300 }, cadence_seconds: 300, retention_days: 30,
    series: METRICS.map((metric) => ({
      metric_id: metric.id, unit: 'bytes', basis: metric.basis, state: 'partial', reason: 'sample_gap',
      points: [{ time: range.from, value: metric.kind === 'stock' ? 1024 : null, state: metric.kind === 'stock' ? 'ready' : 'unavailable' }],
      coverage: { complete: false, truncated: false, missing_buckets: 1 },
      summary: { first_at: range.from, last_at: range.to, net_change_bytes: null, average_bytes_per_day: null, total_bytes: null },
    })),
    links: { grafana: { state: 'available', dashboard_url: 'https://metrics.example/d/quanttrad-capacity-growth/quanttrad-capacity-and-database-growth', reason: null } },
    ...overrides }
}
const flush = async () => act(async () => { await Promise.resolve(); await Promise.resolve() })
beforeEach(() => {
  vi.clearAllMocks()
  Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
  api.readStorageMetrics.mockImplementation(async (range) => metrics(range))
})
afterEach(() => { vi.useRealTimers() })

describe('Storage trends', () => {
  it('renders distinct byte bases and accessible UTC data without turning missing samples into zero', async () => {
    render(<StorageTrends />)
    expect(await screen.findByRole('img', { name: /Database footprint/ })).toBeVisible()
    expect(screen.getAllByRole('img')).toHaveLength(4)
    // Captions identify metrics once; disconnected segments must not repeat
    // their names as native price-axis title labels.
    for (const chart of createChart.mock.results.map((result) => result.value)) {
      for (const [, options] of chart.addSeries.mock.calls) expect(options.title || '').toBe('')
    }
    expect(screen.getByText(/Market schema is a subset/)).toBeVisible()
    expect(screen.getByText(/Separate byte bases and scales/)).toBeVisible()
    expect(screen.getAllByText(/1 missing sample bucket/)).toHaveLength(2)
    expect(screen.getAllByText(/1 bucket without completion records/)).toHaveLength(3)
    expect(screen.getByText(/Your local timezone:/)).toBeVisible()
    const details = screen.getByText('Accessible data table · UTC · bytes').closest('details')
    fireEvent.click(details.querySelector('summary'))
    expect(screen.getByRole('region', { name: 'Scrollable storage trends data' })).toHaveAttribute('tabindex', '0')
    expect(screen.getAllByText('Unavailable').length).toBeGreaterThan(0)
    const link = screen.getByRole('link', { name: /Open in Grafana/ })
    expect(link).toHaveAttribute('target', '_blank')
    expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    expect(api.reviewStorageChange).not.toHaveBeenCalled()
    expect(api.applyStorageChange).not.toHaveBeenCalled()
  })
  it('keeps last successfully displayed bounds in Grafana after a changed-range request fails', async () => {
    render(<StorageTrends />)
    const link = await screen.findByRole('link', { name: /Open in Grafana/ })
    const original = link.href
    api.readStorageMetrics.mockRejectedValueOnce(new Error('Metrics temporarily unavailable'))
    fireEvent.change(screen.getByLabelText('Range'), { target: { value: '6h' } })
    expect(await screen.findByRole('alert')).toHaveTextContent('Last successful range remains displayed')
    expect(screen.getByRole('link', { name: /Open in Grafana/ }).href).toBe(original)
    expect(screen.getAllByRole('img')).toHaveLength(4)
  })
  it('accepts bounded custom UTC inputs, freezes on Grafana open, and rejects invalid spans', async () => {
    render(<StorageTrends />)
    await screen.findByRole('link', { name: /Open in Grafana/ })
    fireEvent.change(screen.getByLabelText('Range'), { target: { value: 'custom' } })
    await waitFor(() => expect(screen.getByRole('button', { name: 'Apply range' })).not.toBeDisabled())
    fireEvent.change(screen.getByLabelText('From (UTC)'), { target: { value: '2026-01-01T00:00' } })
    fireEvent.change(screen.getByLabelText('To exclusive (UTC)'), { target: { value: '2026-03-01T00:00' } })
    fireEvent.click(screen.getByRole('button', { name: 'Apply range' }))
    expect(screen.getByRole('alert')).toHaveTextContent('30 days or less')
    fireEvent.change(screen.getByLabelText('From (UTC)'), { target: { value: '2026-01-01T00:00' } })
    fireEvent.change(screen.getByLabelText('To exclusive (UTC)'), { target: { value: '2026-01-02T00:00' } })
    fireEvent.click(screen.getByRole('button', { name: 'Apply range' }))
    await waitFor(() => expect(new URL(screen.getByRole('link', { name: /Open in Grafana/ }).href).searchParams.get('to')).toBe(String(Date.parse('2026-01-02T00:00Z'))))
    expect(screen.getByLabelText(/Live/)).not.toBeChecked()
    expect(api.readStorageMetrics.mock.calls.at(-1)[0]).toMatchObject({
      from: '2026-01-01T00:00:00.000Z', to: '2026-01-02T00:00:00.000Z' })
  })
  it('labels stale response and partial coverage, with no fabricated totals', async () => {
    api.readStorageMetrics.mockImplementation(async (range) => metrics(range, { generated_at: new Date(Date.now() - 901_000).toISOString() }))
    render(<StorageTrends />)
    expect(await screen.findByText(/Stale trends response/)).toBeVisible()
    expect(screen.getAllByText(/Coverage partial/)).toHaveLength(5)
    expect(screen.getAllByText('Recorded total Unavailable')).toHaveLength(3)
  })
  it('does not overlap requests, pauses hidden and aborts superseded or unmounted work', async () => {
    vi.useFakeTimers()
    let active = 0
    let maxActive = 0
    const signals = []
    api.readStorageMetrics.mockImplementation((range, signal) => new Promise((resolve, reject) => {
      active += 1
      maxActive = Math.max(maxActive, active)
      signals.push(signal)
      signal.addEventListener('abort', () => { active -= 1; reject(new DOMException('Aborted', 'AbortError')) }, { once: true })
    }))
    const view = render(<StorageTrends />)
    await flush()
    await act(async () => vi.advanceTimersByTimeAsync(15_000))
    expect(api.readStorageMetrics).toHaveBeenCalledTimes(1)
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' })
    fireEvent(document, new Event('visibilitychange'))
    await flush()
    expect(signals[0].aborted).toBe(true)
    await act(async () => vi.advanceTimersByTimeAsync(120_000))
    expect(api.readStorageMetrics).toHaveBeenCalledTimes(1)
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
    fireEvent(document, new Event('visibilitychange'))
    await act(async () => vi.advanceTimersByTimeAsync(0))
    expect(api.readStorageMetrics).toHaveBeenCalledTimes(2)
    fireEvent.change(screen.getByLabelText('Range'), { target: { value: '7d' } })
    await flush()
    expect(signals[1].aborted).toBe(true)
    expect(maxActive).toBe(1)
    view.unmount()
    expect(signals.at(-1).aborted).toBe(true)
  })
  it('retries an interrupted frozen custom range when the tab becomes visible', async () => {
    vi.useFakeTimers()
    render(<StorageTrends />)
    await flush()
    fireEvent.change(screen.getByLabelText('Range'), { target: { value: 'custom' } })
    await flush()
    let interruptedSignal
    api.readStorageMetrics.mockImplementationOnce((range, signal) => new Promise((resolve, reject) => {
      interruptedSignal = signal
      signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), { once: true })
    }))
    fireEvent.change(screen.getByLabelText('From (UTC)'), { target: { value: '2026-01-01T00:00' } })
    fireEvent.change(screen.getByLabelText('To exclusive (UTC)'), { target: { value: '2026-01-02T00:00' } })
    fireEvent.click(screen.getByRole('button', { name: 'Apply range' }))
    await flush()
    const interruptedRange = api.readStorageMetrics.mock.calls.at(-1)[0]
    const callsBeforeHidden = api.readStorageMetrics.mock.calls.length
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' })
    fireEvent(document, new Event('visibilitychange'))
    await flush()
    expect(interruptedSignal.aborted).toBe(true)
    await act(async () => vi.advanceTimersByTimeAsync(120_000))
    expect(api.readStorageMetrics).toHaveBeenCalledTimes(callsBeforeHidden)
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
    fireEvent(document, new Event('visibilitychange'))
    await flush()
    expect(api.readStorageMetrics).toHaveBeenCalledTimes(callsBeforeHidden + 1)
    expect(api.readStorageMetrics.mock.calls.at(-1)[0]).toEqual(interruptedRange)
    expect(screen.getByLabelText(/Live/)).not.toBeChecked()
    expect(new URL(screen.getByRole('link', { name: /Open in Grafana/ }).href).searchParams.get('to')).toBe(String(Date.parse(interruptedRange.to)))
  })
  it('ages the last response while a later request hangs, then aborts with a retryable timeout', async () => {
    vi.useFakeTimers()
    api.readStorageMetrics.mockImplementationOnce(async (range) => metrics(range, {
      generated_at: new Date(Date.now() - 840_000).toISOString(),
    }))
    render(<StorageTrends />)
    await flush()
    const original = screen.getByRole('link', { name: /Open in Grafana/ }).href
    let hungSignal
    api.readStorageMetrics.mockImplementationOnce((range, signal) => {
      hungSignal = signal
      // A transport that ignores abort must not stop the independent age clock
      // or start overlapping reads.
      return new Promise(() => {})
    })
    await act(async () => vi.advanceTimersByTimeAsync(75_000))
    expect(api.readStorageMetrics).toHaveBeenCalledTimes(2)
    expect(screen.getByText(/Stale trends response/)).toBeVisible()
    expect(hungSignal.aborted).toBe(false)
    await act(async () => vi.advanceTimersByTimeAsync(5_000))
    expect(hungSignal.aborted).toBe(true)
    expect(screen.getByRole('alert')).toHaveTextContent('timed out')
    expect(screen.getByRole('button', { name: 'Retry trends' })).toBeEnabled()
    expect(screen.getByRole('link', { name: /Open in Grafana/ }).href).toBe(original)
    expect(screen.getAllByRole('img')).toHaveLength(4)
    await act(async () => vi.advanceTimersByTimeAsync(60_000))
    expect(api.readStorageMetrics).toHaveBeenCalledTimes(2)
  })
  it('serializes current-status polling and rejects a superseded old response without resetting a dirty draft', async () => {
    vi.useFakeTimers()
    const policy = { recent: ['ssd'], history: ['ssd'], archives: ['ssd'], backups: ['ssd'],
      recent_days: 30, reserve_percent: 20, backup_interval_hours: 24, backup_copies: 2,
      movement_enabled: false, backup_enabled: false }
    const current = { revision: 5, policy, targets: [], candidates: [], health: 'available',
      movement: { state: 'ready' }, backup: { state: 'unknown' } }
    let finishOld
    let oldSignal
    api.readStorage.mockImplementationOnce((signal) => {
      oldSignal = signal
      return new Promise((resolve) => { finishOld = resolve })
    }).mockResolvedValue(current)
    const view = render(<StorageRoom />)
    await act(async () => vi.advanceTimersByTimeAsync(30_000))
    expect(api.readStorage).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('button', { name: 'Refresh storage' }))
    await flush()
    expect(oldSignal.aborted).toBe(true)
    expect(api.readStorage).toHaveBeenCalledTimes(2)
    fireEvent.click(screen.getByText('Advanced'))
    fireEvent.change(screen.getByLabelText('Free space reserve (%)'), { target: { value: '25' } })
    await act(async () => finishOld({ ...current, revision: 1, movement: { state: 'old' } }))
    expect(screen.getByText(/Movement: ready/)).toBeVisible()
    expect(screen.queryByText(/Movement: old/)).not.toBeInTheDocument()
    expect(screen.getByLabelText('Free space reserve (%)')).toHaveValue(25)
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'hidden' })
    fireEvent(document, new Event('visibilitychange'))
    await act(async () => vi.advanceTimersByTimeAsync(60_000))
    expect(api.readStorage).toHaveBeenCalledTimes(2)
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
    fireEvent(document, new Event('visibilitychange'))
    await flush()
    expect(api.readStorage).toHaveBeenCalledTimes(3)
    expect(screen.getByLabelText('Free space reserve (%)')).toHaveValue(25)
    const latestSignal = api.readStorage.mock.calls.at(-1)[0]
    view.unmount()
    // Completed reads need no cancellation; unmount clears every active owner.
    expect(latestSignal).toBeInstanceOf(AbortSignal)
    expect(api.reviewStorageChange).not.toHaveBeenCalled()
    expect(api.applyStorageChange).not.toHaveBeenCalled()
  })
  it('keeps an unsaved policy draft during metrics range changes and status refreshes, without POST', async () => {
    vi.useFakeTimers()
    const policy = { recent: ['ssd'], history: ['ssd'], archives: ['ssd'], backups: ['ssd'],
      recent_days: 30, reserve_percent: 20, backup_interval_hours: 24, backup_copies: 2,
      movement_enabled: false, backup_enabled: false }
    api.readStorage.mockResolvedValue({ revision: 1, policy, targets: [], candidates: [],
      health: 'available', movement: { state: 'unknown' }, backup: { state: 'unknown' } })
    render(<StorageRoom />)
    await flush()
    fireEvent.click(screen.getByText('Advanced'))
    fireEvent.change(screen.getByLabelText('Free space reserve (%)'), { target: { value: '25' } })
    fireEvent.change(screen.getByLabelText('Range'), { target: { value: '6h' } })
    await act(async () => vi.advanceTimersByTimeAsync(60_000))
    await flush()
    expect(screen.getByLabelText('Free space reserve (%)')).toHaveValue(25)
    expect(api.readStorage.mock.calls.length).toBeGreaterThan(1)
    expect(api.readStorageMetrics.mock.calls.length).toBeGreaterThan(1)
    expect(api.reviewStorageChange).not.toHaveBeenCalled()
    expect(api.applyStorageChange).not.toHaveBeenCalled()
    expect(api.registerStorageTarget).not.toHaveBeenCalled()
  })
})
