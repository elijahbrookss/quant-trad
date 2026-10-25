import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { StorageTrends } from './StorageTrends.jsx'
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
    expect(screen.getByText(/Market schema is a subset/)).toBeVisible()
    expect(screen.getByText(/Separate byte bases and scales/)).toBeVisible()
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
    await act(async () => vi.advanceTimersByTimeAsync(120_000))
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
