import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { StorageRoom } from './StorageRoom.jsx'
import * as api from '../../adapters/storage.adapter.js'

vi.mock('../../adapters/storage.adapter.js', () => ({
  readStorage: vi.fn(), readStorageMetrics: vi.fn(), registerStorageTarget: vi.fn(),
  reviewStorageChange: vi.fn(), applyStorageChange: vi.fn(),
}))

const policy = {
  recent: ['ssd'], history: ['hdd'], archives: ['hdd'], backups: ['hdd'],
  recent_days: 30, reserve_percent: 20, backup_interval_hours: 24, backup_copies: 2,
  movement_enabled: false, backup_enabled: false,
}
const target = (id, medium) => ({ target_id: id, label: id.toUpperCase(), medium, roles: ['recent', 'history', 'archives', 'backups'],
  state: 'active', status: 'available', capacity: { total_bytes: 1000, available_bytes: 800 } })
const snapshot = { revision: 3, policy, targets: [target('ssd', 'ssd'), target('hdd', 'hdd')],
  candidates: [], plans: [], health: 'available', movement: { state: 'unknown' }, backup: { state: 'unknown' },
  retention: { state: 'ready', sampled_at: '2026-10-19T18:00:00Z', source_revision: 'a'.repeat(40),
    policy: { fingerprint: 'b'.repeat(64) }, completeness: { complete: true, truncated: false, unknowns: [] },
    lifecycle: { current: { state: 'running', name: 'canonical_retention', progress: {
      stage: 'executing', storage_day: '2026-09-02', action: 'verify_page' } },
      last_durable_advancement: { stage: 'archive_page_committed', storage_day: '2026-09-02', happened_at: '2026-10-19T17:59:00Z' } },
    backlog: { eligible_source_bytes: 3000, eligible_partitions: 3, oldest_eligible_age_days: 48,
      verified_guarded_source_bytes: 1000, verified_guarded_partitions: 1 },
    storage: { physical_hot: { source_bytes: 5000, estimated_rows: 100 },
      compressed_archive: { bytes: 2000, verified_bytes: 1500 }, filesystem_delta: { state: 'unavailable' },
      permanent_metadata_growth: { state: 'unavailable' } },
    rates: { windows: [{ name: '6h', intake: { estimated_source_bytes: 20 }, archive: { compressed_bytes: 10 },
      verify: { compressed_bytes: 8 }, reclaim: { physical_hot_bytes: 7 } }],
      eta: { state: 'unavailable', reason: 'aligned_physical_intake_and_reclaim_series_required' } },
    recovery_floor: { state: 'above_floor', available_bytes: 800, margin_bytes: 100 } } }

beforeEach(() => {
  api.readStorage.mockResolvedValue(structuredClone(snapshot))
  api.readStorageMetrics.mockRejectedValue(new Error('Metrics unavailable'))
})

describe('Storage settings', () => {
  it('shows unknown movement and backup without inventing success', async () => {
    render(<StorageRoom />)
    expect(await screen.findByText(/Movement: unknown/)).toHaveTextContent('Backup: unknown')
    expect(screen.getByText('Advanced').closest('details')).not.toHaveAttribute('open')
  })
  it('shows bounded lifecycle stage, durable progress, domains, and no invented ETA', async () => {
    render(<StorageRoom />)
    expect(await screen.findByRole('heading', { name: 'Storage lifecycle' })).toBeVisible()
    expect(screen.getByText('canonical retention')).toBeVisible()
    expect(screen.getByText(/Day 2026-09-02/)).toHaveTextContent('verify page')
    expect(screen.getByText('archive page committed')).toBeVisible()
    expect(screen.getByRole('heading', { name: 'Storage trends' })).toBeVisible()
    expect(screen.queryByText('Distinct lifecycle bytes by exact rolling window')).not.toBeInTheDocument()
    expect(screen.queryByText(/Drain ETA:/)).not.toBeInTheDocument()
    expect(screen.getByText(/Current storage and lifecycle status/)).toBeVisible()
  })
  it('keeps a current backup failure visible alongside an older copy date', async () => {
    api.readStorage.mockResolvedValue({ ...snapshot, health: 'needs_attention',
      movement: { state: 'stale' },
      backup: { state: 'failed', last_completed_at: '2026-09-17T12:00:00Z' } })
    render(<StorageRoom />)
    const health = await screen.findByText(/Movement: stale/)
    expect(health).toHaveTextContent('Backup: failed')
    expect(health).toHaveTextContent('Last copy:')
    expect(screen.getByText('Advanced').closest('details')).not.toHaveAttribute('open')
  })
  it('shows missing capacity instead of an empty healthy usage bar', async () => {
    api.readStorage.mockResolvedValue({ ...snapshot, targets: [{ ...target('hdd', 'hdd'), status: 'unavailable', capacity: null }] })
    render(<StorageRoom />)
    expect(await screen.findByText('Capacity unavailable')).toBeVisible()
    expect(screen.queryByRole('meter')).not.toBeInTheDocument()
  })
  it('reviews with the server revision and does not apply a blocked plan', async () => {
    api.reviewStorageChange.mockResolvedValue({ id: 'review-1', policy_hash: 'hash', impact: {
      changes: [], warnings: [], blockers: [{ code: 'rehearsal', detail: 'Migration rehearsal required.' }],
      requires_migration: true, estimated_seconds: null,
    } })
    render(<StorageRoom />)
    fireEvent.click(await screen.findByRole('button', { name: 'Review changes' }))
    expect(await screen.findByRole('dialog')).toBeVisible()
    expect(api.reviewStorageChange).toHaveBeenCalledWith(policy, 3, expect.any(String))
    expect(screen.getByRole('button', { name: 'Apply changes' })).toBeDisabled()
    expect(api.applyStorageChange).not.toHaveBeenCalled()
  })
  it('reports saved settings without claiming physical movement completed', async () => {
    api.reviewStorageChange.mockResolvedValue({ id: 'settings-1', policy_hash: 'hash',
      impact: { changes: [], warnings: [], blockers: [] } })
    api.applyStorageChange.mockResolvedValue({ state: 'completed',
      progress: { operation: 'qt.storage_policy_settings.v1', physical_data_moved: false } })
    render(<StorageRoom />)
    fireEvent.click(await screen.findByRole('button', { name: 'Review changes' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Apply changes' }))
    expect(await screen.findByText('Storage settings saved. Check movement and recovery status below.')).toBeVisible()
    expect(screen.getByText(/Movement: unknown/)).toHaveTextContent('Backup: unknown')
    expect(screen.queryByText('Storage change completed.')).not.toBeInTheDocument()
  })
  it('displays a failed review and never reports completion', async () => {
    api.reviewStorageChange.mockRejectedValue(new Error('storage_policy_changed'))
    render(<StorageRoom />)
    fireEvent.click(await screen.findByRole('button', { name: 'Review changes' }))
    await waitFor(() => expect(screen.getByText('storage_policy_changed')).toBeVisible())
    expect(api.applyStorageChange).not.toHaveBeenCalled()
  })
})
