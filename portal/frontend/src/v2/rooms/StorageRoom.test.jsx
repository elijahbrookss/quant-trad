import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { StorageRoom } from './StorageRoom.jsx'
import * as api from '../../adapters/storage.adapter.js'

vi.mock('../../adapters/storage.adapter.js', () => ({
  readStorage: vi.fn(), registerStorageTarget: vi.fn(),
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
  candidates: [], plans: [], health: 'available', movement: { state: 'unknown' }, backup: { state: 'unknown' } }

beforeEach(() => {
  api.readStorage.mockResolvedValue(structuredClone(snapshot))
})

describe('Storage settings', () => {
  it('shows unknown movement and backup without inventing success', async () => {
    render(<StorageRoom />)
    expect(await screen.findByText(/Movement: unknown/)).toHaveTextContent('Backup: unknown')
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
  it('displays a failed review and never reports completion', async () => {
    api.reviewStorageChange.mockRejectedValue(new Error('storage_policy_changed'))
    render(<StorageRoom />)
    fireEvent.click(await screen.findByRole('button', { name: 'Review changes' }))
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('storage_policy_changed'))
    expect(api.applyStorageChange).not.toHaveBeenCalled()
  })
})
