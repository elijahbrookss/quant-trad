import { useCallback, useEffect, useRef, useState } from 'react'
import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { HardDrive, Plus, RefreshCw } from 'lucide-react'
import { applyStorageChange, readStorage, registerStorageTarget, reviewStorageChange } from '../../adapters/storage.adapter.js'
import './storage.css'

const ROLES = [
  ['recent', 'Recent'], ['history', 'History'], ['archives', 'Archives'], ['backups', 'Backups'],
]
const DEFAULT_POLICY = {
  schema_version: 'qt.storage_policy.v1',
  recent: [], history: [], archives: [], backups: [],
  recent_days: 30, reserve_percent: 20, backup_interval_hours: 24, backup_copies: 2,
  movement_enabled: false, backup_enabled: false,
}

function bytes(value) {
  if (!Number.isFinite(value)) return 'Unknown'
  const unit = value >= 1024 ** 4 ? 'TiB' : 'GiB'
  return `${(value / 1024 ** (unit === 'TiB' ? 4 : 3)).toFixed(1)} ${unit}`
}

function words(value) {
  return String(value || 'unknown').replaceAll('_', ' ')
}

export function StorageRoom() {
  const [snapshot, setSnapshot] = useState(null)
  const [draft, setDraft] = useState(null)
  const [draftRevision, setDraftRevision] = useState(0)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [adding, setAdding] = useState(false)
  const [plan, setPlan] = useState(null)
  const [notice, setNotice] = useState('')
  const dirty = useRef(false)

  const refresh = useCallback(async (signal) => {
    const next = await readStorage(signal)
    if (signal?.aborted) return
    setSnapshot(next)
    if (!dirty.current) {
      setDraft(next.policy || { ...DEFAULT_POLICY })
      setDraftRevision(next.revision)
    }
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    const load = () => refresh(controller.signal).catch((err) => {
      if (!controller.signal.aborted) setError(err.message)
    })
    load()
    const timer = setInterval(load, 15_000)
    return () => { controller.abort(); clearInterval(timer) }
  }, [refresh])

  function edit(key, value) {
    dirty.current = true
    setDraft((current) => ({ ...current, [key]: value }))
    setPlan(null)
    setNotice('')
  }

  async function action(fn) {
    setBusy(true)
    setError('')
    try { await fn() } catch (err) { setError(err.message) } finally { setBusy(false) }
  }

  function choose(role, targetId, checked) {
    const current = draft[role]
    edit(role, role === 'recent' ? [targetId] : checked
      ? [...current, targetId] : current.filter((id) => id !== targetId))
  }

  const targets = snapshot?.targets || []
  const label = (id) => targets.find((target) => target.target_id === id)?.label || id
  const canReview = draft && ROLES.every(([role]) => draft[role].length > 0)
  const active = snapshot?.active_plan

  return (
    <section className="qt2-storage" aria-labelledby="storage-title">
      <header className="qt2-storage-heading">
        <div><span className="qt2-storage-eyebrow">Settings</span><h1 id="storage-title">Storage</h1></div>
        <button type="button" disabled={busy} onClick={() => action(() => refresh())} aria-label="Refresh storage"><RefreshCw size={16} /></button>
      </header>

      {error && <div role="alert" className="qt2-storage-error">{error}</div>}
      {notice && <p role="status">{notice}</p>}
      {!snapshot ? <p role="status">{error ? 'Storage status unavailable.' : 'Loading storage…'}</p> : <>
        <div className="qt2-storage-drives" aria-label="Registered drives">
          {!targets.length && <p>No drives enrolled.</p>}
          {targets.map((target) => {
            const total = target.capacity?.total_bytes
            const available = target.capacity?.available_bytes
            const used = Number.isFinite(total) && Number.isFinite(available) ? Math.max(0, total - available) : null
            return <div className="qt2-storage-drive" key={target.target_id}>
              <HardDrive size={20} aria-hidden="true" />
              <div><strong>{target.label}</strong><small>{target.medium.toUpperCase()} · {words(target.status)}</small></div>
              <div className="qt2-storage-capacity">
                {used === null ? <span>Capacity unavailable</span> : <>
                  <meter min="0" max={total} value={used} aria-label={`${target.label} drive usage`} />
                  <small>{bytes(available)} available / {bytes(total)}</small>
                </>}
              </div>
            </div>
          })}
        </div>
        <button type="button" disabled={busy} onClick={() => setAdding(true)}><Plus size={16} /> Add drive</button>

        {draft && <div className="qt2-storage-assignments">
          {ROLES.map(([role, title]) => <fieldset key={role} disabled={busy}>
            <legend>{title}</legend>
            <div className="qt2-storage-options">
              {targets.filter((target) => target.roles.includes(role)).map((target) => <label key={target.target_id}>
                <input type={role === 'recent' ? 'radio' : 'checkbox'} name={role}
                  checked={draft[role].includes(target.target_id)}
                  disabled={target.state !== 'active' || target.status !== 'available'}
                  onChange={(event) => choose(role, target.target_id, event.target.checked)} />
                {target.label}
              </label>)}
              {!targets.some((target) => target.roles.includes(role)) && <span className="qt2-storage-muted">No eligible drive</span>}
            </div>
          </fieldset>)}
        </div>}

        <p className="qt2-storage-health" role="status">
          Storage: {words(snapshot.health)} · Movement: {words(snapshot.movement?.state)} · Backup: {snapshot.backup?.last_completed_at
            ? new Date(snapshot.backup.last_completed_at).toLocaleString() : words(snapshot.backup?.state)}
        </p>
        {active && <p role="status">Change {words(active.state)}{active.progress?.detail ? `: ${active.progress.detail}` : ''}</p>}

        {draft && <details className="qt2-storage-advanced">
          <summary>Advanced</summary>
          <div>
            {[
              ['recent_days', 'Recent window (days)', 1, 3650],
              ['reserve_percent', 'Free space reserve (%)', 10, 80],
              ['backup_interval_hours', 'Backup interval (hours)', 1, 168],
              ['backup_copies', 'Recovery copies', 1, 30],
            ].map(([key, title, min, max]) => <label key={key}>{title}
              <input type="number" min={min} max={max} step="1" value={draft[key]} disabled={busy}
                onChange={(event) => edit(key, event.target.value === '' ? '' : Number(event.target.value))} />
            </label>)}
            <label><input type="checkbox" checked={draft.movement_enabled} disabled={busy}
              onChange={(event) => edit('movement_enabled', event.target.checked)} /> Automatic movement</label>
            <label><input type="checkbox" checked={draft.backup_enabled} disabled={busy}
              onChange={(event) => edit('backup_enabled', event.target.checked)} /> Scheduled recovery copies</label>
          </div>
        </details>}
        <footer><button type="button" className="qt2-storage-primary" disabled={busy || !canReview || !!active}
          onClick={() => action(async () => {
            const reviewed = await reviewStorageChange(draft, draftRevision, crypto.randomUUID())
            setPlan(reviewed)
          })}>{busy ? 'Working…' : 'Review changes'}</button></footer>
      </>}

      <Dialog open={adding} onClose={() => !busy && setAdding(false)} className="qt2-storage-dialog">
        <div className="qt2-storage-backdrop" aria-hidden="true" />
        <div className="qt2-storage-dialog-position"><DialogPanel>
          <DialogTitle>Add drive</DialogTitle>
          {error && <p role="alert">{error}</p>}
          {!snapshot?.candidates?.length && <p>No prepared drives found.</p>}
          {snapshot?.candidates?.map((target) => <div className="qt2-storage-candidate" key={target.target_id}>
            <span><strong>{target.label}</strong><small>{target.medium.toUpperCase()} · {words(target.status)}</small></span>
            <button type="button" disabled={busy || target.status !== 'available'} onClick={() => action(async () => {
              await registerStorageTarget(target.target_id); await refresh(); setAdding(false)
            })}>Enroll</button>
          </div>)}
          <button type="button" disabled={busy} onClick={() => setAdding(false)}>Close</button>
        </DialogPanel></div>
      </Dialog>

      <Dialog open={!!plan} onClose={() => !busy && setPlan(null)} className="qt2-storage-dialog">
        <div className="qt2-storage-backdrop" aria-hidden="true" />
        <div className="qt2-storage-dialog-position"><DialogPanel>
          <DialogTitle>Review storage changes</DialogTitle>
          {error && <p role="alert">{error}</p>}
          {plan?.impact.changes.map((change) => <p key={change.role}>
            <strong>{words(change.role)}</strong>: {change.before.map(label).join(', ') || 'Unassigned'} → {change.after.map(label).join(', ')}
          </p>)}
          {plan?.impact.setting_changes?.map((change) => <p key={change.setting}>{words(change.setting)}: {String(change.before ?? 'Default')} → {String(change.after)}</p>)}
          {plan?.impact.requires_migration && <p>Data movement required. Duration: {plan.impact.estimated_seconds === null ? 'not measured' : `${Math.ceil(plan.impact.estimated_seconds / 60)} minutes`}.</p>}
          {plan?.impact.warnings.map((warning) => <p key={warning}>{warning}</p>)}
          {plan?.impact.blockers.map((blocker, index) => <p role="alert" key={`${blocker.code}-${index}`}>{blocker.detail}</p>)}
          <div className="qt2-storage-dialog-actions">
            <button type="button" disabled={busy} onClick={() => setPlan(null)}>Back</button>
            <button type="button" className="qt2-storage-primary" disabled={busy || !!plan?.impact.blockers.length}
              onClick={() => action(async () => {
                const result = await applyStorageChange(plan)
                setNotice(`Storage change ${words(result.state)}.`)
                dirty.current = false; setPlan(null); await refresh()
              })}>Apply changes</button>
          </div>
        </DialogPanel></div>
      </Dialog>
    </section>
  )
}
