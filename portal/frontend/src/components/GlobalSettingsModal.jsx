import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { useEffect, useMemo, useState } from 'react'
import { X } from 'lucide-react'
import { fetchBotSettingsCatalog } from '../adapters/bot.adapter.js'
import { usePortalSettings } from '../contexts/PortalSettingsContext.jsx'
import { useAccentColor } from '../contexts/AccentColorContext.jsx'

export function GlobalSettingsModal({ open, onClose }) {
  const { settings, updateSettings } = usePortalSettings()
  const { setAccentColor } = useAccentColor()
  const [envCatalog, setEnvCatalog] = useState([])
  const [loadError, setLoadError] = useState(null)

  useEffect(() => {
    if (!open) return
    fetchBotSettingsCatalog()
      .then((payload) => {
        setEnvCatalog(Array.isArray(payload?.runtime_env) ? payload.runtime_env : [])
        setLoadError(null)
      })
      .catch((err) => setLoadError(err?.message || 'Unable to load bot settings catalog'))
  }, [open])

  const accentValue = settings?.accentColor || '#175a81'
  const snapshotValue = Number(settings?.botDefaults?.snapshotIntervalMs || 1000)
  const envText = settings?.botDefaults?.envText || ''

  const parsedEnvCount = useMemo(() => {
    return String(envText)
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line && line.includes('=') && !line.startsWith('#')).length
  }, [envText])

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <div className="fixed inset-0 bg-black/70 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="w-full max-w-3xl rounded-2xl border border-slate-800 bg-slate-950 p-6 shadow-2xl">
          <div className="flex items-center justify-between border-b border-slate-800 pb-4">
            <DialogTitle className="text-lg font-semibold text-slate-100">Global Settings</DialogTitle>
            <button type="button" onClick={onClose} className="rounded-md border border-slate-700 p-2 text-slate-400 hover:text-slate-200">
              <X className="size-4" />
            </button>
          </div>

          <div className="mt-5 grid gap-5 md:grid-cols-2">
            <section className="space-y-2 rounded-lg border border-slate-800 bg-slate-900/40 p-4">
              <h4 className="text-sm font-semibold text-slate-100">Portal</h4>
              <label className="text-xs text-slate-400">Accent color</label>
              <input
                type="color"
                value={accentValue}
                onChange={(event) => {
                  updateSettings({ accentColor: event.target.value })
                  setAccentColor(event.target.value)
                }}
                className="h-10 w-full rounded border border-slate-700 bg-slate-900"
              />
              <p className="text-[11px] text-slate-500">Stored in local storage and applied immediately.</p>
            </section>

            <section className="space-y-2 rounded-lg border border-slate-800 bg-slate-900/40 p-4">
              <h4 className="text-sm font-semibold text-slate-100">Bot defaults</h4>
              <label className="text-xs text-slate-400">Default snapshot interval (ms)</label>
              <input
                type="number"
                min={100}
                step={100}
                value={snapshotValue}
                onChange={(event) => updateSettings({ botDefaults: { snapshotIntervalMs: Number(event.target.value) || 1000 } })}
                className="w-full rounded border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100"
              />
              <label className="text-xs text-slate-400">Default bot env overrides ({parsedEnvCount})</label>
              <textarea
                rows={6}
                value={envText}
                onChange={(event) => updateSettings({ botDefaults: { envText: event.target.value } })}
                placeholder={'SNAPSHOT_INTERVAL_MS=1000\nBACKEND_TELEMETRY_WS_URL=ws://backend.quanttrad:8000/api/bots/ws/telemetry/ingest'}
                className="w-full rounded border border-slate-700 bg-slate-950 px-3 py-2 font-mono text-xs text-slate-100"
              />
              <p className="text-[11px] text-slate-500">Applied as defaults in bot creation. Runtime env changes require bot restart.</p>
            </section>
          </div>

          <section className="mt-5 rounded-lg border border-slate-800 bg-slate-900/40 p-4">
            <h4 className="text-sm font-semibold text-slate-100">Runtime env visibility (masked)</h4>
            {loadError ? <p className="mt-2 text-xs text-rose-300">{loadError}</p> : null}
            <div className="mt-2 max-h-52 overflow-auto rounded border border-slate-800 bg-slate-950/70">
              {(envCatalog || []).map((row) => (
                <div key={row.key} className="grid grid-cols-[1.3fr_1fr_auto] items-center gap-2 border-b border-slate-800 px-3 py-2 text-xs last:border-b-0">
                  <span className="font-mono text-slate-300">{row.key}</span>
                  <span className="font-mono text-slate-500">{row.value || '—'}</span>
                  <span className={`rounded px-2 py-0.5 ${row.is_set ? 'bg-emerald-900/40 text-emerald-300' : 'bg-slate-800 text-slate-500'}`}>
                    {row.is_set ? 'set' : 'unset'}
                  </span>
                </div>
              ))}
            </div>
          </section>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
