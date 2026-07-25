import { Dialog, DialogPanel, DialogTitle } from '@headlessui/react'
import { Check, Edit3, LoaderCircle, Lock, Save, X } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'

import {
  EDITABLE_FIELD_KEYS,
  JSON_EDIT_FIELDS,
  buildBotConfigEditForm,
  buildBotConfigModel,
  buildBotConfigUpdatePayload,
} from './botConfigModel.js'

const RUN_TYPE_OPTIONS = [
  { value: 'backtest', label: 'Backtest' },
  { value: 'sim_trade', label: 'Sim Trade' },
  { value: 'paper', label: 'Paper' },
  { value: 'live', label: 'Live' },
]

const PLAYBACK_OPTIONS = [
  { value: 'instant', label: 'Instant' },
  { value: 'walk-forward', label: 'Walk Forward' },
]

const EXECUTION_MODE_OPTIONS = [
  { value: 'fast', label: 'Fast' },
  { value: 'full', label: 'Full' },
]

const EXECUTION_BEHAVIOR_OPTIONS = [
  { value: 'simulated', label: 'Simulated' },
  { value: 'observe-only', label: 'Observe Only' },
]

const EXECUTION_SEMANTICS_OPTIONS = [
  { value: '', label: 'Infer' },
  { value: 'spot', label: 'Spot' },
  { value: 'derivative', label: 'Derivative' },
  { value: 'proxy_derivative', label: 'Proxy Derivative' },
]

function fieldLabel(key) {
  return String(key || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase())
}

function EditField({ id, label, value, type = 'text', options = null, rows = null, onChange }) {
  const baseClass = rows
    ? 'qt-mono min-h-28 resize-y rounded-[3px] border border-white/10 bg-black/28 px-3 py-2 text-[12px] leading-5 text-slate-200 outline-none transition focus:border-[color:var(--accent-alpha-50)]'
    : 'h-9 rounded-[3px] border border-white/10 bg-black/28 px-3 text-sm text-slate-200 outline-none transition focus:border-[color:var(--accent-alpha-50)]'

  return (
    <label className="grid min-w-0 gap-1.5">
      <span className="qt-ops-kicker">{label}</span>
      {options ? (
        <select
          id={id}
          value={value}
          onChange={(event) => onChange(id, event.target.value)}
          className={baseClass}
        >
          {options.map((option) => (
            <option key={option.value} value={option.value}>{option.label}</option>
          ))}
        </select>
      ) : rows ? (
        <textarea
          id={id}
          value={value}
          rows={rows}
          spellCheck={false}
          onChange={(event) => onChange(id, event.target.value)}
          className={baseClass}
        />
      ) : (
        <input
          id={id}
          type={type}
          value={value}
          onChange={(event) => onChange(id, event.target.value)}
          className={baseClass}
        />
      )}
    </label>
  )
}

function BotConfigEditForm({ form, onChange }) {
  return (
    <div className="grid gap-5">
      <section className="grid gap-3 md:grid-cols-2">
        <EditField id="name" label="Name" value={form.name} onChange={onChange} />
        <EditField id="run_type" label="Run Type" value={form.run_type} options={RUN_TYPE_OPTIONS} onChange={onChange} />
        <EditField id="mode" label="Playback" value={form.mode} options={PLAYBACK_OPTIONS} onChange={onChange} />
        <EditField id="execution_mode" label="Execution Mode" value={form.execution_mode} options={EXECUTION_MODE_OPTIONS} onChange={onChange} />
        <EditField id="execution_behavior" label="Execution Behavior" value={form.execution_behavior} options={EXECUTION_BEHAVIOR_OPTIONS} onChange={onChange} />
        <EditField id="execution_semantics" label="Execution Semantics" value={form.execution_semantics} options={EXECUTION_SEMANTICS_OPTIONS} onChange={onChange} />
        <EditField id="atm_template_id" label="ATM Template" value={form.atm_template_id} onChange={onChange} />
        <EditField id="focus_symbol" label="Focus Symbol" value={form.focus_symbol} onChange={onChange} />
        <EditField id="backtest_start" label="Backtest Start" value={form.backtest_start} onChange={onChange} />
        <EditField id="backtest_end" label="Backtest End" value={form.backtest_end} onChange={onChange} />
        <EditField id="snapshot_interval_ms" label="Snapshot Interval Ms" type="number" value={form.snapshot_interval_ms} onChange={onChange} />
        <EditField id="playback_speed" label="Playback Speed" type="number" value={form.playback_speed} onChange={onChange} />
      </section>

      <section className="grid gap-3 lg:grid-cols-3">
        {JSON_EDIT_FIELDS.map((field) => (
          <EditField
            key={field}
            id={field}
            label={fieldLabel(field)}
            rows={12}
            value={form[field]}
            onChange={onChange}
          />
        ))}
      </section>
    </div>
  )
}

function ConfigRow({ row }) {
  return (
    <div className="min-w-0 border-t border-white/6 py-2 first:border-t-0">
      <div className="grid min-w-0 gap-1 sm:grid-cols-[10rem_minmax(0,1fr)] sm:gap-3">
        <dt className="qt-ops-kicker min-w-0">{row.label}</dt>
        <dd className="min-w-0">
          <div className="flex min-w-0 items-center gap-2">
            <span
              title={row.detail || row.value}
              className={`min-w-0 truncate text-sm ${row.mono ? 'qt-mono tabular-nums text-slate-300' : 'text-slate-100'}`}
            >
              {row.value}
            </span>
            {row.masked ? <Lock className="size-3 shrink-0 text-slate-600" /> : null}
          </div>
          {row.detail ? (
            <p className="mt-0.5 truncate text-[11px] text-slate-500">{row.detail}</p>
          ) : null}
          {row.jsonValue ? (
            <details className="mt-1.5">
              <summary className="qt-mono cursor-pointer text-[10px] uppercase tracking-[0.14em] text-slate-500 transition hover:text-slate-300">
                JSON
              </summary>
              <pre className="qt-mono mt-2 max-h-44 overflow-auto rounded-[3px] border border-white/8 bg-black/30 p-2 text-[11px] leading-4 text-slate-300">
                {row.jsonValue}
              </pre>
            </details>
          ) : null}
        </dd>
      </div>
    </div>
  )
}

function ConfigSection({ section }) {
  const rows = Array.isArray(section.rows) ? section.rows : []

  return (
    <section className="border-t border-white/8 pt-3 first:border-t-0 first:pt-0">
      <div className="mb-2 flex items-center justify-between gap-3">
        <h3 className="text-sm font-semibold text-slate-100">{section.title}</h3>
        <span className="qt-mono text-[10px] uppercase tracking-[0.14em] text-slate-600">{rows.length}</span>
      </div>
      {rows.length ? (
        <dl>
          {rows.map((row) => (
            <ConfigRow key={row.key} row={row} />
          ))}
        </dl>
      ) : (
        <p className="py-2 text-sm text-slate-500">{section.emptyLabel || 'No config values'}</p>
      )}
    </section>
  )
}

function ReadOnlyConfig({ sections }) {
  return (
    <div className="grid gap-5 lg:grid-cols-2">
      {sections.map((section) => (
        <ConfigSection key={section.key} section={section} />
      ))}
    </div>
  )
}

export function BotConfigModal({
  active = false,
  bot,
  canUpdate = false,
  onClose,
  onSave,
  open,
  saving = false,
  strategy = null,
}) {
  const [editing, setEditing] = useState(false)
  const [form, setForm] = useState(() => buildBotConfigEditForm(bot))
  const [error, setError] = useState(null)

  const model = useMemo(
    () => buildBotConfigModel(bot, { strategy, canUpdate, active }),
    [active, bot, canUpdate, strategy],
  )

  useEffect(() => {
    if (!open || !bot) return
    setEditing(false)
    setForm(buildBotConfigEditForm(bot))
    setError(null)
  }, [bot, open])

  useEffect(() => {
    if (!model.canEdit && editing) setEditing(false)
  }, [editing, model.canEdit])

  if (!open || !bot) return null

  function handleFieldChange(field, value) {
    if (!EDITABLE_FIELD_KEYS.has(field)) return
    setForm((current) => ({ ...current, [field]: value }))
  }

  async function handleSave() {
    setError(null)
    let payload
    try {
      payload = buildBotConfigUpdatePayload(form)
    } catch (err) {
      setError(err?.message || 'Invalid bot config')
      return
    }
    try {
      await onSave?.(bot.id, payload)
      setEditing(false)
    } catch (err) {
      setError(err?.message || 'Unable to update bot config')
    }
  }

  return (
    <Dialog open={open} onClose={onClose} className="relative z-[82]">
      <div className="fixed inset-0 bg-black/80 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="flex max-h-[calc(100vh-2rem)] w-full max-w-5xl flex-col overflow-hidden rounded-lg border border-white/[0.06] bg-[#0b1019]/96 shadow-[0_30px_80px_rgba(0,0,0,0.45)]">
          <div className="flex items-start justify-between gap-4 border-b border-white/[0.06] px-5 py-3">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <DialogTitle className="min-w-0 truncate text-lg font-semibold text-slate-50">
                  {model.title}
                </DialogTitle>
                <span className="qt-mono inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-white/[0.04] px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-300">
                  {model.canEdit ? <Check className="size-3 text-emerald-300" /> : <Lock className="size-3 text-slate-500" />}
                  {model.modeLabel}
                </span>
              </div>
              <p className="mt-1 truncate text-sm text-slate-400">{model.subtitle}</p>
            </div>

            <div className="flex shrink-0 items-center gap-2">
              {model.canEdit ? (
                <button
                  type="button"
                  onClick={() => {
                    setEditing((value) => !value)
                    setError(null)
                  }}
                  className="qt-mono inline-flex h-9 items-center gap-1.5 rounded-[3px] border border-white/[0.08] bg-black/30 px-3 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-300 transition hover:border-white/[0.14] hover:bg-black/45 hover:text-slate-100"
                >
                  <Edit3 className="size-3.5" />
                  {editing ? 'View' : 'Edit'}
                </button>
              ) : null}
              <button
                type="button"
                onClick={onClose}
                className="inline-flex h-9 w-9 items-center justify-center rounded-[3px] border border-white/[0.08] bg-black/30 text-slate-400 transition hover:border-white/[0.14] hover:bg-black/45 hover:text-slate-200"
                aria-label="Close"
                title="Close"
              >
                <X className="size-4" />
              </button>
            </div>
          </div>

          <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4">
            {error ? (
              <div className="mb-4 rounded-[3px] border border-rose-900/60 bg-rose-950/25 px-3 py-2 text-sm text-rose-200">
                {error}
              </div>
            ) : null}
            {editing ? (
              <BotConfigEditForm form={form} onChange={handleFieldChange} />
            ) : (
              <ReadOnlyConfig sections={model.sections} />
            )}
          </div>

          {editing ? (
            <div className="flex items-center justify-end gap-2 border-t border-white/[0.06] px-5 py-3">
              <button
                type="button"
                onClick={() => {
                  setForm(buildBotConfigEditForm(bot))
                  setEditing(false)
                  setError(null)
                }}
                className="qt-mono inline-flex h-9 items-center rounded-[3px] border border-white/[0.08] bg-black/25 px-3 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-300 transition hover:border-white/[0.14] hover:bg-black/40 hover:text-slate-100"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleSave}
                disabled={saving}
                className="qt-mono inline-flex h-9 items-center gap-1.5 rounded-[3px] border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-12)] px-3 text-[10px] font-semibold uppercase tracking-[0.14em] text-[color:var(--accent-text-strong)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-20)] disabled:cursor-not-allowed disabled:opacity-50"
              >
                {saving ? <LoaderCircle className="size-3.5 animate-spin" /> : <Save className="size-3.5" />}
                Save
              </button>
            </div>
          ) : null}
        </DialogPanel>
      </div>
    </Dialog>
  )
}
