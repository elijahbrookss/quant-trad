import { useEffect, useMemo, useState } from 'react'

import ActionButton from '../ui/ActionButton.jsx'

const createRow = (key = '', value = '') => ({
  id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
  key,
  value,
})

const stringifyValue = (value) => {
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  if (value === null || value === undefined) return value === null ? 'null' : ''
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

const parseValue = (raw) => {
  const text = String(raw ?? '').trim()
  if (text === '') return ''
  if (text === 'true') return true
  if (text === 'false') return false
  if (text === 'null') return null
  if (/^-?\d+(\.\d+)?$/.test(text)) return Number(text)
  if (text.startsWith('{') || text.startsWith('[')) {
    return JSON.parse(text)
  }
  return text
}

function VariantFormModal({ open, initialValues, onSubmit, onCancel, submitting, error }) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [isDefault, setIsDefault] = useState(false)
  const [rows, setRows] = useState([createRow()])
  const [localError, setLocalError] = useState(null)

  useEffect(() => {
    if (!open) return
    setName(initialValues?.name || '')
    setDescription(initialValues?.description || '')
    setIsDefault(Boolean(initialValues?.is_default))

    const overrides = initialValues?.param_overrides
    const nextRows = overrides && typeof overrides === 'object'
      ? Object.entries(overrides).map(([key, value]) => createRow(key, stringifyValue(value)))
      : [createRow()]

    setRows(nextRows.length ? nextRows : [createRow()])
    setLocalError(null)
  }, [open, initialValues])

  const errorMessage = localError || error
  const title = initialValues?.id ? 'Edit variant' : 'Create variant'
  const actionLabel = submitting ? 'Saving…' : (initialValues?.id ? 'Save changes' : 'Create variant')

  const hasDuplicateKeys = useMemo(() => {
    const seen = new Set()
    for (const row of rows) {
      const key = String(row.key || '').trim()
      if (!key) continue
      if (seen.has(key)) return true
      seen.add(key)
    }
    return false
  }, [rows])

  if (!open) return null

  const handleRowChange = (rowId, field) => (event) => {
    const nextValue = event.target.value
    setRows((current) => current.map((row) => (row.id === rowId ? { ...row, [field]: nextValue } : row)))
  }

  const handleAddRow = () => {
    setRows((current) => [...current, createRow()])
  }

  const handleRemoveRow = (rowId) => {
    setRows((current) => {
      const next = current.filter((row) => row.id !== rowId)
      return next.length ? next : [createRow()]
    })
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    const trimmedName = String(name || '').trim()
    if (!trimmedName) {
      setLocalError('Variant name is required.')
      return
    }
    if (hasDuplicateKeys) {
      setLocalError('Parameter override keys must be unique.')
      return
    }

    const paramOverrides = {}
    try {
      for (const row of rows) {
        const key = String(row.key || '').trim()
        if (!key) continue
        paramOverrides[key] = parseValue(row.value)
      }
    } catch (parseErr) {
      setLocalError(parseErr?.message || 'Failed to parse parameter override value.')
      return
    }

    setLocalError(null)
    await onSubmit?.({
      name: trimmedName,
      description: String(description || '').trim() || null,
      param_overrides: paramOverrides,
      is_default: isDefault,
    })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="w-full max-w-2xl space-y-6 rounded-2xl border border-white/10 bg-[#14171f] p-6 text-slate-100 shadow-xl">
        <header className="space-y-1">
          <h3 className="text-lg font-semibold">{title}</h3>
          <p className="text-sm text-slate-400">
            Save a named preset of parameter overrides for this strategy.
          </p>
        </header>

        <form className="space-y-5" onSubmit={handleSubmit}>
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Name</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="e.g. Aggressive"
                required
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">Description</label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={description}
                onChange={(event) => setDescription(event.target.value)}
                placeholder="Optional"
              />
            </div>
          </div>

          <label className="flex items-center gap-3 rounded-lg border border-white/10 bg-black/20 px-3 py-2 text-sm text-slate-300">
            <input
              type="checkbox"
              className="h-4 w-4 rounded border border-white/20 bg-black/60"
              checked={isDefault}
              onChange={(event) => setIsDefault(event.target.checked)}
            />
            Make this the default variant for the strategy
          </label>

          <div className="rounded-xl border border-white/10 bg-black/20 p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-400">Parameter Overrides</p>
                <p className="mt-1 text-xs text-slate-500">
                  Leave empty to use strategy defaults. Values are stored as strings, numbers, booleans, null, or JSON objects/arrays.
                </p>
              </div>
              <ActionButton type="button" variant="ghost" onClick={handleAddRow}>
                Add row
              </ActionButton>
            </div>

            <div className="mt-4 space-y-3">
              {rows.map((row, index) => (
                <div key={row.id} className="grid gap-3 md:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]">
                  <input
                    className="rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={row.key}
                    onChange={handleRowChange(row.id, 'key')}
                    placeholder={`param_key_${index + 1}`}
                  />
                  <input
                    className="rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={row.value}
                    onChange={handleRowChange(row.id, 'value')}
                    placeholder="0.5"
                  />
                  <ActionButton
                    type="button"
                    variant="subtle"
                    onClick={() => handleRemoveRow(row.id)}
                    className="justify-center"
                  >
                    Remove
                  </ActionButton>
                </div>
              ))}
            </div>
          </div>

          {errorMessage ? (
            <div className="rounded-lg border border-rose-500/20 bg-rose-500/10 px-3 py-2">
              <p className="text-xs text-rose-200">{errorMessage}</p>
            </div>
          ) : null}

          <div className="flex justify-end gap-3">
            <ActionButton type="button" variant="ghost" onClick={onCancel}>
              Cancel
            </ActionButton>
            <ActionButton type="submit" disabled={submitting}>
              {actionLabel}
            </ActionButton>
          </div>
        </form>
      </div>
    </div>
  )
}

export default VariantFormModal
