import { useEffect, useState } from 'react'

import ActionButton from '../ui/ActionButton.jsx'

const formatFilters = (filters) => {
  if (!Array.isArray(filters) || !filters.length) return '[]'
  return JSON.stringify(filters, null, 2)
}

function VariantFormModal({ open, initialValues, onSubmit, onCancel, submitting, error }) {
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [isDefault, setIsDefault] = useState(false)
  const [filtersText, setFiltersText] = useState('[]')
  const [localError, setLocalError] = useState(null)

  useEffect(() => {
    if (!open) return
    setName(initialValues?.name || '')
    setDescription(initialValues?.description || '')
    setIsDefault(Boolean(initialValues?.is_default))
    setFiltersText(formatFilters(initialValues?.output_filters))
    setLocalError(null)
  }, [open, initialValues])

  const errorMessage = localError || error
  const title = initialValues?.id ? 'Edit variant' : 'Create variant'
  const actionLabel = submitting ? 'Saving…' : (initialValues?.id ? 'Save changes' : 'Create variant')

  if (!open) return null

  const handleSubmit = async (event) => {
    event.preventDefault()
    const trimmedName = String(name || '').trim()
    if (!trimmedName) {
      setLocalError('Variant name is required.')
      return
    }

    let outputFilters = []
    try {
      const parsed = JSON.parse(String(filtersText || '[]'))
      if (Array.isArray(parsed)) {
        outputFilters = parsed
      } else if (parsed && typeof parsed === 'object') {
        outputFilters = [parsed]
      } else {
        setLocalError('Output filters must be a JSON object or array.')
        return
      }
    } catch (parseErr) {
      setLocalError(parseErr?.message || 'Failed to parse output filters JSON.')
      return
    }

    setLocalError(null)
    await onSubmit?.({
      name: trimmedName,
      description: String(description || '').trim() || null,
      output_filters: outputFilters,
      is_default: isDefault,
    })
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="w-full max-w-2xl space-y-6 rounded-2xl border border-white/10 bg-[#14171f] p-6 text-slate-100 shadow-xl">
        <header className="space-y-1">
          <h3 className="text-lg font-semibold">{title}</h3>
          <p className="text-sm text-slate-400">
            Save a named set of decision filters over attached indicator outputs.
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
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-400">Output Filters</p>
              <p className="mt-1 text-xs text-slate-500">
                JSON object or array. Filters apply to attached indicator outputs and compile into rule guards.
              </p>
            </div>
            <textarea
              className="mt-4 min-h-48 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 font-mono text-xs text-slate-200 focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              value={filtersText}
              onChange={(event) => setFiltersText(event.target.value)}
              spellCheck={false}
            />
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
