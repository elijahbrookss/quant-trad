import React, { useMemo, useState } from 'react'
import { FilterBuilder } from './FilterBuilder.jsx'
import { buildFilterPayload, buildFilterSummary, createEmptyPredicate, parseFilterToDraft } from './filterUtils.js'
import { Button } from '../../ui'

const buildEmptyDraft = () => ({
  name: '',
  description: '',
  enabled: true,
  groupMode: 'all',
  predicates: [createEmptyPredicate()],
})

export const FilterModal = ({ open, initialFilter, onClose, onSave, title }) => {
  const initialDraft = useMemo(
    () => (initialFilter ? parseFilterToDraft(initialFilter) : buildEmptyDraft()),
    [initialFilter],
  )
  const [draft, setDraft] = useState(initialDraft)

  if (!open) return null

  const handleSave = () => {
    const payload = buildFilterPayload(draft)
    onSave(payload)
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="w-full max-w-3xl space-y-5 rounded-2xl border border-white/10 bg-[#14171f] p-6 text-slate-100 shadow-xl">
        <header className="space-y-1">
          <h3 className="text-lg font-semibold text-white">{title}</h3>
          <p className="text-sm text-slate-400">Filters act as gates after signals match.</p>
        </header>

        <div className="grid gap-3 md:grid-cols-2">
          <div>
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Label</label>
            <input
              className="mt-2 w-full rounded border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200"
              value={draft.name}
              onChange={(event) => setDraft({ ...draft, name: event.target.value })}
              placeholder={buildFilterSummary({ dsl: buildFilterPayload(draft).dsl })}
            />
          </div>
          <div>
            <label className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Description</label>
            <input
              className="mt-2 w-full rounded border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200"
              value={draft.description}
              onChange={(event) => setDraft({ ...draft, description: event.target.value })}
              placeholder="Optional"
            />
          </div>
        </div>

        <FilterBuilder draft={draft} onChange={setDraft} />

        <label className="inline-flex items-center gap-2 text-xs text-slate-300">
          <input
            type="checkbox"
            className="h-4 w-4 rounded border-white/20 bg-black/40"
            checked={draft.enabled !== false}
            onChange={(event) => setDraft({ ...draft, enabled: event.target.checked })}
          />
          Enabled
        </label>

        <footer className="flex items-center justify-end gap-2">
          <Button variant="ghost" onClick={onClose}>Cancel</Button>
          <Button onClick={handleSave}>Save filter</Button>
        </footer>
      </div>
    </div>
  )
}
