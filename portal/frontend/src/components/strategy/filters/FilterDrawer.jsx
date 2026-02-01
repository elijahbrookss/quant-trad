import React, { useEffect, useMemo, useRef, useState } from 'react'
import { Dialog, DialogBackdrop, DialogPanel, DialogTitle } from '@headlessui/react'
import { X } from 'lucide-react'
import { FilterBuilder } from './FilterBuilder.jsx'
import { buildFilterPayload, buildFilterPreview, createEmptyPredicate, parseFilterToDraft } from './filterUtils.js'
import { Button } from '../../ui'

const buildEmptyDraft = () => ({
  name: '',
  description: '',
  enabled: true,
  groupMode: 'all',
  predicates: [createEmptyPredicate()],
})

export const FilterDrawer = ({ open, initialFilter, onClose, onSave, title }) => {
  const initialDraft = useMemo(
    () => (initialFilter ? parseFilterToDraft(initialFilter) : buildEmptyDraft()),
    [initialFilter],
  )
  const [draft, setDraft] = useState(initialDraft)
  const initialFocusRef = useRef(null)

  useEffect(() => {
    if (open) {
      setDraft(initialDraft)
    }
  }, [open, initialDraft])

  if (!open) return null

  const preview = buildFilterPreview(draft)

  const handleSave = () => {
    const payload = buildFilterPayload(draft)
    onSave(payload)
  }

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50" initialFocus={initialFocusRef}>
      <DialogBackdrop className="fixed inset-0 bg-black/40" />
      <div className="fixed inset-0 flex justify-end">
        <DialogPanel className="flex h-full w-full max-w-3xl flex-col border-l border-white/10 bg-[#111622] text-slate-100 shadow-2xl">
          <header className="flex items-start justify-between border-b border-white/10 px-5 py-4">
            <div>
              <DialogTitle className="text-base font-semibold text-white">
                {title}
              </DialogTitle>
              <p className="mt-1 text-xs text-slate-400">
                Configure global gates without leaving the strategy view.
              </p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="rounded p-1 text-slate-400 hover:text-white"
              aria-label="Close"
              ref={initialFocusRef}
            >
              <X className="h-4 w-4" />
            </button>
          </header>

          <form className="flex-1 space-y-5 overflow-y-auto px-5 py-4" onSubmit={(event) => event.preventDefault()}>
            <div className="rounded-lg border border-white/10 bg-black/30 px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Preview</p>
              <p className="mt-1 text-sm text-slate-200">{preview}</p>
            </div>

            <div className="space-y-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Match</p>
                  <p className="text-xs text-slate-400">Require all conditions or allow any.</p>
                </div>
                <select
                  className="rounded border border-white/10 bg-black/40 px-2 py-1 text-xs text-slate-200"
                  value={draft.groupMode || 'all'}
                  onChange={(event) => setDraft({ ...draft, groupMode: event.target.value })}
                >
                  <option value="all">All conditions</option>
                  <option value="any">Any condition</option>
                </select>
              </div>

              <FilterBuilder draft={draft} onChange={setDraft} />
            </div>

            <details className="rounded-lg border border-white/10 bg-black/30 px-4 py-3">
              <summary className="cursor-pointer text-xs font-semibold uppercase tracking-[0.18em] text-slate-300">
                Optional details
              </summary>
              <div className="mt-3 grid gap-3 md:grid-cols-2">
                <div>
                  <label className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Preview</label>
                  <input
                    className="mt-2 w-full rounded border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200"
                    value={draft.name}
                    onChange={(event) => setDraft({ ...draft, name: event.target.value })}
                    placeholder={preview}
                  />
                </div>
                <div>
                  <label className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Description</label>
                  <input
                    className="mt-2 w-full rounded border border-white/10 bg-black/40 px-3 py-2 text-sm text-slate-200"
                    value={draft.description}
                    onChange={(event) => setDraft({ ...draft, description: event.target.value })}
                    placeholder="Optional"
                  />
                </div>
              </div>
            </details>

            <label className="inline-flex items-center gap-2 text-xs text-slate-300">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-white/20 bg-black/40"
                checked={draft.enabled !== false}
                onChange={(event) => setDraft({ ...draft, enabled: event.target.checked })}
              />
              Enabled
            </label>
          </form>

          <footer className="flex items-center justify-end gap-2 border-t border-white/10 px-5 py-3">
            <Button variant="ghost" onClick={onClose}>Cancel</Button>
            <Button onClick={handleSave}>Save filter</Button>
          </footer>
        </DialogPanel>
      </div>
    </Dialog>
  )
}
