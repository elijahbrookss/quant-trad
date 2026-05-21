import { AlertTriangle } from 'lucide-react'

import { BotLensPanel } from './BotLensPanel.jsx'

export function WarningPanel({ model }) {
  return (
    <BotLensPanel
      eyebrow="Health"
      title="Runtime warnings"
      subtitle="Indicator warnings are part of current run health and stay outside retrieval history."
    >
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="inline-flex items-center gap-2 text-sm text-slate-200">
          <AlertTriangle className="size-4 text-amber-300" />
          <span>{model.count} active warnings</span>
        </div>
      </div>
      {model.items.length ? (
        <div className="grid gap-2">
          {model.items.map((warning) => (
            <article key={warning.warning_id} className="rounded-xl border border-white/10 bg-black/20 px-3 py-3">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div className="min-w-0">
                  <p className="text-sm font-medium text-slate-100">{warning.title}</p>
                  <p className="mt-1 text-xs leading-relaxed text-slate-400">{warning.message}</p>
                </div>
                <div className="flex items-center gap-2 text-[11px] text-slate-400">
                  <span className="rounded-full bg-white/5 px-2 py-1 uppercase tracking-[0.18em]">
                    x{Math.max(1, Number(warning.count || 1) || 1)}
                  </span>
                  <span>{warning.seenLabel}</span>
                </div>
              </div>
            </article>
          ))}
        </div>
      ) : (
        <div className="rounded-xl border border-dashed border-white/10 px-4 py-5 text-sm text-slate-400">
          No runtime warnings are active.
        </div>
      )}
    </BotLensPanel>
  )
}
