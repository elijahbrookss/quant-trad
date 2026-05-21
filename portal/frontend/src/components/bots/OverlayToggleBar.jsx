import { ChevronDown, ChevronUp, Layers3 } from 'lucide-react'

import { resolveOverlayGroup } from './hooks/useOverlayControls.js'

const groupLabel = {
  market: 'Market',
  regime: 'Regime',
  indicator: 'Indicators',
  trade: 'Trades',
}

const groupDescription = {
  market: 'Market Profile',
  regime: 'Regime zones and markers',
  indicator: 'ATR and candle statistics',
  trade: 'Entries, exits, stops, and targets',
}

export const OverlayToggleBar = ({
  overlays = [],
  visibility = {},
  onToggle,
  collapsed = false,
  onToggleCollapse,
}) => {
  if (!overlays.length) return null

  const grouped = overlays.reduce(
    (acc, overlay) => {
      const bucket = resolveOverlayGroup(overlay)
      acc[bucket] = acc[bucket] || []
      acc[bucket].push(overlay)
      return acc
    },
    {},
  )

  const order = ['market', 'regime', 'indicator', 'trade']

  if (collapsed) {
    return (
      <div className="flex justify-end">
        <button
          type="button"
          onClick={onToggleCollapse}
          className="inline-flex items-center gap-1.5 rounded-[3px] border border-white/8 bg-black/20 px-2.5 py-1.5 text-xs font-semibold text-slate-400 transition hover:border-white/16 hover:bg-black/30 hover:text-slate-100"
        >
          <Layers3 className="size-3" />
          Overlays
          <span className="text-slate-600">{overlays.length}</span>
          <ChevronDown className="size-3" />
        </button>
      </div>
    )
  }

  return (
    <div className="rounded-[3px] border border-white/8 bg-black/15 px-3 py-2">
      <div className="mb-2 flex items-center justify-between gap-3">
        <p className="text-sm font-semibold text-slate-100">Overlays</p>
        <button
          type="button"
          onClick={onToggleCollapse}
          className="inline-flex items-center gap-1 rounded-[3px] border border-white/10 bg-black/25 px-2 py-1 text-xs font-semibold text-slate-400 transition hover:border-white/16 hover:bg-black/40 hover:text-slate-100"
        >
          Hide
          <ChevronUp className="size-3" />
        </button>
      </div>

      {order
        .map((key) => ({ key, items: grouped[key] || [] }))
        .filter(({ items }) => items.length > 0)
        .map(({ key, items }) => (
          <div key={`overlay-group-${key}`} className="mb-2 last:mb-0">
            <div className="mb-1.5 flex items-baseline gap-2">
              <span className="text-xs font-medium text-slate-300">{groupLabel[key] || 'Overlays'}</span>
              <span className="text-[10px] text-slate-600">{groupDescription[key]}</span>
            </div>
            <div className="flex flex-wrap gap-1.5">
              {items.map((overlay) => {
                const isVisible = visibility[overlay.type] !== false
                return (
                  <button
                    key={`overlay-toggle-${overlay.type}`}
                    type="button"
                    onClick={() => onToggle?.(overlay.type)}
                    className={`rounded-[3px] border px-2.5 py-1 text-[11px] font-medium transition-colors ${
                      isVisible
                        ? 'border-sky-500/30 bg-sky-500/10 text-sky-100'
                        : 'border-white/10 bg-black/20 text-slate-400 hover:border-white/16 hover:text-slate-200'
                    }`}
                    aria-pressed={isVisible}
                  >
                    <span className="inline-flex items-center gap-1.5">
                      {overlay.color ? (
                        <span
                          className="size-1.5 rounded-full"
                          style={{ backgroundColor: overlay.color }}
                          aria-hidden="true"
                        />
                      ) : null}
                      {overlay.label}
                    </span>
                  </button>
                )
              })}
            </div>
          </div>
        ))}
    </div>
  )
}
