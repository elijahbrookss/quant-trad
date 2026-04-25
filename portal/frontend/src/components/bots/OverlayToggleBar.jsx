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

  return (
    <div className="qt-ops-console p-3">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <p className="text-xs font-semibold text-slate-200">Overlays</p>
          <p className="mt-1 text-xs text-slate-500">Server-projected chart layers.</p>
        </div>
        <button
          type="button"
          onClick={onToggleCollapse}
          className="qt-mono rounded-[3px] border border-white/10 bg-black/25 px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-300 transition hover:border-white/16 hover:bg-black/40 hover:text-slate-100"
        >
          {collapsed ? 'Show' : 'Hide'}
        </button>
      </div>

      {!collapsed &&
        order
          .map((key) => ({ key, items: grouped[key] || [] }))
          .filter(({ items }) => items.length > 0)
          .map(({ key, items }) => (
            <div key={`overlay-group-${key}`} className="mb-3 last:mb-0">
              <div className="mb-2 flex items-baseline gap-2">
                <span className="text-xs font-medium text-slate-300">{groupLabel[key] || 'Overlays'}</span>
                <span className="text-[11px] text-slate-600">{groupDescription[key]}</span>
              </div>
              <div className="flex flex-wrap gap-2">
                {items.map((overlay) => {
                  const isVisible = visibility[overlay.type] !== false
                  return (
                    <button
                      key={`overlay-toggle-${overlay.type}`}
                      type="button"
                      onClick={() => onToggle?.(overlay.type)}
                      className={`rounded-[4px] border px-3 py-1.5 text-[12px] font-medium transition-colors ${
                        isVisible
                          ? 'border-sky-500/30 bg-sky-500/10 text-sky-100'
                          : 'border-white/10 bg-black/25 text-slate-400 hover:border-white/16 hover:text-slate-200'
                      }`}
                      aria-pressed={isVisible}
                    >
                      <span className="inline-flex items-center gap-1.5">
                        {overlay.color ? (
                          <span
                            className="size-2 rounded-full"
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
