const groupLabel = {
  indicator: 'Indicator Overlays',
  trade: 'Trade Overlays',
  regime: 'Regime / Context Overlays',
}

const deriveGroup = (overlay) => {
  const key = (overlay?.group || overlay?.ui?.group || '').toString().toLowerCase()
  if (['indicator', 'trade', 'regime', 'context'].includes(key)) return key === 'context' ? 'regime' : key

  const type = (overlay?.type || '').toString().toLowerCase()
  if (['trade', 'tp', 'sl', 'stop', 'target', 'ray', 'leg', 'exit', 'entry'].some((token) => type.includes(token))) return 'trade'
  if (['regime', 'context', 'session'].some((token) => type.includes(token))) return 'regime'
  return 'indicator'
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
      const bucket = deriveGroup(overlay)
      acc[bucket] = acc[bucket] || []
      acc[bucket].push(overlay)
      return acc
    },
    {},
  )

  const order = ['indicator', 'trade', 'regime']

  return (
    <div className="space-y-2">
      <button
        type="button"
        onClick={onToggleCollapse}
        className="flex w-full items-center justify-between rounded-md border border-slate-800 bg-slate-950/60 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.25em] text-slate-200 transition-colors hover:border-slate-700 hover:bg-slate-900"
      >
        <span>Toggle Overlays</span>
        <span className="text-[10px] text-slate-400">{collapsed ? 'Show' : 'Hide'}</span>
      </button>

      {!collapsed &&
        order
          .map((key) => ({ key, items: grouped[key] || [] }))
          .filter(({ items }) => items.length > 0)
          .map(({ key, items }) => (
            <div key={`overlay-group-${key}`} className="flex flex-wrap items-center gap-2">
              <span className="text-[10px] font-medium uppercase tracking-[0.3em] text-slate-600">
                {groupLabel[key] || 'Overlays'}
              </span>
              {items.map((overlay) => {
                const isVisible = visibility[overlay.type] !== false
                return (
                  <button
                    key={`overlay-toggle-${overlay.type}`}
                    type="button"
                    onClick={() => onToggle?.(overlay.type)}
                    className={`rounded-md border px-2.5 py-1 text-[11px] font-medium uppercase tracking-wider transition-colors ${
                      isVisible
                        ? 'border-slate-700 bg-slate-800/80 text-slate-200'
                        : 'border-slate-800 bg-slate-950/50 text-slate-500 hover:border-slate-700 hover:text-slate-400'
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
          ))}
    </div>
  )
}
