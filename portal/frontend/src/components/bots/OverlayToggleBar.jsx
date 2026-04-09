const groupLabel = {
  indicator: 'Indicator Overlays',
  trade: 'Trade Overlays',
  regime: 'Regime Context',
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
    <div className="rounded-2xl border border-white/10 bg-black/20 p-3">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Overlay Controls</p>
          <p className="mt-1 text-sm text-slate-300">Server-projected overlays only.</p>
        </div>
        <button
          type="button"
          onClick={onToggleCollapse}
          className="rounded-lg border border-white/10 bg-white/5 px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.24em] text-slate-300 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-12)] hover:text-[color:var(--accent-text-soft)]"
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
              <div className="mb-2 text-[10px] font-medium uppercase tracking-[0.3em] text-slate-600">
                {groupLabel[key] || 'Overlays'}
              </div>
              <div className="flex flex-wrap gap-2">
                {items.map((overlay) => {
                  const isVisible = visibility[overlay.type] !== false
                  return (
                    <button
                      key={`overlay-toggle-${overlay.type}`}
                      type="button"
                      onClick={() => onToggle?.(overlay.type)}
                      className={`rounded-lg border px-3 py-1.5 text-[11px] font-medium uppercase tracking-[0.2em] transition-colors ${
                        isVisible
                          ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-strong)]'
                          : 'border-white/10 bg-white/5 text-slate-400 hover:border-[color:var(--accent-alpha-30)] hover:text-slate-200'
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
