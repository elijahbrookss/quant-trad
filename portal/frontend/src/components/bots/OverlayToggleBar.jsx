export const OverlayToggleBar = ({ overlays = [], visibility = {}, onToggle }) => {
  if (!overlays.length) return null

  return (
    <div className="flex flex-wrap items-center gap-2">
      <span className="text-[10px] font-medium uppercase tracking-[0.3em] text-slate-600">Overlays</span>
      {overlays.map((overlay) => {
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
  )
}
