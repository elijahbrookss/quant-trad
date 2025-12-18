export const MarkerTooltip = ({ markerTooltip }) => {
  if (!markerTooltip?.entries?.length) return null
  return (
    <div
      className="pointer-events-none absolute z-10 rounded-lg border border-white/10 bg-black/70 px-3 py-2 text-xs text-white shadow-lg backdrop-blur"
      style={{ left: markerTooltip.x, top: markerTooltip.y - 12 }}
    >
      <p className="text-[11px] uppercase tracking-[0.25em] text-slate-300">TP / SL breakdown</p>
      <ul className="mt-1 space-y-0.5 text-slate-100">
        {markerTooltip.entries.map((line, idx) => (
          <li key={`${line}-${idx}`} className="whitespace-nowrap">
            {line}
          </li>
        ))}
      </ul>
    </div>
  )
}

