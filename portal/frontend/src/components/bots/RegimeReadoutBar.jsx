import { buildAxisTooltip, glyphForAxisState } from './regimeReadoutUtils.js'

const pillStyles = {
  structure: 'bg-slate-800/80 text-slate-100 border-slate-600/60',
  volatility: 'bg-orange-500/10 text-orange-200 border-orange-400/40',
  liquidity: 'bg-cyan-500/10 text-cyan-200 border-cyan-400/40',
  expansion: 'bg-fuchsia-500/10 text-fuchsia-200 border-fuchsia-400/40',
}

const formatConfidence = (confidence) => {
  if (!Number.isFinite(Number(confidence))) return 'n/a'
  return `${Math.round(Number(confidence) * 100)}%`
}

export const RegimeReadoutBar = ({ snapshot }) => {
  if (!snapshot) return null

  const { structure, volatility, liquidity, expansion } = snapshot
  return (
    <div className="pointer-events-auto absolute right-3 top-3 z-20 flex flex-wrap items-center gap-2 rounded-xl border border-white/10 bg-slate-950/80 px-3 py-2 text-[11px] shadow-lg backdrop-blur">
      <span className="text-[10px] font-semibold uppercase tracking-[0.35em] text-slate-400">Regime</span>
      <div
        className={`flex items-center gap-1 rounded-md border px-2 py-1 ${pillStyles.structure}`}
        title={buildAxisTooltip('structure', structure)}
      >
        <span className="font-semibold">Structure</span>
        <span>{glyphForAxisState('structure', structure?.state)}</span>
        <span className="text-slate-200">{structure?.state ?? 'unknown'}</span>
        <span className="text-slate-400">({formatConfidence(snapshot?.confidence)})</span>
      </div>
      <div
        className={`flex items-center gap-1 rounded-md border px-2 py-1 ${pillStyles.volatility}`}
        title={buildAxisTooltip('volatility', volatility)}
      >
        <span className="font-semibold">Vol</span>
        <span>{glyphForAxisState('volatility', volatility?.state)}</span>
        <span>{volatility?.state ?? 'unknown'}</span>
        <span className="text-slate-400">({formatConfidence(snapshot?.confidence)})</span>
      </div>
      <div
        className={`flex items-center gap-1 rounded-md border px-2 py-1 ${pillStyles.liquidity}`}
        title={buildAxisTooltip('liquidity', liquidity)}
      >
        <span className="font-semibold">Liq</span>
        <span>{glyphForAxisState('liquidity', liquidity?.state)}</span>
        <span>{liquidity?.state ?? 'unknown'}</span>
        <span className="text-slate-400">({formatConfidence(snapshot?.confidence)})</span>
      </div>
      <div
        className={`flex items-center gap-1 rounded-md border px-2 py-1 ${pillStyles.expansion}`}
        title={buildAxisTooltip('expansion', expansion)}
      >
        <span className="font-semibold">Exp</span>
        <span>{glyphForAxisState('expansion', expansion?.state)}</span>
        <span>{expansion?.state ?? 'unknown'}</span>
        <span className="text-slate-400">({formatConfidence(snapshot?.confidence)})</span>
      </div>
    </div>
  )
}
