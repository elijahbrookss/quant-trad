import React from 'react'

const formatNumber = (value) => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return '—'
  return numeric.toLocaleString(undefined, { maximumFractionDigits: 4 })
}

const formatCurrency = (value) => {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return '—'
  return numeric.toLocaleString(undefined, {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
  })
}

export const RiskTab = ({ strategy }) => {
  const riskConfig = strategy?.risk_config && typeof strategy.risk_config === 'object' ? strategy.risk_config : {}
  const instrumentMultipliers =
    riskConfig?.instrument_multipliers && typeof riskConfig.instrument_multipliers === 'object'
      ? riskConfig.instrument_multipliers
      : {}
  const slots = Array.isArray(strategy?.instrument_slots) ? strategy.instrument_slots : []

  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-2">
        <div className="rounded-xl border border-white/10 bg-black/20 p-4">
          <p className="text-[10px] font-semibold uppercase tracking-[0.28em] text-slate-500">Base Risk Per Trade</p>
          <p className="mt-2 text-2xl font-semibold text-white">{formatCurrency(riskConfig.base_risk_per_trade)}</p>
        </div>
        <div className="rounded-xl border border-white/10 bg-black/20 p-4">
          <p className="text-[10px] font-semibold uppercase tracking-[0.28em] text-slate-500">Global Multiplier</p>
          <p className="mt-2 text-2xl font-semibold text-white">{formatNumber(riskConfig.global_risk_multiplier)}x</p>
        </div>
      </div>

      <div className="rounded-xl border border-white/10 bg-black/20 p-4">
        <p className="text-[10px] font-semibold uppercase tracking-[0.28em] text-slate-500">Per-Symbol Multipliers</p>
        {!slots.length ? (
          <p className="mt-2 text-sm text-slate-500">No instruments configured.</p>
        ) : (
          <div className="mt-3 grid gap-2 md:grid-cols-2">
            {slots.map((slot) => {
              const symbol = String(slot?.symbol || '').trim().toUpperCase()
              const multiplier = instrumentMultipliers[symbol]
              return (
                <div key={symbol} className="rounded-lg border border-white/8 bg-white/[0.03] px-3 py-2">
                  <div className="flex items-center justify-between text-sm">
                    <span className="font-semibold text-white">{symbol || 'Unknown'}</span>
                    <span className="text-slate-300">{formatNumber(multiplier ?? 1)}x</span>
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}

export default RiskTab
