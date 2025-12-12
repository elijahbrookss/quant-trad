import React from 'react'
import { formatNumber, formatCurrency } from '../../../utils'

/**
 * Overview tab showing strategy information, risk configuration, and statistics.
 */
export const OverviewTab = ({ strategy, ruleCount, indicatorCount, atmTargets }) => {
  return (
    <div className="space-y-4">
      {/* Strategy Information */}
      <div>
        <h3 className="text-sm font-semibold text-white">Strategy Information</h3>
        <dl className="mt-3 grid gap-4 text-sm md:grid-cols-2">
          <div>
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Name</dt>
            <dd className="mt-1 text-white">{strategy.name || '—'}</dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Timeframe</dt>
            <dd className="mt-1 text-white">{strategy.timeframe || '—'}</dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Symbols</dt>
            <dd className="mt-1 text-white">{(strategy.symbols || []).join(', ') || '—'}</dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Data Source</dt>
            <dd className="mt-1 text-white">{strategy.datasource || '—'}</dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Exchange</dt>
            <dd className="mt-1 text-white">{strategy.exchange || '—'}</dd>
          </div>
          {strategy.description && (
            <div className="md:col-span-2">
              <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Description</dt>
              <dd className="mt-1 text-slate-300">{strategy.description}</dd>
            </div>
          )}
        </dl>
      </div>

      {/* Risk Configuration */}
      <div>
        <h3 className="text-sm font-semibold text-white">Risk Configuration</h3>
        <dl className="mt-3 grid gap-4 text-sm md:grid-cols-2">
          <div>
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Base Risk per Trade</dt>
            <dd className="mt-1 text-white">
              {strategy.base_risk_per_trade != null ? formatCurrency(strategy.base_risk_per_trade) : '—'}
            </dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Global Risk Multiplier</dt>
            <dd className="mt-1 text-white">
              {strategy.global_risk_multiplier != null ? `${formatNumber(strategy.global_risk_multiplier)}x` : '—'}
            </dd>
          </div>
        </dl>
      </div>

      {/* Statistics */}
      <div>
        <h3 className="text-sm font-semibold text-white">Statistics</h3>
        <div className="mt-3 grid gap-3 text-sm md:grid-cols-3">
          <div className="rounded-xl border border-white/10 bg-white/5 p-3">
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Rules</dt>
            <dd className="mt-1 text-2xl font-semibold text-white">{ruleCount}</dd>
          </div>
          <div className="rounded-xl border border-white/10 bg-white/5 p-3">
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">Indicators</dt>
            <dd className="mt-1 text-2xl font-semibold text-white">{indicatorCount}</dd>
          </div>
          <div className="rounded-xl border border-white/10 bg-white/5 p-3">
            <dt className="text-xs uppercase tracking-[0.3em] text-slate-500">TP Targets</dt>
            <dd className="mt-1 text-2xl font-semibold text-white">{atmTargets.length}</dd>
          </div>
        </div>
      </div>
    </div>
  )
}
