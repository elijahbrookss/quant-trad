import { Fragment, useEffect, useMemo, useState } from 'react'
import { Switch } from '@headlessui/react'
import {
  fetchStrategies,
  createStrategy,
  updateStrategy,
  deleteStrategy,
  attachStrategyIndicator,
  detachStrategyIndicator,
  createStrategyRule,
  updateStrategyRule,
  deleteStrategyRule,
  generateStrategySignals,
} from '../adapters/strategy.adapter.js'
import { fetchIndicators } from '../adapters/indicator.adapter.js'
import { createLogger } from '../utils/logger.js'
import { useChartState } from '../contexts/ChartStateContext.jsx'

const STRATEGY_DEFAULTS = {
  name: '',
  description: '',
  symbols: '',
  timeframe: '15m',
  datasource: '',
  exchange: '',
}

const RULE_DEFAULTS = {
  name: '',
  description: '',
  indicator_id: '',
  signal_type: '',
  min_confidence: 0.5,
  action: 'buy',
  enabled: true,
}

const formatIndicatorLabel = (indicator) => {
  if (!indicator) return 'Unknown indicator'
  const name = indicator.name || indicator.type
  const typeLabel = indicator.type ? indicator.type.replace(/[_-]/g, ' ') : 'custom'
  return `${name} (${typeLabel})`
}

const parseSymbols = (symbols) => {
  if (!symbols) return []
  if (Array.isArray(symbols)) return symbols
  return symbols
    .split(/[\s,;]+/)
    .map((token) => token.trim())
    .filter(Boolean)
}

const StrategyForm = ({
  open,
  onClose,
  onSubmit,
  initialValues,
  submitting,
}) => {
  const [form, setForm] = useState(STRATEGY_DEFAULTS)

  useEffect(() => {
    if (initialValues) {
      setForm({
        name: initialValues.name || '',
        description: initialValues.description || '',
        symbols: (initialValues.symbols || []).join(', '),
        timeframe: initialValues.timeframe || '15m',
        datasource: initialValues.datasource || '',
        exchange: initialValues.exchange || '',
      })
    } else {
      setForm(STRATEGY_DEFAULTS)
    }
  }, [initialValues])

  if (!open) return null

  const handleSubmit = async (event) => {
    event.preventDefault()
    const payload = {
      name: form.name.trim(),
      description: form.description.trim() || null,
      timeframe: form.timeframe.trim() || '15m',
      datasource: form.datasource.trim() || null,
      exchange: form.exchange.trim() || null,
      symbols: parseSymbols(form.symbols),
    }
    await onSubmit(payload)
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-6">
      <div className="w-full max-w-lg space-y-6 rounded-2xl border border-white/10 bg-[#1b1e28] p-6 text-slate-200 shadow-xl">
        <div>
          <h3 className="text-lg font-semibold text-white">
            {initialValues ? 'Edit Strategy' : 'Create Strategy'}
          </h3>
          <p className="mt-1 text-sm text-slate-400">
            Configure baseline metadata for your strategy blueprint.
          </p>
        </div>

        <form className="space-y-4" onSubmit={handleSubmit}>
          <div>
            <label className="block text-xs font-semibold uppercase tracking-widest text-slate-400">
              Name
            </label>
            <input
              className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              value={form.name}
              onChange={(e) => setForm((prev) => ({ ...prev, name: e.target.value }))}
              required
            />
          </div>

          <div>
            <label className="block text-xs font-semibold uppercase tracking-widest text-slate-400">
              Description
            </label>
            <textarea
              className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              rows={3}
              value={form.description}
              onChange={(e) => setForm((prev) => ({ ...prev, description: e.target.value }))}
            />
          </div>

          <div>
            <label className="block text-xs font-semibold uppercase tracking-widest text-slate-400">
              Symbols (comma separated)
            </label>
            <input
              className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
              value={form.symbols}
              onChange={(e) => setForm((prev) => ({ ...prev, symbols: e.target.value }))}
            />
          </div>

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-widest text-slate-400">
                Timeframe
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.timeframe}
                onChange={(e) => setForm((prev) => ({ ...prev, timeframe: e.target.value }))}
                placeholder="15m"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-widest text-slate-400">
                Datasource
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.datasource}
                onChange={(e) => setForm((prev) => ({ ...prev, datasource: e.target.value }))}
                placeholder="alpaca"
              />
            </div>
            <div>
              <label className="block text-xs font-semibold uppercase tracking-widest text-slate-400">
                Exchange
              </label>
              <input
                className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                value={form.exchange}
                onChange={(e) => setForm((prev) => ({ ...prev, exchange: e.target.value }))}
                placeholder="CBOT"
              />
            </div>
          </div>

          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              className="rounded-lg border border-white/10 px-4 py-2 text-sm text-slate-300 hover:border-white/20 hover:text-white"
              onClick={onClose}
              disabled={submitting}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="rounded-lg bg-[color:var(--accent-alpha-70)] px-4 py-2 text-sm font-semibold text-black shadow-lg shadow-[color:var(--accent-shadow-strong)] disabled:cursor-not-allowed disabled:opacity-60"
              disabled={submitting}
            >
              {submitting ? 'Saving…' : initialValues ? 'Save Changes' : 'Create Strategy'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

const StrategyCard = ({
  strategy,
  isExpanded,
  onToggleExpand,
  onEdit,
  onDelete,
  availableIndicators,
  onToggleIndicator,
  onCreateRule,
  onUpdateRule,
  onDeleteRule,
  onGenerateSignals,
  signalResult,
  loadingSignals,
}) => {
  const [ruleForm, setRuleForm] = useState(RULE_DEFAULTS)
  const [editingRuleId, setEditingRuleId] = useState(null)

  useEffect(() => {
    if (!isExpanded) {
      setRuleForm(RULE_DEFAULTS)
      setEditingRuleId(null)
    }
  }, [isExpanded])

  const handleRuleSubmit = async (event) => {
    event.preventDefault()
    const payload = {
      name: ruleForm.name.trim(),
      description: ruleForm.description.trim() || null,
      indicator_id: ruleForm.indicator_id || null,
      signal_type: ruleForm.signal_type.trim(),
      min_confidence: Number(ruleForm.min_confidence) || 0,
      action: ruleForm.action,
      enabled: ruleForm.enabled,
    }

    if (!payload.name || !payload.signal_type) {
      return
    }

    if (editingRuleId) {
      await onUpdateRule(strategy.id, editingRuleId, payload)
    } else {
      await onCreateRule(strategy.id, payload)
    }

    setRuleForm(RULE_DEFAULTS)
    setEditingRuleId(null)
  }

  const handleRuleEdit = (rule) => {
    setEditingRuleId(rule.id)
    setRuleForm({
      name: rule.name || '',
      description: rule.description || '',
      indicator_id: rule.indicator_id || '',
      signal_type: rule.signal_type || '',
      min_confidence: rule.min_confidence ?? 0,
      action: rule.action || 'buy',
      enabled: rule.enabled ?? true,
    })
  }

  const handleRuleDelete = async (ruleId) => {
    await onDeleteRule(strategy.id, ruleId)
    if (editingRuleId === ruleId) {
      setEditingRuleId(null)
      setRuleForm(RULE_DEFAULTS)
    }
  }

  const currentIndicators = new Set(strategy.indicator_ids || [])
  const buySignals = signalResult?.buy_signals || []
  const sellSignals = signalResult?.sell_signals || []

  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-6 text-sm text-slate-200">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-2">
          <h3 className="text-lg font-semibold text-white">{strategy.name}</h3>
          <p className="text-xs uppercase tracking-[0.4em] text-slate-400">
            {strategy.timeframe} • {strategy.symbols?.join(' / ') || 'No symbols'}
          </p>
          {strategy.description && (
            <p className="text-sm text-slate-400">{strategy.description}</p>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            onClick={() => onToggleExpand(strategy.id)}
            className="rounded-full border border-white/10 px-4 py-2 text-xs uppercase tracking-[0.3em] text-slate-300 hover:border-[color:var(--accent-alpha-40)] hover:text-white"
          >
            {isExpanded ? 'Hide Details' : 'Manage Strategy'}
          </button>
          <button
            onClick={() => onEdit(strategy)}
            className="rounded-full border border-white/10 px-4 py-2 text-xs uppercase tracking-[0.3em] text-slate-300 hover:border-[color:var(--accent-alpha-40)] hover:text-white"
          >
            Edit
          </button>
          <button
            onClick={() => onDelete(strategy.id)}
            className="rounded-full border border-red-500/20 px-4 py-2 text-xs uppercase tracking-[0.3em] text-red-300 hover:border-red-500/40 hover:text-red-100"
          >
            Delete
          </button>
        </div>
      </div>

      {isExpanded && (
        <div className="mt-6 space-y-6">
          <div className="space-y-3">
            <h4 className="text-sm font-semibold uppercase tracking-[0.3em] text-slate-300">
              Indicators
            </h4>
            <div className="grid gap-3 md:grid-cols-2">
              {availableIndicators.map((indicator) => {
                const checked = currentIndicators.has(indicator.id)
                return (
                  <label
                    key={indicator.id}
                    className={`flex cursor-pointer items-center justify-between rounded-xl border px-4 py-3 text-xs transition ${
                      checked
                        ? 'border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-strong)]'
                        : 'border-white/10 bg-black/20 text-slate-300 hover:border-white/20'
                    }`}
                  >
                    <span className="pr-4 text-left text-sm font-medium">
                      {formatIndicatorLabel(indicator)}
                    </span>
                    <input
                      type="checkbox"
                      className="h-4 w-4"
                      checked={checked}
                      onChange={(e) => onToggleIndicator(strategy.id, indicator.id, e.target.checked)}
                    />
                  </label>
                )
              })}
              {!availableIndicators.length && (
                <p className="rounded-xl border border-dashed border-white/10 bg-black/20 px-4 py-4 text-xs text-slate-400">
                  Create indicators first to attach them to strategies.
                </p>
              )}
            </div>
          </div>

          <div className="space-y-3">
            <h4 className="text-sm font-semibold uppercase tracking-[0.3em] text-slate-300">
              Rules
            </h4>

            <form className="space-y-3 rounded-xl border border-white/10 bg-black/30 p-4" onSubmit={handleRuleSubmit}>
              <div className="grid gap-3 md:grid-cols-2">
                <div>
                  <label className="block text-xs uppercase tracking-[0.3em] text-slate-400">Rule Name</label>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={ruleForm.name}
                    onChange={(e) => setRuleForm((prev) => ({ ...prev, name: e.target.value }))}
                    required
                  />
                </div>
                <div>
                  <label className="block text-xs uppercase tracking-[0.3em] text-slate-400">Signal Type</label>
                  <input
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={ruleForm.signal_type}
                    onChange={(e) => setRuleForm((prev) => ({ ...prev, signal_type: e.target.value }))}
                    placeholder="breakout"
                    required
                  />
                </div>
                <div>
                  <label className="block text-xs uppercase tracking-[0.3em] text-slate-400">Indicator</label>
                  <select
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={ruleForm.indicator_id}
                    onChange={(e) => setRuleForm((prev) => ({ ...prev, indicator_id: e.target.value }))}
                  >
                    <option value="">Select indicator</option>
                    {availableIndicators.map((indicator) => (
                      <option key={indicator.id} value={indicator.id}>
                        {indicator.name || indicator.type}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-xs uppercase tracking-[0.3em] text-slate-400">Min Confidence</label>
                  <input
                    type="number"
                    step="0.1"
                    min="0"
                    max="1"
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={ruleForm.min_confidence}
                    onChange={(e) => setRuleForm((prev) => ({ ...prev, min_confidence: e.target.value }))}
                  />
                </div>
              </div>

              <div className="grid gap-3 md:grid-cols-2">
                <div>
                  <label className="block text-xs uppercase tracking-[0.3em] text-slate-400">Action</label>
                  <select
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                    value={ruleForm.action}
                    onChange={(e) => setRuleForm((prev) => ({ ...prev, action: e.target.value }))}
                  >
                    <option value="buy">Buy</option>
                    <option value="sell">Sell</option>
                  </select>
                </div>
                <div className="flex items-center justify-between">
                  <div>
                    <span className="block text-xs uppercase tracking-[0.3em] text-slate-400">Enabled</span>
                    <p className="mt-1 text-xs text-slate-400">Toggle to include this rule in signal evaluation.</p>
                  </div>
                  <Switch
                    checked={ruleForm.enabled}
                    onChange={(value) => setRuleForm((prev) => ({ ...prev, enabled: value }))}
                    className={`${
                      ruleForm.enabled ? 'bg-[color:var(--accent-alpha-70)]' : 'bg-slate-600'
                    } relative inline-flex h-6 w-11 items-center rounded-full transition`}
                  >
                    <span
                      className={`${
                        ruleForm.enabled ? 'translate-x-6' : 'translate-x-1'
                      } inline-block h-4 w-4 transform rounded-full bg-white transition`}
                    />
                  </Switch>
                </div>
              </div>

              <div>
                <label className="block text-xs uppercase tracking-[0.3em] text-slate-400">Notes</label>
                <textarea
                  className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  rows={2}
                  value={ruleForm.description}
                  onChange={(e) => setRuleForm((prev) => ({ ...prev, description: e.target.value }))}
                />
              </div>

              <div className="flex justify-end gap-2 pt-2">
                {editingRuleId && (
                  <button
                    type="button"
                    onClick={() => {
                      setEditingRuleId(null)
                      setRuleForm(RULE_DEFAULTS)
                    }}
                    className="rounded-lg border border-white/10 px-4 py-2 text-xs uppercase tracking-[0.3em] text-slate-300 hover:border-white/20 hover:text-white"
                  >
                    Cancel Edit
                  </button>
                )}
                <button
                  type="submit"
                  className="rounded-lg bg-[color:var(--accent-alpha-70)] px-4 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-black shadow-lg shadow-[color:var(--accent-shadow-strong)]"
                >
                  {editingRuleId ? 'Update Rule' : 'Add Rule'}
                </button>
              </div>
            </form>

            <div className="space-y-2">
              {(strategy.rules || []).length === 0 && (
                <p className="text-xs text-slate-400">No rules defined yet. Add one above to start evaluating signals.</p>
              )}
              {(strategy.rules || []).map((rule) => (
                <div
                  key={rule.id}
                  className="flex flex-col gap-3 rounded-xl border border-white/10 bg-black/20 p-4 md:flex-row md:items-center md:justify-between"
                >
                  <div className="space-y-1">
                    <p className="text-sm font-semibold text-white">{rule.name}</p>
                    <p className="text-xs uppercase tracking-[0.3em] text-slate-400">
                      {rule.action?.toUpperCase()} • {rule.signal_type}
                    </p>
                    <p className="text-xs text-slate-400">
                      {rule.indicator_id ? `Indicator: ${rule.indicator_id}` : 'No indicator attached'}
                    </p>
                    {rule.description && <p className="text-xs text-slate-500">{rule.description}</p>}
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <span
                      className={`rounded-full px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.3em] ${
                        rule.enabled ? 'bg-[color:var(--accent-alpha-25)] text-[color:var(--accent-text-strong)]' : 'bg-white/10 text-slate-400'
                      }`}
                    >
                      {rule.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                    <button
                      onClick={() => handleRuleEdit(rule)}
                      className="rounded-full border border-white/10 px-3 py-1 text-[10px] uppercase tracking-[0.3em] text-slate-300 hover:border-white/20 hover:text-white"
                    >
                      Edit
                    </button>
                    <button
                      onClick={() => handleRuleDelete(rule.id)}
                      className="rounded-full border border-red-500/20 px-3 py-1 text-[10px] uppercase tracking-[0.3em] text-red-300 hover:border-red-500/40 hover:text-red-100"
                    >
                      Delete
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="space-y-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <h4 className="text-sm font-semibold uppercase tracking-[0.3em] text-slate-300">
                  Signal Checks
                </h4>
                <p className="text-xs text-slate-400">Run the rule deck against the current chart window.</p>
              </div>
              <button
                onClick={() => onGenerateSignals(strategy.id)}
                className="rounded-full bg-[color:var(--accent-alpha-70)] px-4 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-black shadow-lg shadow-[color:var(--accent-shadow-strong)]"
                disabled={loadingSignals}
              >
                {loadingSignals ? 'Evaluating…' : 'Generate Signals'}
              </button>
            </div>

            {signalResult && (
              <div className="grid gap-4 md:grid-cols-2">
                <div className="rounded-xl border border-white/10 bg-black/20 p-4">
                  <h5 className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Buy Signals</h5>
                  {buySignals.length === 0 ? (
                    <p className="mt-3 text-xs text-slate-400">No buy signals for the current window.</p>
                  ) : (
                    <ul className="mt-3 space-y-2 text-xs text-slate-200">
                      {buySignals.map((sig) => (
                        <li key={sig.rule_id} className="rounded-lg border border-emerald-500/20 bg-emerald-500/10 px-3 py-2">
                          <p className="font-semibold">{sig.rule_name}</p>
                          <p className="text-[10px] uppercase tracking-[0.3em] text-emerald-300">
                            {sig.signal?.type || 'trigger'} • confidence {Number(sig.signal?.confidence || 0).toFixed(2)}
                          </p>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

                <div className="rounded-xl border border-white/10 bg-black/20 p-4">
                  <h5 className="text-xs font-semibold uppercase tracking-[0.3em] text-rose-300">Sell Signals</h5>
                  {sellSignals.length === 0 ? (
                    <p className="mt-3 text-xs text-slate-400">No sell signals for the current window.</p>
                  ) : (
                    <ul className="mt-3 space-y-2 text-xs text-slate-200">
                      {sellSignals.map((sig) => (
                        <li key={sig.rule_id} className="rounded-lg border border-rose-500/20 bg-rose-500/10 px-3 py-2">
                          <p className="font-semibold">{sig.rule_name}</p>
                          <p className="text-[10px] uppercase tracking-[0.3em] text-rose-300">
                            {sig.signal?.type || 'trigger'} • confidence {Number(sig.signal?.confidence || 0).toFixed(2)}
                          </p>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </div>
            )}

            {loadingSignals && (
              <p className="text-xs text-slate-400">Collecting signal data…</p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

const StrategyTab = ({ chartId }) => {
  const [strategies, setStrategies] = useState([])
  const [indicators, setIndicators] = useState([])
  const [loading, setLoading] = useState(true)
  const [status, setStatus] = useState(null)
  const [modalOpen, setModalOpen] = useState(false)
  const [editing, setEditing] = useState(null)
  const [saving, setSaving] = useState(false)
  const [expandedId, setExpandedId] = useState(null)
  const [signalResults, setSignalResults] = useState({})
  const [signalLoading, setSignalLoading] = useState({})

  const { getChart } = useChartState()
  const chartState = getChart(chartId)

  const logger = useMemo(() => createLogger('StrategyTab', { chartId }), [chartId])

  useEffect(() => {
    let isMounted = true
    async function bootstrap() {
      setLoading(true)
      try {
        const [strategyPayload, indicatorPayload] = await Promise.all([
          fetchStrategies(),
          fetchIndicators(),
        ])
        if (!isMounted) return
        setStrategies(Array.isArray(strategyPayload) ? strategyPayload : [])
        setIndicators(Array.isArray(indicatorPayload) ? indicatorPayload : [])
        logger.info('strategy_tab_bootstrap_complete', {
          strategies: strategyPayload?.length ?? 0,
          indicators: indicatorPayload?.length ?? 0,
        })
      } catch (err) {
        if (!isMounted) return
        logger.error('strategy_tab_bootstrap_failed', err)
        setStatus({ type: 'error', message: err.message || 'Failed to load strategies' })
      } finally {
        if (isMounted) setLoading(false)
      }
    }
    bootstrap()
    return () => {
      isMounted = false
    }
  }, [logger])

  const upsertStrategy = (payload) => {
    setStrategies((prev) => {
      const idx = prev.findIndex((item) => item.id === payload.id)
      if (idx === -1) return [...prev, payload]
      const next = [...prev]
      next[idx] = payload
      return next
    })
  }

  const removeStrategy = (strategyId) => {
    setStrategies((prev) => prev.filter((item) => item.id !== strategyId))
    setSignalResults((prev) => {
      const clone = { ...prev }
      delete clone[strategyId]
      return clone
    })
    setSignalLoading((prev) => {
      const clone = { ...prev }
      delete clone[strategyId]
      return clone
    })
  }

  const handleCreateStrategy = async () => {
    setEditing(null)
    setModalOpen(true)
  }

  const handleEditStrategy = (strategy) => {
    setEditing(strategy)
    setModalOpen(true)
  }

  const handleSubmitStrategy = async (formPayload) => {
    setSaving(true)
    try {
      if (editing) {
        const payload = await updateStrategy(editing.id, formPayload)
        upsertStrategy(payload)
        setStatus({ type: 'success', message: 'Strategy updated successfully.' })
      } else {
        const payload = await createStrategy(formPayload)
        upsertStrategy(payload)
        setStatus({ type: 'success', message: 'Strategy created successfully.' })
      }
      setModalOpen(false)
      setEditing(null)
    } catch (err) {
      logger.error('strategy_form_submit_failed', err)
      setStatus({ type: 'error', message: err.message || 'Unable to save strategy' })
    } finally {
      setSaving(false)
    }
  }

  const handleDeleteStrategy = async (strategyId) => {
    if (!strategyId) return
    try {
      await deleteStrategy(strategyId)
      removeStrategy(strategyId)
      setStatus({ type: 'success', message: 'Strategy deleted.' })
      if (expandedId === strategyId) {
        setExpandedId(null)
      }
    } catch (err) {
      logger.error('strategy_delete_failed', err)
      setStatus({ type: 'error', message: err.message || 'Unable to delete strategy' })
    }
  }

  const handleIndicatorToggle = async (strategyId, indicatorId, checked) => {
    try {
      const payload = checked
        ? await attachStrategyIndicator(strategyId, indicatorId)
        : await detachStrategyIndicator(strategyId, indicatorId)
      upsertStrategy(payload)
      setStatus({ type: 'success', message: 'Indicator assignment updated.' })
    } catch (err) {
      logger.error('strategy_indicator_toggle_failed', err)
      setStatus({ type: 'error', message: err.message || 'Unable to update indicator assignment' })
    }
  }

  const handleCreateRule = async (strategyId, payload) => {
    try {
      const record = await createStrategyRule(strategyId, payload)
      upsertStrategy(record)
      setStatus({ type: 'success', message: 'Rule created.' })
    } catch (err) {
      logger.error('strategy_rule_create_failed', err)
      setStatus({ type: 'error', message: err.message || 'Unable to create rule' })
    }
  }

  const handleUpdateRule = async (strategyId, ruleId, payload) => {
    try {
      const record = await updateStrategyRule(strategyId, ruleId, payload)
      upsertStrategy(record)
      setStatus({ type: 'success', message: 'Rule updated.' })
    } catch (err) {
      logger.error('strategy_rule_update_failed', err)
      setStatus({ type: 'error', message: err.message || 'Unable to update rule' })
    }
  }

  const handleDeleteRule = async (strategyId, ruleId) => {
    try {
      const record = await deleteStrategyRule(strategyId, ruleId)
      upsertStrategy(record)
      setStatus({ type: 'success', message: 'Rule deleted.' })
    } catch (err) {
      logger.error('strategy_rule_delete_failed', err)
      setStatus({ type: 'error', message: err.message || 'Unable to delete rule' })
    }
  }

  const [start, end] = chartState?.dateRange || []
  const toISO = (value) => (typeof value === 'string' ? value : value?.toISOString?.())
  const startISO = toISO(start)
  const endISO = toISO(end)

  const handleGenerateSignals = async (strategyId) => {
    if (!chartState?.symbol || !chartState?.interval) {
      setStatus({ type: 'error', message: 'Select a symbol and timeframe on the chart before generating signals.' })
      return
    }
    setSignalLoading((prev) => ({ ...prev, [strategyId]: true }))
    try {
      const payload = await generateStrategySignals(strategyId, {
        start: startISO,
        end: endISO,
        interval: chartState.interval,
        symbol: chartState.symbol,
      })
      setSignalResults((prev) => ({ ...prev, [strategyId]: payload }))
      setStatus({ type: 'success', message: 'Signal evaluation completed.' })
    } catch (err) {
      logger.error('strategy_signal_generation_failed', err)
      setStatus({ type: 'error', message: err.message || 'Failed to generate signals' })
    } finally {
      setSignalLoading((prev) => ({ ...prev, [strategyId]: false }))
    }
  }

  return (
    <Fragment>
      <div className="flex flex-col gap-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">Strategy Blueprints</h2>
            <p className="text-sm text-slate-400">
              Create reusable playbooks, attach indicators, and stack rule engines to emit buy/sell calls.
            </p>
          </div>
          <button
            onClick={handleCreateStrategy}
            className="rounded-full bg-[color:var(--accent-alpha-70)] px-5 py-2 text-xs font-semibold uppercase tracking-[0.3em] text-black shadow-lg shadow-[color:var(--accent-shadow-strong)]"
          >
            New Strategy
          </button>
        </div>

        {status && (
          <div
            className={`rounded-xl border px-4 py-3 text-xs ${
              status.type === 'error'
                ? 'border-red-500/40 bg-red-500/10 text-red-200'
                : 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200'
            }`}
          >
            {status.message}
          </div>
        )}

        {loading ? (
          <div className="rounded-2xl border border-white/10 bg-black/20 p-6 text-sm text-slate-400">
            Loading strategies…
          </div>
        ) : strategies.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-white/10 bg-black/20 p-6 text-sm text-slate-400">
            No strategies yet. Create one to start wiring indicator outputs into actionable flows.
          </div>
        ) : (
          <div className="space-y-4">
            {strategies.map((strategy) => (
              <StrategyCard
                key={strategy.id}
                strategy={strategy}
                isExpanded={expandedId === strategy.id}
                onToggleExpand={(id) => setExpandedId((prev) => (prev === id ? null : id))}
                onEdit={handleEditStrategy}
                onDelete={handleDeleteStrategy}
                availableIndicators={indicators}
                onToggleIndicator={handleIndicatorToggle}
                onCreateRule={handleCreateRule}
                onUpdateRule={handleUpdateRule}
                onDeleteRule={handleDeleteRule}
                onGenerateSignals={handleGenerateSignals}
                signalResult={signalResults[strategy.id]}
                loadingSignals={signalLoading[strategy.id]}
              />
            ))}
          </div>
        )}
      </div>

      <StrategyForm
        open={modalOpen}
        onClose={() => {
          setModalOpen(false)
          setEditing(null)
        }}
        onSubmit={handleSubmitStrategy}
        initialValues={editing}
        submitting={saving}
      />
    </Fragment>
  )
}

export default StrategyTab

