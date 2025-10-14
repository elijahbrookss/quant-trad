import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  fetchStrategies,
  saveStrategy,
  uploadStrategyYaml,
  fetchStrategyOrderSignals,
  requestStrategyBacktest,
  launchStrategy,
} from '../adapters/strategy.adapter.js'
import { useChartState } from '../contexts/ChartStateContext.jsx'
import { createLogger } from '../utils/logger.js'

const DEFAULT_FORM = {
  name: '',
  description: '',
  symbol: '',
  timeframe: '',
}

const EMPTY_SELECTION = {
  indicators: [],
  signals: {},
}

const SIGNAL_LABEL_FALLBACK = (signal, index) => {
  if (!signal || typeof signal !== 'object') return `Signal ${index + 1}`
  return signal.label || signal.name || signal.id || signal.type || `Signal ${index + 1}`
}

export const StrategyBuilder = ({ chartId }) => {
  const { getChart } = useChartState()
  const chartState = getChart(chartId) || {}
  const indicators = Array.isArray(chartState.indicators) ? chartState.indicators : []
  const signalResults = chartState.signalResults || {}

  const [form, setForm] = useState(DEFAULT_FORM)
  const [selection, setSelection] = useState(EMPTY_SELECTION)
  const [strategies, setStrategies] = useState([])
  const [strategyId, setStrategyId] = useState(null)
  const [orderSignals, setOrderSignals] = useState([])
  const [yamlSummary, setYamlSummary] = useState([])
  const [statusMessage, setStatusMessage] = useState('')
  const [isSaving, setIsSaving] = useState(false)
  const [isGenerating, setIsGenerating] = useState(false)
  const [backtestStatus, setBacktestStatus] = useState(null)
  const [launchStatus, setLaunchStatus] = useState(null)

  const logger = useMemo(() => createLogger('StrategyBuilder', { chartId }), [chartId])
  const { info, warn, error } = logger

  useEffect(() => {
    setForm((prev) => ({
      ...prev,
      symbol: chartState.symbol || prev.symbol,
      timeframe: chartState.interval || prev.timeframe,
    }))
  }, [chartState.symbol, chartState.interval])

  useEffect(() => {
    const nextIndicators = selection.indicators.filter((id) => indicators.some((ind) => ind.id === id))
    if (nextIndicators.length !== selection.indicators.length) {
      setSelection((prev) => ({
        ...prev,
        indicators: nextIndicators,
        signals: Object.fromEntries(Object.entries(prev.signals).filter(([key]) => nextIndicators.includes(key))),
      }))
    }
  }, [indicators, selection.indicators, selection.signals])

  useEffect(() => {
    let isMounted = true
    ;(async () => {
      try {
        const response = await fetchStrategies()
        if (!isMounted) return
        setStrategies(Array.isArray(response?.strategies) ? response.strategies : [])
      } catch (err) {
        warn('strategy_fetch_failed', { message: err?.message })
      }
    })()
    return () => {
      isMounted = false
    }
  }, [warn])

  const indicatorOptions = useMemo(
    () => indicators.map((ind) => ({ id: ind.id, name: ind.name || ind.type || ind.id, type: ind.type })),
    [indicators],
  )

  const toggleIndicator = useCallback(
    (indicatorId) => {
      setSelection((prev) => {
        const isSelected = prev.indicators.includes(indicatorId)
        if (isSelected) {
          const nextIndicators = prev.indicators.filter((id) => id !== indicatorId)
          const nextSignals = { ...prev.signals }
          delete nextSignals[indicatorId]
          return { indicators: nextIndicators, signals: nextSignals }
        }
        return { indicators: [...prev.indicators, indicatorId], signals: { ...prev.signals } }
      })
    },
    [],
  )

  const toggleSignal = useCallback((indicatorId, signalName) => {
    setSelection((prev) => {
      const indicatorSignals = prev.signals[indicatorId] || []
      const exists = indicatorSignals.includes(signalName)
      const nextSignals = { ...prev.signals }
      nextSignals[indicatorId] = exists
        ? indicatorSignals.filter((name) => name !== signalName)
        : [...indicatorSignals, signalName]
      return { indicators: prev.indicators, signals: nextSignals }
    })
  }, [])

  const onFieldChange = (field) => (event) => {
    const value = event?.target?.value ?? ''
    setForm((prev) => ({ ...prev, [field]: value }))
  }

  const handleSave = useCallback(async () => {
    if (!form.name) {
      setStatusMessage('Please provide a strategy name.')
      return
    }

    setIsSaving(true)
    setStatusMessage('Saving strategy...')
    try {
      const payload = {
        strategy_id: strategyId || undefined,
        name: form.name,
        symbol: form.symbol,
        timeframe: form.timeframe,
        description: form.description,
        indicators: indicatorOptions.filter((ind) => selection.indicators.includes(ind.id)),
        selected_signals: selection.signals,
      }
      const response = await saveStrategy(payload)
      const saved = response?.strategy
      if (saved) {
        setStrategyId(saved.strategy_id)
        setStatusMessage('Strategy saved successfully.')
        setStrategies((prev) => {
          const others = prev.filter((item) => item.strategy_id !== saved.strategy_id)
          return [...others, saved]
        })
        info('strategy_saved', { strategyId: saved.strategy_id })
      } else {
        setStatusMessage('Strategy saved, but response was empty.')
      }
    } catch (err) {
      setStatusMessage(err?.message || 'Failed to save strategy.')
      error('strategy_save_failed', { message: err?.message }, err)
    } finally {
      setIsSaving(false)
    }
  }, [error, form.description, form.name, form.symbol, form.timeframe, indicatorOptions, info, selection.indicators, selection.signals, strategyId])

  const handleYamlUpload = useCallback(
    async (event) => {
      if (!strategyId) {
        setStatusMessage('Save the strategy before uploading YAML.')
        return
      }

      const input = event?.target
      const file = input?.files?.[0]
      if (!file) return

      try {
        const text = await file.text()
        const response = await uploadStrategyYaml(strategyId, text)
        setYamlSummary(Array.isArray(response?.yaml_summary) ? response.yaml_summary : [])
        setStatusMessage('YAML uploaded and parsed successfully.')
        info('strategy_yaml_uploaded', { strategyId })
      } catch (err) {
        setStatusMessage(err?.message || 'Failed to upload YAML.')
        error('strategy_yaml_upload_failed', { message: err?.message }, err)
      } finally {
        if (input) {
          input.value = ''
        }
      }
    },
    [error, info, strategyId],
  )

  const handleGenerateOrderSignals = useCallback(async () => {
    if (!strategyId) {
      setStatusMessage('Save the strategy before generating order signals.')
      return
    }

    setIsGenerating(true)
    setStatusMessage('Generating order signals...')
    try {
      const response = await fetchStrategyOrderSignals(strategyId)
      const signals = Array.isArray(response?.order_signals) ? response.order_signals : []
      setOrderSignals(signals)
      setStatusMessage(signals.length ? 'Order signals generated.' : 'No order signals returned for this configuration.')
      info('strategy_order_signals_generated', { count: signals.length })
    } catch (err) {
      setStatusMessage(err?.message || 'Failed to generate order signals.')
      error('strategy_order_signal_failed', { message: err?.message }, err)
    } finally {
      setIsGenerating(false)
    }
  }, [error, info, strategyId])

  const handleBacktest = useCallback(async () => {
    if (!strategyId) {
      setStatusMessage('Save the strategy before requesting a backtest.')
      return
    }
    try {
      const response = await requestStrategyBacktest(strategyId, {
        start: chartState?.dateRange?.[0],
        end: chartState?.dateRange?.[1],
        timeframe: chartState?.interval,
      })
      setBacktestStatus(response)
      setStatusMessage('Backtest request recorded.')
      info('strategy_backtest_requested', { strategyId })
    } catch (err) {
      setStatusMessage(err?.message || 'Failed to request backtest.')
      error('strategy_backtest_failed', { message: err?.message }, err)
    }
  }, [chartState?.dateRange, chartState?.interval, error, info, strategyId])

  const handleLaunch = useCallback(async () => {
    if (!strategyId) {
      setStatusMessage('Save the strategy before launching.')
      return
    }
    try {
      const response = await launchStrategy(strategyId, { mode: 'simulation' })
      setLaunchStatus(response)
      setStatusMessage('Launch request queued.')
      info('strategy_launch_requested', { strategyId })
    } catch (err) {
      setStatusMessage(err?.message || 'Failed to request launch.')
      error('strategy_launch_failed', { message: err?.message }, err)
    }
  }, [error, info, strategyId])

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-2">
        <div className="space-y-3">
          <h3 className="text-sm font-semibold text-slate-100">Strategy details</h3>
          <label className="block text-xs uppercase tracking-[0.2em] text-slate-400">
            Name
            <input
              type="text"
              className="mt-1 w-full rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-70)] focus:outline-none"
              value={form.name}
              onChange={onFieldChange('name')}
              placeholder="QuantLab Breakout"
            />
          </label>
          <label className="block text-xs uppercase tracking-[0.2em] text-slate-400">
            Description
            <textarea
              className="mt-1 w-full rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-70)] focus:outline-none"
              value={form.description}
              onChange={onFieldChange('description')}
              placeholder="Optional notes about playbook variants"
              rows={3}
            />
          </label>
          <div className="grid grid-cols-2 gap-3">
            <label className="block text-xs uppercase tracking-[0.2em] text-slate-400">
              Symbol
              <input
                type="text"
                className="mt-1 w-full rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-70)] focus:outline-none"
                value={form.symbol}
                onChange={onFieldChange('symbol')}
                placeholder="BTCUSD"
              />
            </label>
            <label className="block text-xs uppercase tracking-[0.2em] text-slate-400">
              Timeframe
              <input
                type="text"
                className="mt-1 w-full rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-sm text-white focus:border-[color:var(--accent-alpha-70)] focus:outline-none"
                value={form.timeframe}
                onChange={onFieldChange('timeframe')}
                placeholder="1h"
              />
            </label>
          </div>
        </div>

        <div className="space-y-3">
          <h3 className="text-sm font-semibold text-slate-100">Indicators and signals</h3>
          <div className="space-y-2 rounded-xl border border-white/10 bg-[#141722]/80 p-3">
            {indicatorOptions.length === 0 && (
              <p className="text-xs text-slate-400">No indicators loaded for this chart yet. Configure them in the Indicators tab.</p>
            )}
            {indicatorOptions.map((indicator) => {
              const isActive = selection.indicators.includes(indicator.id)
              const availableSignals = signalResults[indicator.id] || []
              return (
                <div key={indicator.id} className="rounded-lg border border-white/10 bg-white/5 p-3">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <p className="text-sm font-medium text-slate-100">{indicator.name}</p>
                      <p className="text-xs uppercase tracking-[0.2em] text-slate-500">{indicator.type || 'Custom indicator'}</p>
                    </div>
                    <button
                      type="button"
                      onClick={() => toggleIndicator(indicator.id)}
                      className={`rounded-full px-3 py-1 text-xs transition ${
                        isActive
                          ? 'bg-[color:var(--accent-alpha-30)] text-[color:var(--accent-text-strong)]'
                          : 'bg-white/10 text-slate-300 hover:bg-white/20'
                      }`}
                    >
                      {isActive ? 'Selected' : 'Select'}
                    </button>
                  </div>
                  {isActive && (
                    <div className="mt-3 space-y-2">
                      <p className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Signals</p>
                      {availableSignals.length === 0 && (
                        <p className="text-xs text-slate-400">Generate indicator signals first to enable selection.</p>
                      )}
                      <div className="flex flex-wrap gap-2">
                        {availableSignals.map((signal, idx) => {
                          const label = SIGNAL_LABEL_FALLBACK(signal, idx)
                          const name = signal?.name || signal?.id || label
                          const isChecked = selection.signals[indicator.id]?.includes(name)
                          return (
                            <button
                              key={`${indicator.id}-${name}`}
                              type="button"
                              onClick={() => toggleSignal(indicator.id, name)}
                              className={`rounded-full border px-3 py-1 text-xs transition ${
                                isChecked
                                  ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)]'
                                  : 'border-white/10 bg-white/5 text-slate-300 hover:border-white/30'
                              }`}
                            >
                              {label}
                            </button>
                          )
                        })}
                      </div>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-white/10 bg-[#131722] p-4">
        <div className="flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={handleSave}
            className="rounded-full bg-[color:var(--accent-alpha-30)] px-4 py-2 text-xs font-semibold text-[color:var(--accent-text-strong)] shadow-[0_12px_32px_-18px_var(--accent-shadow-strong)] transition hover:bg-[color:var(--accent-alpha-40)]"
            disabled={isSaving}
          >
            {isSaving ? 'Saving...' : 'Save strategy'}
          </button>
          <label className="text-xs text-slate-300">
            <span className="mr-2 inline-flex rounded-full border border-white/10 px-3 py-2 text-xs uppercase tracking-[0.2em] text-slate-400">Upload YAML</span>
            <input type="file" accept=".yaml,.yml" className="hidden" onChange={handleYamlUpload} />
          </label>
          <button
            type="button"
            onClick={handleGenerateOrderSignals}
            className="rounded-full border border-white/10 px-4 py-2 text-xs text-slate-200 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-20)]"
            disabled={isGenerating}
          >
            {isGenerating ? 'Generating...' : 'Generate order signals'}
          </button>
          <button
            type="button"
            onClick={handleBacktest}
            className="rounded-full border border-white/10 px-4 py-2 text-xs text-slate-200 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-20)]"
          >
            Backtest placeholder
          </button>
          <button
            type="button"
            onClick={handleLaunch}
            className="rounded-full border border-white/10 px-4 py-2 text-xs text-slate-200 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-20)]"
          >
            Launch placeholder
          </button>
        </div>
        {statusMessage && <p className="mt-3 text-xs text-slate-300">{statusMessage}</p>}
        {yamlSummary.length > 0 && (
          <p className="mt-2 text-xs text-slate-400">YAML keys: {yamlSummary.join(', ')}</p>
        )}
      </div>

      {orderSignals.length > 0 && (
        <div className="space-y-3 rounded-2xl border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-10)] p-4">
          <h4 className="text-sm font-semibold text-[color:var(--accent-text-strong)]">Generated order signals</h4>
          <ul className="space-y-2 text-xs text-slate-200">
            {orderSignals.map((sig) => (
              <li key={sig.id} className="rounded-lg border border-white/10 bg-white/5 p-3">
                <div className="flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.2em] text-slate-400">
                  <span>{sig.indicator_name}</span>
                  <span className="text-slate-500">/</span>
                  <span>{sig.signal}</span>
                  <span className="rounded-full bg-white/10 px-2 py-1 text-[10px] text-slate-200">{sig.action}</span>
                </div>
                {sig.tags?.length ? (
                  <p className="mt-2 text-[11px] text-slate-400">Tags: {sig.tags.join(', ')}</p>
                ) : null}
                {sig.stops && Object.keys(sig.stops).length ? (
                  <p className="mt-1 text-[11px] text-slate-400">
                    Stops: {Object.entries(sig.stops)
                      .map(([key, value]) => `${key}: ${value}`)
                      .join(', ')}
                  </p>
                ) : null}
              </li>
            ))}
          </ul>
        </div>
      )}

      {(backtestStatus || launchStatus) && (
        <div className="grid gap-3 md:grid-cols-2">
          {backtestStatus && (
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-xs text-slate-200">
              <h4 className="text-sm font-semibold text-slate-100">Backtest placeholder</h4>
              <p className="mt-2 text-slate-400">Status: {backtestStatus.status}</p>
              <p className="text-slate-400">Requested: {backtestStatus.requested_at}</p>
            </div>
          )}
          {launchStatus && (
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-xs text-slate-200">
              <h4 className="text-sm font-semibold text-slate-100">Launch placeholder</h4>
              <p className="mt-2 text-slate-400">Status: {launchStatus.status}</p>
              <p className="text-slate-400">Mode: {launchStatus.mode}</p>
              <p className="text-slate-400">Requested: {launchStatus.requested_at}</p>
            </div>
          )}
        </div>
      )}

      <div className="rounded-2xl border border-white/10 bg-[#131722] p-4">
        <h4 className="text-sm font-semibold text-slate-100">Saved blueprints</h4>
        {strategies.length === 0 ? (
          <p className="mt-2 text-xs text-slate-400">No saved strategies yet.</p>
        ) : (
          <ul className="mt-3 space-y-2 text-xs text-slate-300">
            {strategies.map((item) => (
              <li key={item.strategy_id} className="rounded-lg border border-white/10 bg-white/5 p-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <p className="text-sm font-semibold text-slate-100">{item.name}</p>
                    <p className="text-[11px] uppercase tracking-[0.3em] text-slate-500">
                      {item.symbol || 'N/A'} · {item.timeframe || 'N/A'}
                    </p>
                  </div>
                  <button
                    type="button"
                    className="rounded-full border border-white/10 px-3 py-1 text-xs text-slate-300 hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-20)]"
                    onClick={() => {
                      setStrategyId(item.strategy_id)
                      setForm({
                        name: item.name || '',
                        description: item.description || '',
                        symbol: item.symbol || '',
                        timeframe: item.timeframe || '',
                      })
                      setSelection({
                        indicators: Array.isArray(item.indicators) ? item.indicators.map((ind) => ind.id).filter(Boolean) : [],
                        signals: item.selected_signals || {},
                      })
                      setOrderSignals([])
                      setYamlSummary(item.yaml_config ? Object.keys(item.yaml_config) : [])
                      setStatusMessage('Loaded strategy for editing.')
                      info('strategy_loaded', { strategyId: item.strategy_id })
                    }}
                  >
                    Load
                  </button>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}

