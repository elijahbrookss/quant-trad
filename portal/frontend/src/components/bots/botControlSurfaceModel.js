import {
  formatLifecyclePhaseLabel,
  formatRelativeTime,
  getBotCardDisplayState,
} from './botStatusModel.js'
import { symbolsFromInstrumentSlots } from '../../utils/instrumentSymbols.js'

function shortRunId(runId) {
  const normalized = String(runId || '').trim()
  return normalized ? normalized.slice(0, 8) : 'pending'
}

function formatClockTime(value) {
  if (!value) return '—'
  const epochMs = Date.parse(String(value))
  if (!Number.isFinite(epochMs)) return '—'
  return new Intl.DateTimeFormat(undefined, {
    hour: 'numeric',
    minute: '2-digit',
  }).format(new Date(epochMs))
}

function strategyFor(bot, strategyLookup) {
  return bot?.strategy_id ? strategyLookup?.get(bot.strategy_id) || null : null
}

function describeTimeframe(bot, strategyLookup) {
  const strategy = strategyFor(bot, strategyLookup)
  const timeframe = String(strategy?.timeframe || bot?.timeframe || '').trim()
  return timeframe ? timeframe.toUpperCase() : '—'
}

function describeSymbols(bot, strategyLookup) {
  const strategy = strategyFor(bot, strategyLookup)
  const symbols = strategy ? symbolsFromInstrumentSlots(strategy.instrument_slots) : []
  if (!symbols.length) return { label: '—', title: undefined }
  const visible = symbols.slice(0, 4)
  const extra = symbols.length - visible.length
  return {
    label: visible.join(', ') + (extra > 0 ? ` +${extra}` : ''),
    title: symbols.join(', '),
  }
}

function describeExecution(bot) {
  const runType = String(bot?.run_type || 'backtest').trim().toLowerCase()
  const mode = String(bot?.mode || '').trim().toLowerCase()
  if (runType === 'backtest') {
    if (mode === 'instant') return 'Backtest · Fast'
    if (mode === 'walk-forward') return 'Backtest · Walk-forward'
    return 'Backtest'
  }
  if (runType === 'paper' || runType === 'paper_trade' || runType === 'sim_trade') {
    return 'Paper'
  }
  return 'Live'
}

function describeActivity(display, bot, nowEpochMs) {
  const lastEventAt = display?.lifecycle?.updatedAt || bot?.updated_at || bot?.last_run_at || null
  const startedAt = display?.startedAt || null
  const endedAt = display?.endedAt || null
  const statusKey = display?.statusKey || 'stopped'

  if (statusKey === 'running' || statusKey === 'degraded' || statusKey === 'paused') {
    return {
      label: 'Started',
      value: formatRelativeTime(startedAt || lastEventAt, { nowEpochMs }) || '—',
      title: startedAt || lastEventAt || undefined,
    }
  }
  if (statusKey === 'starting') {
    return {
      label: 'Last event',
      value: formatRelativeTime(lastEventAt, { nowEpochMs }) || '—',
      title: lastEventAt || undefined,
    }
  }
  if (statusKey === 'failed_start' || statusKey === 'crashed') {
    return {
      label: 'Failed',
      value: formatRelativeTime(lastEventAt, { nowEpochMs }) || '—',
      title: lastEventAt || undefined,
    }
  }
  if (statusKey === 'completed') {
    return {
      label: 'Completed',
      value: formatRelativeTime(endedAt || lastEventAt, { nowEpochMs }) || '—',
      title: endedAt || lastEventAt || undefined,
    }
  }
  return {
    label: 'Updated',
    value: formatRelativeTime(lastEventAt, { nowEpochMs }) || '—',
    title: lastEventAt || undefined,
  }
}

export function buildBotCardViewModel(
  bot,
  { strategyLookup = new Map(), nowEpochMs = Date.now(), pendingStart = false } = {},
) {
  const display = getBotCardDisplayState(bot, { nowEpochMs, pendingStart })
  const strategyLabel = String(strategyFor(bot, strategyLookup)?.name || bot?.strategy_id || '—').trim() || '—'
  const symbolInfo = describeSymbols(bot, strategyLookup)
  const activity = describeActivity(display, bot, nowEpochMs)
  const phaseLabel = formatLifecyclePhaseLabel(display?.lifecycle?.phase)
  const statusKey = display.statusKey
  const metaItems = [
    { key: 'symbols', label: 'Symbols', value: symbolInfo.label, title: symbolInfo.title, mono: true },
    { key: 'timeframe', label: 'Timeframe', value: describeTimeframe(bot, strategyLookup), mono: true },
    { key: 'execution', label: 'Execution', value: describeExecution(bot), mono: false },
  ]
  if (display.runId) {
    metaItems.push({
      key: 'run',
      label: 'Run',
      value: shortRunId(display.runId),
      title: display.runId,
      mono: true,
    })
  }

  const contextItems = [
    { key: 'phase', label: 'Phase', value: phaseLabel, mono: false },
    { key: 'activity', label: activity.label, value: activity.value, mono: false, title: activity.title },
  ]
  if (statusKey === 'failed_start' || statusKey === 'crashed') {
    contextItems.push({
      key: 'next',
      label: 'Next',
      value: 'View diagnostics',
      mono: false,
    })
  }

  return {
    display,
    strategyLabel,
    symbolsLabel: symbolInfo.label,
    symbolsTitle: symbolInfo.title,
    statusLabel: display.displayStatus,
    statusDetail: display.detail,
    metaItems,
    contextItems,
  }
}

export function buildBotFleetSummary(
  bots,
  { nowEpochMs = Date.now(), pendingStartId = null } = {},
) {
  const summary = {
    starting: 0,
    failed: 0,
    live: 0,
    idle: 0,
    total: Array.isArray(bots) ? bots.length : 0,
    lastUpdatedAt: null,
    lastUpdatedLabel: '—',
    items: [],
  }
  if (!Array.isArray(bots) || bots.length === 0) {
    summary.items = [
      { key: 'starting', label: 'Starting', value: 0 },
      { key: 'failed', label: 'Failed', value: 0 },
      { key: 'live', label: 'Live', value: 0 },
      { key: 'idle', label: 'Idle', value: 0 },
      { key: 'updated', label: 'Last update', value: '—' },
    ]
    return summary
  }

  let lastUpdatedEpochMs = 0
  let lastUpdatedAt = null

  for (const bot of bots) {
    const display = getBotCardDisplayState(bot, {
      nowEpochMs,
      pendingStart: pendingStartId === bot?.id,
    })
    if (display.statusKey === 'starting') summary.starting += 1
    else if (display.statusKey === 'failed_start' || display.statusKey === 'crashed') summary.failed += 1
    else if (display.statusKey === 'running' || display.statusKey === 'degraded' || display.statusKey === 'paused') summary.live += 1
    else summary.idle += 1

    const candidate = display?.lifecycle?.updatedAt || bot?.updated_at || bot?.last_run_at || null
    const candidateEpochMs = Date.parse(String(candidate || ''))
    if (Number.isFinite(candidateEpochMs) && candidateEpochMs >= lastUpdatedEpochMs) {
      lastUpdatedEpochMs = candidateEpochMs
      lastUpdatedAt = candidate
    }
  }

  summary.lastUpdatedAt = lastUpdatedAt
  summary.lastUpdatedLabel = formatClockTime(lastUpdatedAt)
  summary.items = [
    { key: 'starting', label: 'Starting', value: summary.starting },
    { key: 'failed', label: 'Failed', value: summary.failed },
    { key: 'live', label: 'Live', value: summary.live },
    { key: 'idle', label: 'Idle', value: summary.idle },
    { key: 'updated', label: 'Last update', value: summary.lastUpdatedLabel },
  ]
  return summary
}
