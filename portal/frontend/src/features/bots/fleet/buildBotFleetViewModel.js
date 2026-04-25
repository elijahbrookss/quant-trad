import {
  formatLifecyclePhaseLabel,
  formatRelativeTime,
  getBotCardDisplayState,
} from '../state/botRuntimeStatus.js'
import { symbolsFromInstrumentSlots } from '../../../utils/instrumentSymbols.js'
import { mapRunToViewModel } from '../viewModels/runViewModel.js'

function truncateIdentifier(value, { head = 7, tail = 5 } = {}) {
  const normalized = String(value || '').trim()
  if (!normalized) return ''
  if (normalized.length <= head + tail + 1) return normalized
  return `${normalized.slice(0, head)}…${normalized.slice(-tail)}`
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

function symbolsFor(bot, strategyLookup) {
  const strategy = strategyFor(bot, strategyLookup)
  return strategy ? symbolsFromInstrumentSlots(strategy.instrument_slots) : []
}

function describeTimeframe(bot, strategyLookup) {
  const strategy = strategyFor(bot, strategyLookup)
  const timeframe = String(strategy?.timeframe || bot?.timeframe || '').trim()
  return timeframe ? timeframe.toUpperCase() : '—'
}

function describeSymbols(bot, strategyLookup) {
  const symbols = symbolsFor(bot, strategyLookup)
  if (!symbols.length) {
    return {
      count: 0,
      summaryLabel: 'No symbols',
      trackedLabel: '0 symbols tracked',
      preview: '—',
      title: undefined,
    }
  }
  const visible = symbols.slice(0, 4)
  const extra = symbols.length - visible.length
  const noun = symbols.length === 1 ? 'symbol' : 'symbols'
  return {
    count: symbols.length,
    summaryLabel: `${symbols.length} ${noun}`,
    trackedLabel: `${symbols.length} ${symbols.length === 1 ? 'symbol' : 'symbols'} tracked`,
    preview: visible.join(', ') + (extra > 0 ? ` +${extra} more` : ''),
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

function buildHeaderMetaText(strategyLabel, executionLabel, timeframe) {
  const parts = [
    String(strategyLabel || '').trim(),
    ...String(executionLabel || '')
      .split('·')
      .map((part) => part.trim())
      .filter(Boolean),
    String(timeframe || '').trim(),
  ].filter((part) => part && part !== '—')

  return parts.join(' · ') || '—'
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

function firstFiniteNumber(candidates) {
  for (const candidate of candidates) {
    const value = Number(candidate)
    if (Number.isFinite(value)) return value
  }
  return null
}

function readNestedNumber(source, paths) {
  for (const path of paths) {
    let cursor = source
    let missing = false
    for (const segment of path) {
      if (!cursor || typeof cursor !== 'object' || !(segment in cursor)) {
        missing = true
        break
      }
      cursor = cursor[segment]
    }
    if (missing) continue
    const value = Number(cursor)
    if (Number.isFinite(value)) return value
  }
  return null
}

function formatSignedNumber(value, digits = 2) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  if (value === 0) return value.toFixed(digits)
  return `${value > 0 ? '+' : ''}${value.toFixed(digits)}`
}

function formatCountValue(value, { fallback = '—' } = {}) {
  const numeric = Number(value)
  if (Number.isFinite(numeric)) return String(Math.max(0, numeric))
  return fallback
}

function formatDuration(startedAt, endedAt = null, nowEpochMs = Date.now()) {
  const startMs = Date.parse(String(startedAt || ''))
  if (!Number.isFinite(startMs)) return null
  const endMs = endedAt ? Date.parse(String(endedAt)) : nowEpochMs
  if (!Number.isFinite(endMs) || endMs <= startMs) return null
  const elapsedSeconds = Math.max(0, Math.floor((endMs - startMs) / 1000))
  const hours = Math.floor(elapsedSeconds / 3600)
  const minutes = Math.floor((elapsedSeconds % 3600) / 60)
  const seconds = elapsedSeconds % 60

  if (hours > 0) return `${hours}h ${String(minutes).padStart(2, '0')}m`
  if (minutes > 0) return `${minutes}m ${String(seconds).padStart(2, '0')}s`
  return `${seconds}s`
}

function describeHeartbeat(state) {
  const normalized = String(state || '').trim().toLowerCase()
  if (!normalized || normalized === 'inactive') return 'Offline'
  if (normalized === 'fresh') return 'Fresh'
  if (normalized === 'stale') return 'Stale'
  return normalized
    .split('_')
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ')
}

function describeContainer(state) {
  const normalized = String(state || '').trim().toLowerCase()
  if (!normalized) return 'Unknown'
  return normalized
    .split('_')
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ')
}

function describeWorkerUsage(display) {
  const activeWorkers = Number(display?.lifecycle?.telemetry?.active_workers || 0)
  const requestedWorkers = Number(display?.lifecycle?.telemetry?.worker_count || 0)
  if (activeWorkers > 0 && requestedWorkers > 0) return `${activeWorkers}/${requestedWorkers}`
  if (requestedWorkers > 0) return String(requestedWorkers)
  if (activeWorkers > 0) return String(activeWorkers)
  return '—'
}

function runtimeWarningCount(display, bot) {
  return Math.max(
    Number(display?.warningCount || 0) || 0,
    Number(bot?.runtime?.warnings?.length || 0) || 0,
    Number(bot?.lifecycle?.telemetry?.warning_count || 0) || 0,
  )
}

function openTradeCount(display) {
  return Math.max(0, Number(display?.lifecycle?.telemetry?.trade_count || 0) || 0)
}

function totalTrades(bot) {
  return firstFiniteNumber([
    readNestedNumber(bot, [['runtime', 'stats', 'total_trades']]),
    readNestedNumber(bot, [['run', 'summary', 'total_trades']]),
    readNestedNumber(bot, [['last_stats', 'total_trades']]),
    readNestedNumber(bot, [['last_run_artifact', 'summary', 'total_trades']]),
  ])
}

function netPnlValue(bot) {
  return firstFiniteNumber([
    readNestedNumber(bot, [['runtime', 'stats', 'net_pnl']]),
    readNestedNumber(bot, [['run', 'summary', 'net_pnl']]),
    readNestedNumber(bot, [['last_stats', 'net_pnl']]),
    readNestedNumber(bot, [['last_run_artifact', 'summary', 'net_pnl']]),
    readNestedNumber(bot, [['last_run_artifact', 'stats', 'net_pnl']]),
  ])
}

function actionHint(display) {
  if (display?.statusKey === 'failed_start' || display?.statusKey === 'crashed') {
    return 'Use diagnostics first, then restart if needed.'
  }
  if (display?.controls?.canOpenLens) {
    return 'Open Lens for deeper runtime state.'
  }
  if (display?.statusKey === 'starting') {
    return 'Bootstrap in progress.'
  }
  if (display?.statusKey === 'completed') {
    return 'Ready to rerun.'
  }
  return 'Ready to start.'
}

function durationFor(display, bot, nowEpochMs) {
  const statusKey = display?.statusKey || 'stopped'
  const startedAt = bot?.last_run_artifact?.started_at || display?.startedAt || null
  const endedAt = bot?.last_run_artifact?.ended_at || display?.endedAt || null

  if (['starting', 'running', 'degraded', 'paused'].includes(statusKey)) {
    return formatDuration(startedAt, null, nowEpochMs)
  }
  if (display?.isTerminal) {
    return formatDuration(startedAt, endedAt, nowEpochMs)
  }
  return null
}

function buildMetadataItem({ key, label, rawValue = '', value = '', title, mono = false, copyable = false, missing = false }) {
  return {
    key,
    label,
    value: value || (missing ? 'Missing' : '—'),
    rawValue: String(rawValue || '').trim() || null,
    title,
    mono,
    copyable,
    missing,
  }
}

function buildActivityMetadataItem(activity) {
  return buildMetadataItem({
    key: 'activity',
    label: activity?.label || 'Updated',
    rawValue: activity?.title || activity?.value || '',
    value: activity?.value || '—',
    title: activity?.title || undefined,
  })
}

function warningSummaryFor(count) {
  if (count > 0) {
    return {
      count,
      label: count === 1 ? '1 warning active' : `${count} warnings active`,
      detail: 'Runtime warnings are present on the current lifecycle telemetry.',
      tone: 'attention',
    }
  }
  return {
    count: 0,
    label: '0 warnings',
    detail: 'No runtime warnings reported.',
    tone: 'default',
  }
}

export function buildBotCardViewModel(
  bot,
  { strategyLookup = new Map(), nowEpochMs = Date.now(), pendingStart = false } = {},
) {
  const display = getBotCardDisplayState(bot, { nowEpochMs, pendingStart })
  const strategy = strategyFor(bot, strategyLookup)
  const runView = mapRunToViewModel(bot, { strategy, display })
  const strategyLabel = String(strategy?.name || bot?.strategy_id || '—').trim() || '—'
  const symbolInfo = describeSymbols(bot, strategyLookup)
  const activity = describeActivity(display, bot, nowEpochMs)
  const phaseLabel = formatLifecyclePhaseLabel(display?.lifecycle?.phase)
  const warningCount = runtimeWarningCount(display, bot)
  const warningSummary = warningSummaryFor(warningCount)
  const openTrades = runView.openTrades ?? openTradeCount(display)
  const totalTradeCount = runView.totalTrades ?? totalTrades(bot)
  const netPnl = runView.pnl ?? netPnlValue(bot)
  const workerUsage = describeWorkerUsage(display)
  const botId = String(bot?.id || '').trim()
  const runId = String(display?.runId || '').trim()
  const timeframe = describeTimeframe(bot, strategyLookup)
  const executionLabel = describeExecution(bot)
  const headerMetaText = buildHeaderMetaText(strategyLabel, executionLabel, timeframe)
  const durationLabel = durationFor(display, bot, nowEpochMs)
  const metadataItems = [
    buildMetadataItem({
      key: 'bot-id',
      label: 'Bot ID',
      rawValue: botId,
      value: botId ? truncateIdentifier(botId) : 'Missing',
      title: botId || undefined,
      mono: true,
      copyable: Boolean(botId),
      missing: !botId,
    }),
    buildMetadataItem({
      key: 'run-id',
      label: 'Run ID',
      rawValue: runId,
      value: runId ? truncateIdentifier(runId) : '—',
      title: runId || undefined,
      mono: true,
      copyable: Boolean(runId),
    }),
    buildActivityMetadataItem(activity),
    buildMetadataItem({
      key: 'duration',
      label: 'Duration',
      rawValue: durationLabel,
      value: durationLabel || '—',
      title: durationLabel || undefined,
      mono: true,
    }),
  ]
  const metricStats = [
    {
      key: 'open-trades',
      label: 'Open Trades',
      value: formatCountValue(openTrades, { fallback: '0' }),
      mono: true,
    },
    {
      key: 'total-trades',
      label: 'Total Trades',
      value: Number.isFinite(totalTradeCount) ? String(totalTradeCount) : '—',
      mono: true,
    },
    {
      key: 'warnings',
      label: 'Warnings',
      value: formatCountValue(warningCount, { fallback: '0' }),
      mono: true,
      tone: warningCount > 0 ? 'attention' : 'default',
    },
    {
      key: 'net-pnl',
      label: 'Net P&L',
      value: formatSignedNumber(netPnl),
      mono: true,
      tone: typeof netPnl === 'number' && netPnl !== 0 ? (netPnl > 0 ? 'positive' : 'danger') : 'default',
    },
  ]
  const stateFacts = display?.statusKey === 'starting' && phaseLabel && phaseLabel !== 'Idle'
    ? [
        {
          key: 'phase',
          label: 'Phase',
          value: phaseLabel,
          title: phaseLabel,
        },
      ]
    : []

  return {
    display,
    strategyLabel,
    symbolsLabel: symbolInfo.preview,
    symbolsTitle: symbolInfo.title,
    executionLabel,
    timeframeLabel: timeframe,
    headerMetaText,
    statusLabel: display.displayStatus,
    statusDetail: display.detail,
    runView,
    metadataItems,
    metricStats,
    stateFacts,
    bodyStats: [...metricStats, ...stateFacts],
    symbols: {
      count: symbolInfo.count,
      summaryLabel: symbolInfo.summaryLabel,
      trackedLabel: symbolInfo.trackedLabel,
      preview: symbolInfo.preview,
      title: symbolInfo.title,
    },
    warningSummary,
    operationalRows: [
      { key: 'phase', label: 'Phase', value: phaseLabel },
      { key: 'heartbeat', label: 'Heartbeat', value: describeHeartbeat(display?.lifecycle?.heartbeatState) },
      { key: 'container', label: 'Container', value: describeContainer(display?.containerStatus) },
      { key: 'workers', label: 'Workers', value: workerUsage, mono: true },
    ],
    actionHint: actionHint(display),
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

export function sortBots(bots) {
  return [...bots].sort((a, b) => {
    const aTime = Date.parse(a?.created_at || '') || 0
    const bTime = Date.parse(b?.created_at || '') || 0
    if (aTime !== bTime) return bTime - aTime
    return (a.name || '').localeCompare(b.name || '')
  })
}
