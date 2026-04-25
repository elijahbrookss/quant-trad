import {
  describeBotLifecycle,
  formatLifecyclePhaseLabel,
  getBotRunId,
  getBotStatus,
  normalizeBotStatus,
} from '../state/botRuntimeStatus.js'
import { normalizeSeriesKey } from '../../../components/bots/botlensProjection.js'

function formatPercent(value) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  return `${(value * 100).toFixed(1)}%`
}

function formatNumber(value, digits = 2) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  return value.toFixed(digits)
}

function formatSignedNumber(value, digits = 2) {
  if (typeof value !== 'number' || Number.isNaN(value)) return '—'
  if (value === 0) return value.toFixed(digits)
  return `${value > 0 ? '+' : ''}${value.toFixed(digits)}`
}

function formatMoment(value) {
  if (!value) return '—'
  try {
    return new Date(value).toLocaleString()
  } catch {
    return String(value)
  }
}

function formatRelativeTime(value) {
  if (!value) return 'just now'
  const timestamp = new Date(value).getTime()
  if (!Number.isFinite(timestamp)) return 'recently'
  const deltaMs = Math.max(0, Date.now() - timestamp)
  const seconds = Math.floor(deltaMs / 1000)
  if (seconds < 10) return 'just now'
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

function shortId(value, length = 8) {
  const normalized = String(value || '').trim()
  return normalized ? normalized.slice(0, length) : '—'
}

function humanizeToken(value) {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return '—'
  return normalized
    .split('_')
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(' ')
}

function formatBooleanState(value, { trueLabel = 'Yes', falseLabel = 'No' } = {}) {
  return value ? trueLabel : falseLabel
}

function warningRowTitle(warning) {
  const indicator = String(warning?.indicator_id || '').trim() || 'indicator'
  const symbol = String(warning?.symbol || '').trim()
  const title = String(warning?.title || '').trim()
  if (!title) return symbol ? `${indicator} · ${symbol}` : indicator
  return symbol ? `${indicator} · ${symbol} · ${title}` : `${indicator} · ${title}`
}

function isOpenTrade(trade) {
  if (!trade || typeof trade !== 'object') return false
  if (trade.closed_at) return false
  const status = String(trade.status || '').toLowerCase()
  if (status === 'closed' || status === 'completed' || status === 'complete') return false
  const legs = Array.isArray(trade.legs) ? trade.legs : []
  if (!legs.length) return true
  return legs.some((leg) => {
    if (!leg || typeof leg !== 'object') return false
    if (!leg.exit_time) return true
    return String(leg.status || '').toLowerCase() === 'open'
  })
}

function buildTradeChip(trade) {
  if (!trade || typeof trade !== 'object') return null
  const direction = String(trade.direction || '').toLowerCase() === 'short' ? 'short' : 'long'
  const quantityRaw = Number(
    trade?.entry_order?.contracts ?? trade?.entry_order?.quantity ?? trade?.qty ?? trade?.quantity ?? trade?.contracts,
  )
  const quantityLabel = Number.isFinite(quantityRaw) && quantityRaw > 0 ? String(Number(quantityRaw.toFixed(4))) : null
  return {
    symbol: String(trade.symbol || '—'),
    direction,
    directionLabel: direction.toUpperCase(),
    sizeLabel: quantityLabel || `${Math.max((trade.legs || []).length, 1)}x`,
    entry: trade.entry_price,
  }
}

function summarizeRun(runMeta, health) {
  if (!runMeta) return 'No active runtime attached'
  const parts = [
    runMeta.strategy_name || 'Runtime',
    normalizeBotStatus(health?.status || runMeta.status || 'idle'),
  ]
  if (runMeta.started_at) parts.push(`started ${formatMoment(runMeta.started_at)}`)
  if (runMeta.datasource || runMeta.exchange) parts.push([runMeta.datasource, runMeta.exchange].filter(Boolean).join(' · '))
  return parts.filter(Boolean).join(' · ')
}

function shouldSurfaceStatusMessage(value) {
  const normalized = String(value || '').trim().toLowerCase()
  if (!normalized) return false
  if (normalized.includes('ready')) return false
  return ['loading', 'failed', 'error', 'unavailable', 'required', 'degraded'].some((token) => normalized.includes(token))
}

function buildNotices({ statusMessage, error }) {
  const notices = []
  if (shouldSurfaceStatusMessage(statusMessage)) {
    notices.push({
      key: 'status',
      tone: 'neutral',
      message: statusMessage,
    })
  }
  if (error) {
    notices.push({
      key: 'runtime-error',
      tone: 'error',
      message: error,
    })
  }
  return notices
}

function topBarTone(status) {
  const normalized = normalizeBotStatus(status)
  if (normalized === 'running') return 'emerald'
  if (normalized === 'degraded' || normalized === 'paused' || normalized === 'telemetry_degraded') return 'amber'
  if (normalized === 'starting' || normalized === 'bootstrapping') return 'sky'
  if (['error', 'failed', 'failed_start', 'crashed', 'startup_failed'].includes(normalized)) return 'rose'
  return 'slate'
}

function buildRecentTradeRows(trades = []) {
  return (Array.isArray(trades) ? trades : []).map((trade, index) => ({
    key: String(trade?.event_id || trade?.trade_id || `${trade?.event_ts || trade?.entry_time || 'trade'}-${index}`),
    symbol: String(trade?.symbol || '—'),
    status: humanizeToken(trade?.status || 'open'),
    direction: String(trade?.direction || '').trim().toUpperCase() || '—',
    openedAt: formatMoment(trade?.event_ts || trade?.opened_at || trade?.entry_time || trade?.created_at),
    entryPrice: formatNumber(Number(trade?.entry_price), 2),
    exitPrice: formatNumber(Number(trade?.exit_price), 2),
    netPnl: formatSignedNumber(Number(trade?.net_pnl), 2),
    tradeId: String(trade?.trade_id || '').trim() || '—',
  }))
}

function normalizeTimestamp(value) {
  if (value === undefined || value === null || value === '') return null
  if (typeof value === 'number' && Number.isFinite(value)) {
    const epochMs = value > 1e12 ? value : value * 1000
    const date = new Date(epochMs)
    return Number.isNaN(date.getTime()) ? null : date.toISOString()
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    const text = String(value || '').trim()
    return text || null
  }
  return date.toISOString()
}

function isClosedTradeState(value) {
  const normalized = String(value || '').trim().toLowerCase()
  return ['closed', 'completed', 'complete', 'exited'].includes(normalized)
}

function buildSignalLedgerEntry(entry, index = 0) {
  if (!entry || typeof entry !== 'object') return null
  const timestamp = normalizeTimestamp(entry.event_ts || entry.bar_time || entry.bar_epoch)
  return {
    event_id: String(entry.event_id || entry.signal_id || `signal-${index}`),
    parent_event_id: entry.parent_event_id || entry.parent_id || null,
    root_id: entry.root_id || null,
    created_at: timestamp,
    event_ts: timestamp,
    event_type: 'signal',
    event_subtype: String(entry.signal_type || 'strategy_signal').trim().toLowerCase() || 'strategy_signal',
    reason_code: entry.signal_type || null,
    reason_detail: entry.message || null,
    signal_id: entry.signal_id || entry.event_id || null,
    decision_id: entry.decision_id || null,
    instrument_id: entry.instrument_id || null,
    symbol: entry.symbol || null,
    timeframe: entry.timeframe || null,
    side: entry.direction || null,
    price: Number.isFinite(Number(entry.signal_price)) ? Number(entry.signal_price) : null,
    rule_id: entry.rule_id || null,
    intent: entry.intent || null,
    event_key: entry.event_key || null,
    payload: { ...entry },
  }
}

function buildDecisionLedgerEntry(entry, index = 0) {
  if (!entry || typeof entry !== 'object') return null
  const timestamp = normalizeTimestamp(entry.event_ts || entry.bar_time || entry.bar_epoch)
  const state = String(entry.decision_state || '').trim().toLowerCase()
  return {
    event_id: String(entry.event_id || entry.decision_id || `decision-${index}`),
    parent_event_id: entry.parent_event_id || entry.parent_id || null,
    root_id: entry.root_id || null,
    created_at: timestamp,
    event_ts: timestamp,
    event_type: 'decision',
    event_subtype: state === 'rejected' ? 'signal_rejected' : 'signal_accepted',
    reason_code: entry.reason_code || null,
    reason_detail: entry.message || null,
    signal_id: entry.signal_id || entry.decision_id || null,
    decision_id: entry.decision_id || entry.event_id || null,
    instrument_id: entry.instrument_id || null,
    symbol: entry.symbol || null,
    timeframe: entry.timeframe || null,
    side: entry.direction || null,
    price: Number.isFinite(Number(entry.signal_price)) ? Number(entry.signal_price) : null,
    rule_id: entry.rule_id || null,
    intent: entry.intent || null,
    event_key: entry.event_key || null,
    payload: { ...entry },
  }
}

function buildTradeLedgerEntry(entry, index = 0) {
  if (!entry || typeof entry !== 'object') return null
  const closed = isClosedTradeState(entry.trade_state || entry.status)
  const timestamp = normalizeTimestamp(entry.event_ts || entry.updated_at || entry.closed_at || entry.opened_at)
  const price = closed ? entry.exit_price : entry.entry_price
  return {
    event_id: String(entry.event_id || entry.trade_id || `trade-${index}`),
    parent_event_id: entry.parent_event_id || entry.parent_id || null,
    root_id: entry.root_id || null,
    created_at: timestamp,
    event_ts: timestamp,
    event_type: 'execution',
    event_subtype: closed ? 'close' : 'entry',
    reason_code: entry.trade_state || entry.status || null,
    reason_detail: entry.message || null,
    trade_id: entry.trade_id || null,
    instrument_id: entry.instrument_id || null,
    symbol: entry.symbol || null,
    timeframe: entry.timeframe || null,
    side: entry.direction || entry.side || null,
    qty: Number.isFinite(Number(entry.qty)) ? Number(entry.qty) : null,
    price: Number.isFinite(Number(price)) ? Number(price) : null,
    event_impact_pnl: Number.isFinite(Number(entry.event_impact_pnl)) ? Number(entry.event_impact_pnl) : null,
    trade_net_pnl: Number.isFinite(Number(entry.trade_net_pnl ?? entry.net_pnl)) ? Number(entry.trade_net_pnl ?? entry.net_pnl) : null,
    payload: { ...entry },
  }
}

export function buildBotLensDecisionLedgerEntries({
  signals = [],
  decisions = [],
  trades = [],
} = {}) {
  return [
    ...(Array.isArray(signals) ? signals : []).map((entry, index) => buildSignalLedgerEntry(entry, index)),
    ...(Array.isArray(decisions) ? decisions : []).map((entry, index) => buildDecisionLedgerEntry(entry, index)),
    ...(Array.isArray(trades) ? trades : []).map((entry, index) => buildTradeLedgerEntry(entry, index)),
  ]
    .filter(Boolean)
    .sort((left, right) => {
      const leftTs = Date.parse(left.created_at || left.event_ts || '') || 0
      const rightTs = Date.parse(right.created_at || right.event_ts || '') || 0
      if (leftTs !== rightTs) return leftTs - rightTs
      return String(left.event_id || '').localeCompare(String(right.event_id || ''))
    })
}

function buildDecisionSummaryRows({ signals = [], decisions = [], trades = [], ledgerEntries = [] }) {
  const accepted = (Array.isArray(decisions) ? decisions : []).filter(
    (entry) => String(entry?.decision_state || '').trim().toLowerCase() === 'accepted',
  ).length
  const rejected = (Array.isArray(decisions) ? decisions : []).filter(
    (entry) => String(entry?.decision_state || '').trim().toLowerCase() === 'rejected',
  ).length
  return [
    { key: 'ledger-events', label: 'Ledger Events', value: String(ledgerEntries.length) },
    { key: 'signals', label: 'Signals Emitted', value: String((Array.isArray(signals) ? signals : []).length) },
    { key: 'accepted', label: 'Accepted Decisions', value: String(accepted) },
    { key: 'rejected', label: 'Rejected Decisions', value: String(rejected) },
    { key: 'trades', label: 'Trade Executions', value: String((Array.isArray(trades) ? trades : []).length) },
  ]
}

function buildDecisionLatestRows({ signals = [], decisions = [], trades = [], runtime = {} }) {
  const lastSignal = (Array.isArray(signals) ? signals : []).at(-1) || null
  const lastDecision = (Array.isArray(decisions) ? decisions : []).at(-1) || null
  const lastTrade = (Array.isArray(trades) ? trades : []).at(-1) || null
  return [
    {
      key: 'last-signal',
      label: 'Last Signal',
      value: lastSignal
        ? [
            humanizeToken(lastSignal.signal_type || 'signal'),
            String(lastSignal.direction || '').trim().toUpperCase() || null,
            formatMoment(lastSignal.event_ts || lastSignal.bar_time || lastSignal.bar_epoch),
          ].filter(Boolean).join(' · ')
        : '—',
    },
    {
      key: 'last-decision',
      label: 'Last Decision',
      value: lastDecision
        ? [
            humanizeToken(lastDecision.decision_state || 'decision'),
            lastDecision.reason_code || null,
            formatMoment(lastDecision.event_ts || lastDecision.bar_time || lastDecision.bar_epoch),
          ].filter(Boolean).join(' · ')
        : '—',
    },
    {
      key: 'last-trade',
      label: 'Last Trade',
      value: lastTrade
        ? [
            humanizeToken(lastTrade.trade_state || lastTrade.status || 'trade'),
            String(lastTrade.direction || lastTrade.side || '').trim().toUpperCase() || null,
            formatMoment(lastTrade.event_ts || lastTrade.updated_at || lastTrade.closed_at || lastTrade.opened_at),
          ].filter(Boolean).join(' · ')
        : '—',
    },
    {
      key: 'runtime-event',
      label: 'Runtime Update',
      value: formatMoment(runtime?.last_event_at),
    },
  ]
}

export function buildBotLensWalletRows({
  openTradeCount = 0,
  recentTrades = [],
  runtime = {},
  stats = {},
} = {}) {
  const normalizedStats = stats && typeof stats === 'object' ? stats : {}
  const runtimeState = String(runtime?.runtime_state || runtime?.status || '').trim()
  const totalFees = normalizedStats.fees_paid ?? normalizedStats.total_fees
  const rows = [
    {
      key: 'quote-currency',
      label: 'Quote Currency',
      value: String(normalizedStats.quote_currency || '').trim().toUpperCase() || '—',
    },
    {
      key: 'net-pnl',
      label: 'Net P&L',
      value: formatSignedNumber(Number(normalizedStats.net_pnl), 2),
    },
    {
      key: 'gross-pnl',
      label: 'Gross P&L',
      value: formatSignedNumber(Number(normalizedStats.gross_pnl), 2),
    },
    {
      key: 'fees-paid',
      label: 'Fees Paid',
      value: formatNumber(Number(totalFees), 2),
    },
    {
      key: 'closed-trades',
      label: 'Closed Trades',
      value: Number.isFinite(Number(normalizedStats.completed_trades))
        ? String(Math.max(Number(normalizedStats.completed_trades), 0))
        : '—',
    },
    {
      key: 'open-trades',
      label: 'Open Trades',
      value: String(Math.max(Number(openTradeCount || 0), 0)),
    },
    {
      key: 'win-rate',
      label: 'Win Rate',
      value: formatPercent(Number(normalizedStats.win_rate)),
    },
    {
      key: 'trade-events',
      label: 'Trade Events',
      value: String((Array.isArray(recentTrades) ? recentTrades : []).length),
    },
    {
      key: 'runtime-state',
      label: 'Runtime State',
      value: runtimeState ? humanizeToken(runtimeState) : '—',
    },
    {
      key: 'last-event',
      label: 'Last Runtime Event',
      value: formatMoment(runtime?.last_event_at),
    },
  ]
  return rows.filter((row) => row.value !== '—' || ['open-trades', 'trade-events'].includes(row.key))
}

export function buildBotLensRuntimeViewModel({
  activeRunId,
  bot,
  chartCandles,
  chartHistory,
  chartHistoryCacheCount,
  chartHistoryStatus,
  chartOverlays,
  chartTrades,
  error,
  logs,
  openTrades,
  runState,
  runtimeStatus,
  selectedLabel,
  selectedSymbolBootstrapStatus,
  selectedSymbolDecisions,
  selectedSymbolKey,
  selectedSymbolMetadata,
  selectedSymbolSignals,
  selectedSymbolState,
  selectedSummary,
  statusMessage,
  streamState,
  symbolOptions,
  warningItems,
}) {
  const botLifecycle = describeBotLifecycle(bot)
  const botStatus = normalizeBotStatus(getBotStatus(bot))
  const resolvedRunId = activeRunId || getBotRunId(bot) || '—'
  const runSummaryText = summarizeRun(runState?.runMeta, runState?.health)
  const notices = buildNotices({ statusMessage, error })
  const warningCount = Math.max(
    Array.isArray(warningItems) ? warningItems.length : 0,
    Number(runState?.health?.warning_count || 0) || 0,
  )
  const selectedSymbol = String(selectedSymbolMetadata?.symbol || selectedSummary?.symbol || '').trim().toUpperCase() || '—'
  const selectedTimeframe = String(selectedSymbolMetadata?.timeframe || selectedSummary?.timeframe || '').trim().toUpperCase() || '—'
  const selectedNetPnlValue = Number(selectedSymbolState?.stats?.net_pnl ?? selectedSummary?.stats?.net_pnl)
  const openTradeCount = Object.keys(runState?.openTradesIndex || {}).length
  const logCount = Array.isArray(logs) ? logs.length : 0
  const recentTradeRows = buildRecentTradeRows(chartTrades)
  const topTone = topBarTone(runState?.health?.status || botStatus)
  const strategyName = String(runState?.runMeta?.strategy_name || bot?.strategy_variant_name || bot?.strategy_id || 'Strategy').trim()
  const selectedStats = selectedSymbolState?.stats && typeof selectedSymbolState.stats === 'object'
    ? selectedSymbolState.stats
    : selectedSummary?.stats && typeof selectedSummary.stats === 'object'
      ? selectedSummary.stats
      : {}
  const runtimeSnapshot = selectedSymbolState?.runtime && typeof selectedSymbolState.runtime === 'object'
    ? selectedSymbolState.runtime
    : runState?.health && typeof runState.health === 'object'
      ? runState.health
      : {}
  const selectedOpenTradeCount = Number(selectedSummary?.open_trade_count || 0)
  const decisionLedgerEntries = buildBotLensDecisionLedgerEntries({
    signals: selectedSymbolSignals,
    decisions: selectedSymbolDecisions,
    trades: chartTrades,
  })
  const decisionCount = decisionLedgerEntries.length
  const decisionSummaryRows = buildDecisionSummaryRows({
    signals: selectedSymbolSignals,
    decisions: selectedSymbolDecisions,
    trades: chartTrades,
    ledgerEntries: decisionLedgerEntries,
  })
  const decisionLatestRows = buildDecisionLatestRows({
    signals: selectedSymbolSignals,
    decisions: selectedSymbolDecisions,
    trades: chartTrades,
    runtime: runtimeSnapshot,
  })
  const walletRows = buildBotLensWalletRows({
    openTradeCount: selectedOpenTradeCount,
    recentTrades: chartTrades,
    runtime: runtimeSnapshot,
    stats: selectedStats,
  })
  const runReadiness = runState?.readiness && typeof runState.readiness === 'object'
    ? runState.readiness
    : {}
  const selectedReadiness = selectedSymbolState?.readiness && typeof selectedSymbolState.readiness === 'object'
    ? selectedSymbolState.readiness
    : selectedSymbolMetadata?.readiness && typeof selectedSymbolMetadata.readiness === 'object'
      ? selectedSymbolMetadata.readiness
      : selectedSummary?.readiness && typeof selectedSummary.readiness === 'object'
        ? selectedSummary.readiness
        : {
            catalog_discovered: Boolean(selectedSymbolKey),
            snapshot_ready: false,
            symbol_live: false,
            run_live: Boolean(runReadiness.run_live),
          }
  const transportEligible = Boolean(runState?.transportEligible)
  const selectedSnapshotReady = Boolean(selectedReadiness.snapshot_ready)

  const header = {
    kicker: 'BotLens Runtime',
    title: bot?.name || 'Runtime workspace',
    description: statusMessage || runSummaryText || botLifecycle.detail,
    meta: `bot_id=${bot?.id || '—'} · run_id=${resolvedRunId} · selected=${selectedLabel || '—'}`,
    pills: [
      { key: 'stream', label: 'Live Stream', value: streamState || 'idle' },
      { key: 'bootstrap', label: 'Bootstrap', value: selectedSymbolBootstrapStatus || 'idle' },
      { key: 'selected', label: 'Selected Symbol', value: selectedLabel || '—' },
      { key: 'warnings', label: 'Warnings', value: String(warningCount) },
    ],
  }

  let mode = 'ready'
  if (!bot) {
    mode = 'empty'
  } else if (runtimeStatus === 'bootstrapping') {
    mode = 'loading'
  } else if (!runState) {
    mode = runtimeStatus === 'error' || error ? 'error' : 'idle'
  }

  const symbolPriceContext = new Map()
  Object.entries(runState?.symbolIndex || {}).forEach(([symbolKey, summary]) => {
    symbolPriceContext.set(symbolKey, {
      currentPrice: Number(summary?.last_price),
      latestBarTime: Number.isFinite(Number(summary?.last_bar_time))
        ? new Date(Number(summary.last_bar_time) * 1000).toISOString()
        : null,
    })
  })

  const symbolSelector = {
    selectedKey: selectedSymbolKey,
    selectedLabel,
    bootstrapStatus: selectedSymbolBootstrapStatus || 'idle',
    items: (Array.isArray(symbolOptions) ? symbolOptions : []).map((summary) => ({
      key: summary.symbol_key,
      label: summary.display_label || `${summary.symbol || summary.symbol_key} · ${summary.timeframe || '—'}`,
      symbol: summary.symbol || '—',
      timeframe: summary.timeframe || '—',
      status: summary.status || 'waiting',
      lastEventAt: summary.last_event_at || null,
      trades: Number(summary?.stats?.total_trades || 0),
      netPnl: Number(summary?.stats?.net_pnl || 0),
      openTrades: Number(summary?.open_trade_count || 0),
      isSelected: summary.symbol_key === selectedSymbolKey,
      isLoading: summary.symbol_key === selectedSymbolKey && selectedSymbolBootstrapStatus === 'loading',
      isReady: Boolean(summary?.readiness?.snapshot_ready),
    })),
  }

  const currentStatePanels = {
    overview: {
      runRows: [
        { key: 'run-status', label: 'Run Status', value: runState?.health?.status || botStatus || '—' },
        { key: 'phase', label: 'Phase', value: runState?.lifecycle?.phase || '—' },
        { key: 'tracked-symbols', label: 'Tracked Symbols', value: String(symbolSelector.items.length) },
        { key: 'open-trades', label: 'Open Trades', value: String(openTradeCount) },
        { key: 'run-live', label: 'Run Live', value: formatBooleanState(runReadiness.run_live) },
        { key: 'started', label: 'Started', value: formatMoment(runState?.runMeta?.started_at) },
        { key: 'last-event', label: 'Last Event', value: formatMoment(runState?.health?.last_event_at) },
      ],
      selectedRows: [
        { key: 'selected-symbol', label: 'Selected Symbol', value: selectedLabel || '—' },
        { key: 'bootstrap-status', label: 'Bootstrap Status', value: selectedSymbolBootstrapStatus || 'idle' },
        { key: 'catalog-discovered', label: 'Catalog Discovered', value: formatBooleanState(selectedReadiness.catalog_discovered) },
        { key: 'snapshot-ready', label: 'Snapshot Ready', value: formatBooleanState(selectedReadiness.snapshot_ready) },
        { key: 'symbol-live', label: 'Symbol Live', value: formatBooleanState(selectedReadiness.symbol_live) },
        { key: 'runtime-status', label: 'Runtime Status', value: selectedSymbolState?.status || selectedSummary?.status || '—' },
        { key: 'last-event', label: 'Last Symbol Event', value: formatMoment(selectedSymbolState?.last_event_at || selectedSummary?.last_event_at) },
        { key: 'base-candles', label: 'Base Candles', value: String(selectedSymbolState?.candles?.length || 0) },
        { key: 'signals', label: 'Signals', value: String(selectedSymbolSignals?.length || 0) },
        { key: 'decisions', label: 'Decisions', value: String(selectedSymbolDecisions?.length || 0) },
        { key: 'net-pnl', label: 'Net P&L', value: formatSignedNumber(selectedNetPnlValue) },
      ],
    },
    warnings: {
      count: warningCount,
      items: (Array.isArray(warningItems) ? warningItems : []).map((warning) => ({
        ...warning,
        title: warningRowTitle(warning),
        seenLabel: formatRelativeTime(warning.last_seen_at || warning.first_seen_at),
      })),
    },
    tradeActivity: {
      openTrades: (Array.isArray(openTrades) ? openTrades : [])
        .filter((trade) => isOpenTrade(trade))
        .map((trade, index) => {
          const tradeId = String(trade?.trade_id || `${trade?.entry_time || ''}|${trade?.symbol || ''}|${index}`)
          const chip = buildTradeChip(trade)
          const context = symbolPriceContext.get(normalizeSeriesKey(trade?.symbol_key || '')) || null
          return chip
            ? {
                id: tradeId,
                chip,
                trade,
                currentPrice: context?.currentPrice,
                latestBarTime: context?.latestBarTime,
                isActiveSymbol: normalizeSeriesKey(trade?.symbol_key || '') === selectedSymbolKey,
              }
            : null
        })
        .filter(Boolean),
      logs: Array.isArray(logs) ? logs : [],
    },
  }

  const retrievalPanels = {
    chart: {
      status: selectedSnapshotReady
        ? 'ready'
        : selectedSymbolBootstrapStatus === 'loading'
          ? 'loading'
          : selectedSymbolBootstrapStatus === 'unavailable'
            ? 'unavailable'
          : selectedSymbolKey
            ? 'empty'
            : 'idle',
      selectedLabel,
      selectedSymbol: {
        label: selectedLabel || '—',
        symbol: selectedSymbol,
        timeframe: selectedTimeframe,
        status: selectedSymbolState?.status || selectedSummary?.status || '—',
        bootstrapStatus: selectedSymbolBootstrapStatus || 'idle',
        lastEventAt: formatMoment(selectedSymbolState?.last_event_at || selectedSummary?.last_event_at),
        signals: String(selectedSymbolSignals?.length || 0),
        decisions: String(selectedSymbolDecisions?.length || 0),
        trades: String((Array.isArray(chartTrades) ? chartTrades : []).length),
        netPnl: formatSignedNumber(selectedNetPnlValue),
      },
      historyStatus: chartHistoryStatus || 'idle',
      historyCount: Number(chartHistory?.candles?.length || 0),
      cacheCount: Number(chartHistoryCacheCount || 0),
      candles: Array.isArray(chartCandles) ? chartCandles : [],
      trades: Array.isArray(chartTrades) ? chartTrades : [],
      overlays: Array.isArray(chartOverlays) ? chartOverlays : [],
      timeframe: selectedSymbolMetadata?.timeframe || selectedSymbolState?.timeframe || null,
      mode: bot?.mode || null,
      playbackSpeed: Number(bot?.playback_speed || 0),
      emptyMessage: selectedSymbolBootstrapStatus === 'loading'
        ? `Loading symbol snapshot for ${selectedLabel}...`
        : selectedSymbolBootstrapStatus === 'unavailable'
          ? (statusMessage || `Selected-symbol snapshot is unavailable for ${selectedLabel}.`)
        : selectedSymbolKey
          ? 'Selected-symbol snapshot is required before chart deltas render.'
          : 'Select a symbol to load its runtime chart.',
    },
  }

  return {
    botId: bot?.id || null,
    mode,
    header,
    notices,
    symbolSelector,
    currentStatePanels,
    retrievalPanels,
    topBar: {
      kicker: 'BotLens',
      title: bot?.name || 'Runtime workspace',
      subtitle: [strategyName, bot?.run_type ? humanizeToken(bot.run_type) : null, `run ${shortId(resolvedRunId)}`]
        .filter(Boolean)
        .join(' · '),
      status: {
        label: humanizeToken(runState?.health?.status || botStatus || 'idle'),
        tone: topTone,
      },
      identifiers: [
        { key: 'bot_id', label: 'bot_id', value: bot?.id || null, displayValue: shortId(bot?.id, 12) },
        { key: 'run_id', label: 'run_id', value: resolvedRunId !== '—' ? resolvedRunId : null, displayValue: shortId(resolvedRunId, 12) },
      ],
      stats: [
        { key: 'selected-symbol', label: 'Selected Symbol', value: selectedLabel || '—' },
        { key: 'phase', label: 'Phase', value: formatLifecyclePhaseLabel(runState?.lifecycle?.phase || botLifecycle.phase || 'idle') },
        { key: 'open-trades', label: 'Open Trades', value: String(openTradeCount) },
        { key: 'warnings', label: 'Warnings', value: String(warningCount) },
        { key: 'last-event', label: 'Last Event', value: formatRelativeTime(runState?.health?.last_event_at) },
      ],
    },
    tabs: [
      { key: 'state', label: 'State' },
      { key: 'trades', label: 'Trades', badge: String(openTradeCount) },
      { key: 'decisions', label: 'Decisions', badge: String(decisionCount) },
      { key: 'logs', label: 'Logs', badge: String(logCount) },
      { key: 'diagnostics', label: 'Diagnostics', badge: String(warningCount) },
    ],
    inspection: {
      state: {
        runRows: currentStatePanels.overview.runRows,
        selectedRows: currentStatePanels.overview.selectedRows,
      },
      trades: {
        openTrades: currentStatePanels.tradeActivity.openTrades,
        recentTrades: recentTradeRows,
      },
      decisions: {
        entries: decisionLedgerEntries,
        status: selectedSymbolBootstrapStatus === 'loading' ? 'loading' : 'ready',
        nextCursor: { afterSeq: Number(selectedSymbolState?.seq || 0), afterRowId: 0 },
        summaryRows: decisionSummaryRows,
        walletRows,
        latestRows: decisionLatestRows,
      },
      logs: {
        entries: currentStatePanels.tradeActivity.logs,
      },
      diagnostics: {
        warnings: currentStatePanels.warnings,
        checks: [
          { key: 'runtime', label: 'Runtime', value: humanizeToken(runtimeStatus || 'idle') },
          { key: 'stream', label: 'Live Stream', value: humanizeToken(streamState || 'idle') },
          { key: 'catalog', label: 'Catalog Discovered', value: formatBooleanState(selectedReadiness.catalog_discovered) },
          { key: 'snapshot', label: 'Snapshot Ready', value: formatBooleanState(selectedReadiness.snapshot_ready) },
          { key: 'symbol-live', label: 'Symbol Live', value: formatBooleanState(selectedReadiness.symbol_live) },
          { key: 'run-live', label: 'Run Live', value: formatBooleanState(runReadiness.run_live) },
          { key: 'transport', label: 'Transport Eligible', value: formatBooleanState(transportEligible) },
          { key: 'decisions', label: 'Ledger Events', value: String(decisionCount) },
          { key: 'history', label: 'Chart History', value: humanizeToken(retrievalPanels.chart.historyStatus) },
          { key: 'cache', label: 'Chart Cache', value: String(retrievalPanels.chart.cacheCount) },
        ],
        notices,
      },
    },
    botLifecycle: {
      label: botLifecycle.label,
      detail: botLifecycle.detail,
    },
  }
}

export {
  formatMoment,
  formatNumber,
  formatPercent,
}
