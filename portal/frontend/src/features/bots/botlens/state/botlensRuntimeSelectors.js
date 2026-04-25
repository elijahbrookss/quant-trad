import {
  getSelectedSymbolSlices,
  mergeCanonicalCandles,
  normalizeSeriesKey,
} from '../../../../components/bots/botlensProjection.js'
import { getBotLensProjectionStore } from './botlensRuntimeState.js'

export function selectSelectedSymbolBaseSlices(state) {
  const projectionStore = getBotLensProjectionStore(state)
  if (!projectionStore) return null
  return getSelectedSymbolSlices(projectionStore)
}

export function selectSelectedSymbolKey(state) {
  return normalizeSeriesKey(state?.selectedSymbolKey || '') || null
}

export function selectSelectedSymbolState(state) {
  const selectedSymbolKey = selectSelectedSymbolKey(state)
  if (!selectedSymbolKey) return null
  return state?.runState?.symbolStates?.[selectedSymbolKey] || null
}

export function selectSelectedSymbolSummary(state) {
  const selectedSymbolKey = selectSelectedSymbolKey(state)
  if (!selectedSymbolKey) return null
  return state?.runState?.symbolIndex?.[selectedSymbolKey] || null
}

export function selectSelectedSymbolBootstrapStatus(state) {
  const selectedSymbolKey = selectSelectedSymbolKey(state)
  if (!selectedSymbolKey) return 'idle'
  return state?.symbolBootstrapStatusByKey?.[selectedSymbolKey] || 'idle'
}

export function selectSelectedSymbolChartHistory(state) {
  const selectedSymbolKey = selectSelectedSymbolKey(state)
  if (!selectedSymbolKey) return null
  return state?.retrieval?.chartHistoryBySymbol?.[selectedSymbolKey] || null
}

export function selectSelectedSymbolChartHistoryStatus(state) {
  return selectSelectedSymbolChartHistory(state)?.status || 'idle'
}

export function selectSelectedSymbolChartCandles(state) {
  const baseSlices = selectSelectedSymbolBaseSlices(state)
  const history = selectSelectedSymbolChartHistory(state)
  return mergeCanonicalCandles(history?.candles || [], baseSlices?.candles || [])
}

export function selectSymbolOptions(state) {
  return Object.values(state?.runState?.symbolIndex || {}).sort((left, right) => {
    const leftLabel = String(left?.display_label || left?.symbol_key || '')
    const rightLabel = String(right?.display_label || right?.symbol_key || '')
    return leftLabel.localeCompare(rightLabel)
  })
}

export function selectOpenTrades(state) {
  return Object.values(state?.runState?.openTradesIndex || {})
}

export function selectActiveRunId(state) {
  return state?.runState?.runMeta?.run_id || null
}

export function selectWarningItems(state) {
  return Array.isArray(state?.runState?.health?.warnings) ? state.runState.health.warnings : []
}

export function selectSelectedSymbolOverlays(state) {
  return selectSelectedSymbolBaseSlices(state)?.overlays || []
}

export function selectSelectedSymbolRecentTrades(state) {
  return selectSelectedSymbolBaseSlices(state)?.recentTrades || []
}

export function selectSelectedSymbolLogs(state) {
  return selectSelectedSymbolBaseSlices(state)?.logs || []
}

export function selectSelectedSymbolMetadata(state) {
  return selectSelectedSymbolBaseSlices(state)?.metadata || null
}

export function selectSelectedSymbolSignals(state) {
  return Array.isArray(selectSelectedSymbolState(state)?.signals)
    ? selectSelectedSymbolState(state).signals
    : []
}

export function selectSelectedSymbolDecisions(state) {
  return Array.isArray(selectSelectedSymbolState(state)?.decisions)
    ? selectSelectedSymbolState(state).decisions
    : []
}

export function selectChartHistoryCacheCount(state) {
  return Object.keys(state?.retrieval?.chartHistoryBySymbol || {}).length
}
