export function computeInstrumentRow({ symbol, instrumentMap, instrumentMessages = [], refreshStatus = {} }) {
  const key = (symbol || '').toUpperCase()
  const record = key ? instrumentMap.get(key) : null
  const hasMetadata = record && (record.tick_size != null || record.tick_value != null || record.contract_size != null)
  const isRefreshing = Boolean(refreshStatus?.[key]?.loading)
  const freshness = refreshStatus?.[key]?.updatedAt || record?.updated_at
  const staleLabel = freshness ? `Updated ${new Date(freshness).toLocaleString()}` : 'Needs refresh'
  let status = 'valid'
  if (!hasMetadata) status = 'missing'
  if (instrumentMessages.find((msg) => (msg.symbol || '').toUpperCase() === key)) status = 'error'
  return { key, record, hasMetadata, isRefreshing, staleLabel, status }
}
