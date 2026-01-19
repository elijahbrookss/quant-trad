import { fetchCandleData } from '../adapters/candle.adapter.js'
import { resolveInstrument } from '../adapters/instrument.adapter.js'

export async function resolveInstrumentId({
  symbol,
  datasource,
  exchange,
  providerId,
  venueId,
}) {
  if (!symbol || !datasource) {
    throw new Error('symbol and datasource are required to resolve instrument.')
  }
  const resolved = await resolveInstrument({
    symbol,
    datasource,
    exchange: exchange ?? undefined,
    provider_id: providerId ?? undefined,
    venue_id: venueId ?? undefined,
  })
  return resolved?.id || null
}

export async function fetchInstrumentCandles({
  instrumentId,
  symbol,
  datasource,
  exchange,
  providerId,
  venueId,
  timeframe,
  start,
  end,
  resolveIfMissing = false,
}) {
  let resolvedInstrumentId = instrumentId
  if (!resolvedInstrumentId && resolveIfMissing) {
    resolvedInstrumentId = await resolveInstrumentId({
      symbol,
      datasource,
      exchange,
      providerId,
      venueId,
    })
  }
  if (!resolvedInstrumentId) {
    throw new Error('instrument_id is required to fetch candles.')
  }

  const candles = await fetchCandleData({
    instrument_id: resolvedInstrumentId,
    symbol,
    timeframe,
    start,
    end,
    datasource,
    exchange,
    provider_id: providerId,
    venue_id: venueId,
  })
  return { candles, instrumentId: resolvedInstrumentId }
}
