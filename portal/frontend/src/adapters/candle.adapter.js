import { API_BASE_URL } from '../config/appConfig.js';
import { createLogger } from '../utils/logger.js';

const candleLogger = createLogger('CandleAdapter');

/**
 * Adapter to fetch OHLCV candle data from backend API
 * @param {Object} params
 * @param {string} params.instrument_id
 * @param {string} params.timeframe
 * @param {string} params.start - ISO string
 * @param {string} params.end - ISO string
 * @param {string} [params.datasource]
 * @param {string} [params.exchange]
 * @param {string} [params.provider_id]
 * @param {string} [params.venue_id]
 * @returns {Promise<Array>} - array of candles
 */
export async function fetchCandleData({
  instrument_id,
  timeframe,
  start,
  end,
  datasource,
  exchange,
  provider_id,
  venue_id,
}) {
  try {
    if (!instrument_id) {
      throw new Error('instrument_id is required to fetch candles.')
    }
    candleLogger.debug('fetch_candles_request', {
      instrument_id,
      timeframe,
      start,
      end,
      datasource,
      exchange,
      provider_id,
      venue_id,
      baseUrl: API_BASE_URL,
    });
    const payload = { instrument_id, timeframe, start, end };
    if (datasource) payload.datasource = datasource;
    if (exchange) payload.exchange = exchange;
    if (provider_id) payload.provider_id = provider_id;
    if (venue_id) payload.venue_id = venue_id;
    const res = await fetch(`${API_BASE_URL}/candles/`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      throw new Error(`API error: ${res.status} ${res.statusText}`);
    }

    const { candles } = await res.json();
    const items = Array.isArray(candles) ? candles : [];
    candleLogger.info('fetch_candles_success', { instrument_id, timeframe, candles: items.length });
    return items;
  } catch (err) {
    candleLogger.error('fetch_candles_failed', { instrument_id, timeframe }, err);
    return [];
  }
}
