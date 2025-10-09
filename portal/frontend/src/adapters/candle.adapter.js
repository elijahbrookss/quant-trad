import { createLogger } from '../utils/logger.js';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';
const candleLogger = createLogger('CandleAdapter');

/**
 * Adapter to fetch OHLCV candle data from backend API
 * @param {Object} params
 * @param {string} params.symbol
 * @param {string} params.timeframe
 * @param {string} params.start - ISO string
 * @param {string} params.end - ISO string
 * @param {string} [params.datasource]
 * @param {string} [params.exchange]
 * @returns {Promise<Array>} - array of candles
 */
export async function fetchCandleData({ symbol, timeframe, start, end, datasource, exchange }) {
  try {
    candleLogger.debug('fetch_candles_request', { symbol, timeframe, start, end, datasource, exchange });
    const payload = { symbol, timeframe, start, end };
    if (datasource) payload.datasource = datasource;
    if (exchange) payload.exchange = exchange;
    const res = await fetch(`${API_BASE_URL}/api/candles`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      throw new Error(`API error: ${res.status} ${res.statusText}`);
    }

    const { candles } = await res.json();
    const items = Array.isArray(candles) ? candles : [];
    candleLogger.info('fetch_candles_success', { symbol, timeframe, candles: items.length });
    return items;
  } catch (err) {
    candleLogger.error('fetch_candles_failed', { symbol, timeframe }, err);
    return [];
  }
}
