import { createLogger } from '../utils/logger.js';

const candleLogger = createLogger('CandleAdapter');

function normalizeBase(url) {
  if (!url) return '';
  return url.endsWith('/') ? url.slice(0, -1) : url;
}

function resolveApiBase() {
  const configured = normalizeBase(import.meta.env.VITE_API_BASE_URL);
  if (configured) return configured;

  if (typeof window !== 'undefined') {
    const { protocol, hostname, port } = window.location;
    if (port && Number(port) === 5173) {
      return `${protocol}//${hostname}:8000`;
    }
    const basePort = port ? `:${port}` : '';
    return `${protocol}//${hostname}${basePort}`;
  }

  return 'http://localhost:8000';
}

const API_BASE_URL = resolveApiBase();

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
    candleLogger.debug('fetch_candles_request', { symbol, timeframe, start, end, datasource, exchange, baseUrl: API_BASE_URL });
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
