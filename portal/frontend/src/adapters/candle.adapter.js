const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

/**
 * Adapter to fetch OHLCV candle data from backend API
 * @param {Object} params
 * @param {string} params.symbol
 * @param {string} params.timeframe
 * @param {string} params.start - ISO string
 * @param {string} params.end - ISO string
 * @returns {Promise<Array>} - array of candles
 */
export async function fetchCandleData({ symbol, timeframe, start, end }) {
  try {
    const res = await fetch(`${API_BASE_URL}/api/candles`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ symbol, timeframe, start, end }),
    });

    if (!res.ok) {
      throw new Error(`API error: ${res.status} ${res.statusText}`);
    }

    const { candles } = await res.json();
    return Array.isArray(candles) ? candles : [];
  } catch (err) {
    console.error("[CandleAdapter] fetch failed:", err);
    return [];
  }
}
