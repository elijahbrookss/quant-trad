const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

/**
 * Adapter to fetch indicator data from backend API
 * @returns {Promise<Array>} - array of candles
 */
export async function fetchIndicators() {
    console.log("[IndicatorAdapter] Fetching indicators from API...");
  try {
    const res = await fetch(`${API_BASE_URL}/api/indicators`, {
      method: "GET",
      headers: { "Content-Type": "application/json" },
    });

    console.log("[IndicatorAdapter] Response status:", res.status);
    if (!res.ok) {
      throw new Error(`API error: ${res.status} ${res.statusText}`);
    }

    const indicators = await res.json();
    console.log("[IndicatorAdapter] Fetched indicators:", indicators);
    return Array.isArray(indicators) ? indicators : [];

  } catch (err) {
    console.error("[CandleAdapter] fetch failed:", err);
    return [];
  }
}
