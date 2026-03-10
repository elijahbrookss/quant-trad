/**
 * Instrument service - handles all instrument/market data API calls.
 */

import { api } from './api'

export const instrumentService = {
  /**
   * Get all instruments.
   */
  async getAll() {
    return api.get('/instruments')
  },

  /**
   * Get a single instrument by symbol.
   */
  async getBySymbol(symbol, datasource = null, exchange = null) {
    const params = new URLSearchParams()
    if (datasource) params.append('datasource', datasource)
    if (exchange) params.append('exchange', exchange)

    const query = params.toString() ? `?${params.toString()}` : ''
    return api.get(`/instruments/${symbol}${query}`)
  },

  /**
   * Save or update instrument metadata.
   */
  async save(data) {
    return api.post('/instruments', data)
  },

  /**
   * Delete instrument metadata.
   */
  async delete(symbol, datasource = null, exchange = null) {
    const params = new URLSearchParams()
    if (datasource) params.append('datasource', datasource)
    if (exchange) params.append('exchange', exchange)

    const query = params.toString() ? `?${params.toString()}` : ''
    return api.delete(`/instruments/${symbol}${query}`)
  },
}
