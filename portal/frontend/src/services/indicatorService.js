/**
 * Indicator service - handles all indicator-related API calls.
 */

import { api } from './api'

export const indicatorService = {
  /**
   * Get all indicators.
   */
  async getAll() {
    return api.get('/indicators')
  },

  /**
   * Get a single indicator by ID.
   */
  async getById(id) {
    return api.get(`/indicators/${id}`)
  },

  /**
   * Create a new indicator.
   */
  async create(data) {
    return api.post('/indicators', data)
  },

  /**
   * Update an existing indicator.
   */
  async update(id, data) {
    return api.put(`/indicators/${id}`, data)
  },

  /**
   * Delete an indicator.
   */
  async delete(id) {
    return api.delete(`/indicators/${id}`)
  },

  /**
   * Get signal rules for an indicator.
   */
  async getSignalRules(id) {
    return api.get(`/indicators/${id}/signal-rules`)
  },
}
