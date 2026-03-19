/**
 * Strategy service - handles all strategy-related API calls.
 */

import { api } from './api'

export const strategyService = {
  /**
   * Get all strategies.
   */
  async getAll() {
    return api.get('/strategies')
  },

  /**
   * Get a single strategy by ID.
   */
  async getById(id) {
    return api.get(`/strategies/${id}`)
  },

  /**
   * Create a new strategy.
   */
  async create(data) {
    return api.post('/strategies', data)
  },

  /**
   * Update an existing strategy.
   */
  async update(id, data) {
    return api.put(`/strategies/${id}`, data)
  },

  /**
   * Delete a strategy.
   */
  async delete(id) {
    return api.delete(`/strategies/${id}`)
  },

  /**
   * Attach an indicator to a strategy.
   */
  async attachIndicator(strategyId, indicatorId) {
    return api.post(`/strategies/${strategyId}/indicators/${indicatorId}`)
  },

  /**
   * Detach an indicator from a strategy.
   */
  async detachIndicator(strategyId, indicatorId) {
    return api.delete(`/strategies/${strategyId}/indicators/${indicatorId}`)
  },

  /**
   * Create a rule for a strategy.
   */
  async createRule(strategyId, ruleData) {
    return api.post(`/strategies/${strategyId}/rules`, ruleData)
  },

  /**
   * Update a strategy rule.
   */
  async updateRule(strategyId, ruleId, ruleData) {
    return api.put(`/strategies/${strategyId}/rules/${ruleId}`, ruleData)
  },

  /**
   * Delete a strategy rule.
   */
  async deleteRule(strategyId, ruleId) {
    return api.delete(`/strategies/${strategyId}/rules/${ruleId}`)
  },

  /**
   * Run a rule-logic preview for a strategy.
   */
  async runPreview(strategyId, params) {
    return api.post(`/strategies/${strategyId}/preview`, params)
  },

  /**
   * Get all ATM templates.
   */
  async getATMTemplates() {
    return api.get('/strategies/atm-templates')
  },

  /**
   * Save an ATM template.
   */
  async saveATMTemplate(templateData) {
    return api.post('/strategies/atm-templates', templateData)
  },

  /**
   * Get symbol presets.
   */
  async getSymbolPresets() {
    return api.get('/strategies/presets/symbols')
  },

  /**
   * Save a symbol preset.
   */
  async saveSymbolPreset(presetData) {
    return api.post('/strategies/presets/symbols', presetData)
  },

  /**
   * Delete a symbol preset.
   */
  async deleteSymbolPreset(presetId) {
    return api.delete(`/strategies/presets/symbols/${presetId}`)
  },
}
