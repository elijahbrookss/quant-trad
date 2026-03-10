/**
 * Report service - handles report list and detail API calls.
 */

import { compareReports, exportReport, getReport, listReports } from '../adapters/report.adapter.js'

export const reportService = {
  async listReports(params = {}) {
    return listReports(params)
  },

  async getReport(runId) {
    return getReport(runId)
  },

  async compareReports(runIds) {
    return compareReports(runIds)
  },

  async exportReport(runId, options) {
    return exportReport(runId, options)
  },
}
