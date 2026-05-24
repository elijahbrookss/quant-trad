/**
 * Report service - handles report list and detail API calls.
 */

import {
  compareReports,
  compareRunReports,
  explainMetric,
  exportReport,
  getCandleCatalog,
  getContextDataset,
  getDecisionDataset,
  getExportManifest,
  getReport,
  getReportDiagnostics,
  getReportMetrics,
  getReportReadiness,
  getReportSections,
  getReportSummary,
  getRunReport,
  getOperationalHealth,
  getSignalDataset,
  getTimeseriesDataset,
  getTradeDataset,
  listReports,
} from '../adapters/report.adapter.js'

export const reportService = {
  async listReports(params = {}) {
    return listReports(params)
  },

  async getReport(runId, options) {
    return getReport(runId, options)
  },

  async getRunReport(runId, options) {
    return getRunReport(runId, options)
  },

  async getReportReadiness(runId, options) {
    return getReportReadiness(runId, options)
  },

  async getReportSummary(runId, options) {
    return getReportSummary(runId, options)
  },

  async getReportSections(runId, options) {
    return getReportSections(runId, options)
  },

  async getTradeDataset(runId, params, options) {
    return getTradeDataset(runId, params, options)
  },

  async getDecisionDataset(runId, params, options) {
    return getDecisionDataset(runId, params, options)
  },

  async getSignalDataset(runId, params, options) {
    return getSignalDataset(runId, params, options)
  },

  async getTimeseriesDataset(runId, section, params, options) {
    return getTimeseriesDataset(runId, section, params, options)
  },

  async getContextDataset(runId, params, options) {
    return getContextDataset(runId, params, options)
  },

  async getCandleCatalog(runId, options) {
    return getCandleCatalog(runId, options)
  },

  async getReportDiagnostics(runId, options) {
    return getReportDiagnostics(runId, options)
  },

  async getReportMetrics(runId, options) {
    return getReportMetrics(runId, options)
  },

  async getOperationalHealth(runId, options) {
    return getOperationalHealth(runId, options)
  },

  async getExportManifest(runId, options) {
    return getExportManifest(runId, options)
  },

  async explainMetric(runId, metricName, options) {
    return explainMetric(runId, metricName, options)
  },

  async compareReports(runIds) {
    return compareReports(runIds)
  },

  async compareRunReports(leftRunId, rightRunId, options) {
    return compareRunReports(leftRunId, rightRunId, options)
  },

  async exportReport(runId, options) {
    return exportReport(runId, options)
  },
}
