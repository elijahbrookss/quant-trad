const validateRiskSettings = (riskSettings, { minBaseRisk = 1, minRiskMultiplier = 0.01 } = {}) => {
  const errors = {}
  const baseRiskValue = riskSettings?.baseRiskPerTrade
  const baseRisk = baseRiskValue === '' ? null : Number(baseRiskValue)
  if (baseRisk !== null && (!Number.isFinite(baseRisk) || baseRisk < minBaseRisk)) {
    errors.baseRiskPerTrade = `Base risk must be at least ${minBaseRisk}.`
  }

  const globalRiskValue = riskSettings?.globalRiskMultiplier
  const globalRisk = globalRiskValue === '' ? null : Number(globalRiskValue)
  if (globalRisk !== null && (!Number.isFinite(globalRisk) || globalRisk < minRiskMultiplier)) {
    errors.globalRiskMultiplier = `Global risk must be at least ${minRiskMultiplier}.`
  }

  return errors
}

const calculateRiskMetrics = (riskSettings) => {
  const baseRisk = riskSettings?.baseRiskPerTrade === '' ? null : Number(riskSettings?.baseRiskPerTrade)
  const globalRisk = riskSettings?.globalRiskMultiplier === '' ? null : Number(riskSettings?.globalRiskMultiplier)
  return {
    baseRiskPerTrade: Number.isFinite(baseRisk) ? baseRisk : null,
    globalRiskMultiplier: Number.isFinite(globalRisk) ? globalRisk : null,
  }
}

const buildRiskPayload = (riskSettings) => {
  const metrics = calculateRiskMetrics(riskSettings)
  return {
    base_risk_per_trade: metrics.baseRiskPerTrade,
    global_risk_multiplier: metrics.globalRiskMultiplier,
  }
}

export {
  validateRiskSettings,
  calculateRiskMetrics,
  buildRiskPayload,
}
