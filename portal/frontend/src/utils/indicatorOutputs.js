const normalizedOutputName = (value) => String(value || '').trim()

const outputCatalog = (indicatorOrMeta) => {
  if (Array.isArray(indicatorOrMeta?.typed_outputs)) {
    return indicatorOrMeta.typed_outputs
  }
  if (Array.isArray(indicatorOrMeta?.outputs)) {
    return indicatorOrMeta.outputs
  }
  return []
}

export const getIndicatorOutputsByType = (indicatorOrMeta, outputType) => (
  outputCatalog(indicatorOrMeta).filter((entry) => (
    entry?.type === outputType && normalizedOutputName(entry?.name)
  ))
)

export const buildSignalOutputEnabledMap = (indicatorOrMeta) => {
  const enabledByName = {}
  const outputPrefs = indicatorOrMeta?.output_prefs

  getIndicatorOutputsByType(indicatorOrMeta, 'signal').forEach((entry) => {
    const outputName = normalizedOutputName(entry?.name)
    if (!outputName) return
    const prefEnabled = outputPrefs?.[outputName]?.enabled
    enabledByName[outputName] = prefEnabled === false ? false : entry?.enabled !== false
  })

  return enabledByName
}

export const enabledSignalOutputNames = (indicatorOrMeta) => (
  Object.entries(buildSignalOutputEnabledMap(indicatorOrMeta))
    .filter(([, enabled]) => enabled !== false)
    .map(([outputName]) => outputName)
)

export const isSignalOutputEnabled = (indicatorOrMeta, outputName) => {
  const normalized = normalizedOutputName(outputName)
  if (!normalized) return false
  return buildSignalOutputEnabledMap(indicatorOrMeta)[normalized] !== false
}

export const applySignalOutputPrefs = (indicatorOrMeta, outputPrefs = {}) => {
  const next = {
    ...(indicatorOrMeta || {}),
    output_prefs: { ...(outputPrefs || {}) },
  }
  if (!Array.isArray(indicatorOrMeta?.typed_outputs)) {
    return next
  }
  next.typed_outputs = indicatorOrMeta.typed_outputs.map((entry) => {
    if (entry?.type !== 'signal') return entry
    const outputName = normalizedOutputName(entry?.name)
    if (!outputName) return entry
    return {
      ...entry,
      enabled: outputPrefs?.[outputName]?.enabled === false ? false : true,
    }
  })
  return next
}

export const buildSignalOutputPrefs = (indicatorOrMeta, enabledByName = {}) => {
  const outputPrefs = {}

  getIndicatorOutputsByType(indicatorOrMeta, 'signal').forEach((entry) => {
    const outputName = normalizedOutputName(entry?.name)
    if (!outputName) return
    if (enabledByName[outputName] === false) {
      outputPrefs[outputName] = { enabled: false }
    }
  })

  return outputPrefs
}

export const getAuthorableOutputsByType = (
  indicatorOrMeta,
  outputType,
  { selectedOutputName = '' } = {},
) => {
  const selectedName = normalizedOutputName(selectedOutputName)

  return getIndicatorOutputsByType(indicatorOrMeta, outputType).filter((entry) => {
    if (outputType !== 'signal') return true
    const outputName = normalizedOutputName(entry?.name)
    return entry?.enabled !== false || outputName === selectedName
  })
}

export const indicatorHasAuthorableOutputs = (
  indicatorOrMeta,
  outputType,
  options = {},
) => getAuthorableOutputsByType(indicatorOrMeta, outputType, options).length > 0
