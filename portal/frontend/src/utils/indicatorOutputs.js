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
