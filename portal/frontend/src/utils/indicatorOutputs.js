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

export const getAuthorableOutputsByType = (indicatorOrMeta, outputType) => (
  getIndicatorOutputsByType(indicatorOrMeta, outputType)
)

export const indicatorHasAuthorableOutputs = (
  indicatorOrMeta,
  outputType,
) => getAuthorableOutputsByType(indicatorOrMeta, outputType).length > 0
