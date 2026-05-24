export const LEGACY_TRADE_OVERLAY_TYPE = 'bot_trade_rays'

export const isLegacyTradeOverlay = (overlay) => {
  const type = typeof overlay?.type === 'string' ? overlay.type.trim().toLowerCase() : ''
  return type === LEGACY_TRADE_OVERLAY_TYPE
}

export const suppressLegacyTradeOverlays = (overlays = []) => (
  Array.isArray(overlays)
    ? overlays.filter((overlay) => !isLegacyTradeOverlay(overlay))
    : []
)
