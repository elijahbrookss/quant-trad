import { BotLensContent } from './BotLensContent.jsx'

/**
 * Routed replacement for the BotLens modal (v2 /fleet/bots/:botId,
 * /fleet/collectors/:definitionId lenses). Same BotLensContent as the
 * Dialog-based BotLensRuntimeView, just in normal document flow inside
 * .qt2-main instead of a fixed-position overlay — a real page you navigate
 * to and back from, not a dialog you dismiss.
 */
export function BotLensRuntimePageView({
  model,
  changeSelectedSymbol,
  loadOlderHistory,
  onClose,
  open,
  refreshSession,
}) {
  return (
    <div className="qt2-lens-shell qt-ops-shell qt-botlens-shell flex w-full flex-col overflow-hidden">
      <BotLensContent
        model={model}
        changeSelectedSymbol={changeSelectedSymbol}
        loadOlderHistory={loadOlderHistory}
        onClose={onClose}
        open={open}
        refreshSession={refreshSession}
      />
    </div>
  )
}
