import { useMemo } from 'react'
import { buildBotLensRuntimeViewModel } from './buildBotLensRuntimeViewModel.js'
import { useBotLensController } from './hooks/useBotLensController.js'
import { BotLensRuntimeView } from './BotLensRuntimeView.jsx'
import { BotLensRuntimePageView } from './BotLensRuntimePageView.jsx'

/**
 * `variant`: 'dialog' (default, v1's modal — unchanged) or 'page' (v2's
 * routed lens). Both variants render the same BotLensContent; only the
 * outer chrome differs. See BotLensRuntimeView.jsx / BotLensRuntimePageView.jsx.
 */
export function BotLensRuntimeContainer({ bot, runId = null, open = Boolean(bot), onClose, variant = 'dialog' }) {
  const controller = useBotLensController({ open, bot, onClose, runId })
  const runState = controller.runState

  // buildBotLensRuntimeViewModel only ever reads these 7 sub-fields off
  // `runState` (verified by grep — nothing else, notably never `symbolStates`,
  // which is what changes on every candle tick). `controller.runState` itself
  // is a brand-new wrapper object on every dispatch (state.runState is always
  // rebuilt), so passing it through directly would defeat any memoization
  // below it regardless of how narrow the rest of the deps are. Rebuilding
  // just this narrower shape means it only gets a new reference when one of
  // these 7 fields actually changes — not on every unrelated candle tick.
  const runStateForModel = useMemo(
    () => ({
      health: runState?.health,
      openTradesIndex: runState?.openTradesIndex,
      runMeta: runState?.runMeta,
      readiness: runState?.readiness,
      lifecycle: runState?.lifecycle,
      transportEligible: runState?.transportEligible,
      symbolIndex: runState?.symbolIndex,
    }),
    [
      runState?.health,
      runState?.openTradesIndex,
      runState?.runMeta,
      runState?.readiness,
      runState?.lifecycle,
      runState?.transportEligible,
      runState?.symbolIndex,
    ],
  )

  // Single memo boundary around the whole view-model build. Every dep here
  // is now a properly-scoped, stable-across-irrelevant-ticks value (thanks
  // to useBotLensController's per-slice selectors and runStateForModel
  // above), so this genuinely skips rebuilding decisions/trades/diagnostics
  // on a tick that only touched a different symbol's chart data, and vice
  // versa — not just a memo call that always re-executes.
  const model = useMemo(
    () => buildBotLensRuntimeViewModel({
      activeRunId: controller.activeRunId,
      bot,
      chartCandles: controller.chartCandles,
      chartHistory: controller.chartHistory,
      chartHistoryCacheCount: controller.chartHistoryCacheCount,
      chartHistoryStatus: controller.chartHistoryStatus,
      chartOverlays: controller.chartOverlays,
      chartTrades: controller.chartTrades,
      error: controller.error,
      logs: controller.logs,
      openTrades: controller.openTrades,
      runState: runStateForModel,
      runtimeStatus: controller.runtimeStatus,
      selectedLabel: controller.selectedLabel,
      selectedSymbolBootstrapStatus: controller.selectedSymbolBootstrapStatus,
      selectedSymbolDecisions: controller.selectedSymbolDecisions,
      selectedSymbolKey: controller.selectedSymbolKey,
      selectedSymbolMetadata: controller.selectedSymbolMetadata,
      selectedSymbolSignals: controller.selectedSymbolSignals,
      selectedSymbolState: controller.selectedSymbolState,
      selectedSummary: controller.selectedSummary,
      statusMessage: controller.statusMessage,
      streamState: controller.streamState,
      symbolOptions: controller.symbolOptions,
      warningItems: controller.warningItems,
    }),
    [
      controller.activeRunId,
      bot,
      controller.chartCandles,
      controller.chartHistory,
      controller.chartHistoryCacheCount,
      controller.chartHistoryStatus,
      controller.chartOverlays,
      controller.chartTrades,
      controller.error,
      controller.logs,
      controller.openTrades,
      runStateForModel,
      controller.runtimeStatus,
      controller.selectedLabel,
      controller.selectedSymbolBootstrapStatus,
      controller.selectedSymbolDecisions,
      controller.selectedSymbolKey,
      controller.selectedSymbolMetadata,
      controller.selectedSymbolSignals,
      controller.selectedSymbolState,
      controller.selectedSummary,
      controller.statusMessage,
      controller.streamState,
      controller.symbolOptions,
      controller.warningItems,
    ],
  )

  const View = variant === 'page' ? BotLensRuntimePageView : BotLensRuntimeView
  return (
    <View
      model={model}
      changeSelectedSymbol={controller.changeSelectedSymbol}
      loadOlderHistory={controller.loadOlderHistory}
      onClose={controller.closeModal}
      open={open}
      refreshSession={controller.refreshSession}
    />
  )
}
