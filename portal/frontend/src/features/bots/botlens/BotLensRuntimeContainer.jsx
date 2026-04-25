import { buildBotLensRuntimeViewModel } from './buildBotLensRuntimeViewModel.js'
import { useBotLensController } from './hooks/useBotLensController.js'
import { BotLensRuntimeView } from './BotLensRuntimeView.jsx'

export function BotLensRuntimeContainer({ bot, open = Boolean(bot), onClose }) {
  const controller = useBotLensController({ open, bot, onClose })
  const model = buildBotLensRuntimeViewModel({
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
    runState: controller.runState,
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
  })

  return (
    <BotLensRuntimeView
      model={model}
      changeSelectedSymbol={controller.changeSelectedSymbol}
      loadOlderHistory={controller.loadOlderHistory}
      onClose={controller.closeModal}
      open={open}
      refreshSession={controller.refreshSession}
    />
  )
}
