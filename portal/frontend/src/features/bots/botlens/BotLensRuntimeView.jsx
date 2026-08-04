import { Dialog, DialogPanel } from '@headlessui/react'
import { BotLensContent } from './BotLensContent.jsx'

/**
 * Shared modal lens. Browser navigation selects the run, but the lens remains
 * an inspection overlay so closing it returns to the operator inventory.
 */
export function BotLensRuntimeView({
  model,
  changeSelectedSymbol,
  contextHeader = null,
  loadOlderHistory,
  loadMoreDecisionEvidence,
  loadDecisionEvidencePage,
  loadNewerHistory,
  loadTradeEvidencePage,
  loadDiagnosticEvidencePage,
  focusDecision,
  focusTrade,
  onClose,
  open,
  refreshSession,
}) {
  return (
    <Dialog open={open} onClose={onClose} className="relative z-[75]">
      <div className="qt2-lens-backdrop fixed inset-0 bg-black/80 backdrop-blur-md" aria-hidden="true" />
      <div className="fixed inset-0 overflow-y-auto px-3 py-3 sm:px-4 sm:py-4">
        <DialogPanel className="qt2-lens-dialog qt-ops-shell qt-botlens-shell mx-auto flex min-h-[calc(100vh-1.5rem)] w-full max-w-[min(96vw,118rem)] flex-col overflow-hidden">
          {contextHeader}
          <BotLensContent
            model={model}
            changeSelectedSymbol={changeSelectedSymbol}
            loadOlderHistory={loadOlderHistory}
            loadMoreDecisionEvidence={loadMoreDecisionEvidence}
            loadDecisionEvidencePage={loadDecisionEvidencePage}
            loadNewerHistory={loadNewerHistory}
            loadTradeEvidencePage={loadTradeEvidencePage}
            loadDiagnosticEvidencePage={loadDiagnosticEvidencePage}
            focusDecision={focusDecision}
            focusTrade={focusTrade}
            onClose={onClose}
            open={open}
            refreshSession={refreshSession}
          />
        </DialogPanel>
      </div>
    </Dialog>
  )
}
