import { Dialog, DialogPanel } from '@headlessui/react'
import { BotLensContent } from './BotLensContent.jsx'

/**
 * Modal shell used by v1 (BotPanel) and any v2 caller that still wants an
 * overlay. The routed v2 lens page uses BotLensRuntimePageView instead —
 * both share BotLensContent so tab/table/chart logic exists exactly once.
 */
export function BotLensRuntimeView({
  model,
  changeSelectedSymbol,
  loadOlderHistory,
  onClose,
  open,
  refreshSession,
}) {
  return (
    <Dialog open={open} onClose={onClose} className="relative z-[75]">
      <div className="fixed inset-0 bg-black/80 backdrop-blur-sm" aria-hidden="true" />
      <div className="fixed inset-0 overflow-y-auto px-3 py-3 sm:px-4 sm:py-4">
        <DialogPanel className="qt-ops-shell qt-botlens-shell mx-auto flex min-h-[calc(100vh-1.5rem)] w-full max-w-[min(96vw,118rem)] flex-col overflow-hidden">
          <BotLensContent
            model={model}
            changeSelectedSymbol={changeSelectedSymbol}
            loadOlderHistory={loadOlderHistory}
            onClose={onClose}
            open={open}
            refreshSession={refreshSession}
          />
        </DialogPanel>
      </div>
    </Dialog>
  )
}
