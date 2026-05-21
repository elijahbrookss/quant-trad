import { BotLensRuntimeContainer } from '../../botlens/BotLensRuntimeContainer.jsx'

export function BotsRuntimeWorkspace({ lensBot, runtimeState, setLensBotId }) {
  return (
    <section className="space-y-4 xl:sticky xl:top-4">
      {!lensBot ? (
        <div className="rounded-2xl border border-white/10 bg-black/20 px-6 py-10 text-center">
          <p className="text-[10px] font-semibold uppercase tracking-[0.32em] text-[color:var(--accent-text-kicker)]">
            Runtime Workspace
          </p>
          <p className="mt-3 text-lg font-semibold text-slate-100">{runtimeState.title}</p>
          <p className="mt-2 text-sm text-slate-400">{runtimeState.detail}</p>
        </div>
      ) : (
        <BotLensRuntimeContainer bot={lensBot} onClose={() => setLensBotId(null)} />
      )}
    </section>
  )
}
