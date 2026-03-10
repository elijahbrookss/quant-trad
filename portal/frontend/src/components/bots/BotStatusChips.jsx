export function BotStatusChips({ statusDisplay, progressDisplay, streamStatus }) {
  return (
    <div className="rounded-2xl border border-white/5 bg-black/20 px-3 py-2">
      <div className="flex flex-wrap items-center gap-3 text-[10px] uppercase tracking-[0.25em] text-slate-400">
        <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-2 py-1 text-xs text-white">
          <span className="text-[10px] uppercase tracking-[0.25em] text-slate-400">Status</span>
          <span className="font-semibold tracking-normal text-white">{statusDisplay}</span>
        </div>
        <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-2 py-1 text-xs text-white">
          <span className="text-[10px] uppercase tracking-[0.25em] text-slate-400">Progress</span>
          <span className="font-semibold tracking-normal text-white">{progressDisplay}</span>
        </div>
        <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-2 py-1 text-xs text-white">
          <span className="text-[10px] uppercase tracking-[0.25em] text-slate-400">Feed</span>
          <span className="font-semibold tracking-normal text-white">{streamStatus}</span>
        </div>
      </div>
    </div>
  )
}
