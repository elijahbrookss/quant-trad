import { PlusCircle, RefreshCw, Search } from 'lucide-react'

import { BotFleetCard } from '../../fleet/components/BotFleetCard.jsx'

function SummaryMetric({ label, value, tone = 'default' }) {
  const valueClass = tone === 'attention'
    ? 'text-amber-100'
    : tone === 'danger'
      ? 'text-rose-100'
      : 'text-slate-100'

  return (
    <div className="min-w-0 border-l border-white/8 pl-3 first:border-l-0 first:pl-0">
      <p className="qt-ops-kicker">{label}</p>
      <p className={`mt-1 text-base font-semibold tracking-[0.01em] ${valueClass}`}>{value}</p>
    </div>
  )
}

function FleetHeader({
  botStreamState,
  filteredBots,
  fleetSummary,
  handleOpenCreate,
  manualRefreshFleet,
  refreshingFleet,
  runtimeCapacity,
  search,
  setSearch,
  sortedBots,
}) {
  return (
    <section className="qt-ops-shell overflow-hidden">
      <div className="border-b border-white/8 px-4 py-4 sm:px-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0">
            <p className="qt-ops-kicker">Bots</p>
            <h2 className="mt-2 text-[1.6rem] font-semibold tracking-[0.01em] text-slate-50">Fleet management</h2>
            <p className="mt-2 max-w-3xl text-sm leading-relaxed text-slate-400">
              Run state, operator actions, and fleet health stay here. BotLens remains the separate deep inspection surface.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={manualRefreshFleet}
              disabled={refreshingFleet}
              className="qt-mono inline-flex items-center gap-1.5 rounded-[3px] border border-white/10 bg-black/25 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-300 transition hover:border-white/16 hover:bg-black/40 hover:text-slate-100 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <RefreshCw className={`size-3.5 ${refreshingFleet ? 'animate-spin' : ''}`} />
              Refresh
            </button>
            <button
              type="button"
              onClick={handleOpenCreate}
              className="qt-mono inline-flex items-center gap-1.5 rounded-[3px] border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-12)] px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-[color:var(--accent-text-strong)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-20)]"
            >
              <PlusCircle className="size-3.5" />
              New Bot
            </button>
          </div>
        </div>
      </div>

      <div className="space-y-4 px-4 py-4 sm:px-5">
        <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
          <div className="grid gap-3 border-b border-white/6 pb-4 sm:grid-cols-3 xl:grid-cols-7">
            {fleetSummary.items.map((item) => (
              <SummaryMetric
                key={item.key}
                label={item.label}
                value={item.value}
                tone={item.key === 'failed' && Number(item.value) > 0 ? 'danger' : 'default'}
              />
            ))}
            {runtimeCapacity ? (
              <SummaryMetric
                label="CPU"
                value={`${Number(runtimeCapacity.workers_in_use || 0)}/${Number(runtimeCapacity.host_cpu_cores || 0)}`}
                tone={Number(runtimeCapacity.over_capacity_workers || 0) > 0 ? 'attention' : 'default'}
              />
            ) : null}
            {runtimeCapacity ? (
              <SummaryMetric
                label="Load"
                value={`${Number(runtimeCapacity.in_use_pct || 0).toFixed(1)}%`}
                tone={Number(runtimeCapacity.in_use_pct || 0) >= 90 ? 'attention' : 'default'}
              />
            ) : null}
          </div>

          <div className="qt-mono flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.14em] text-slate-500 lg:justify-end">
            <span>{filteredBots.length} shown</span>
            <span className="text-slate-700">/</span>
            <span>{sortedBots.length} total</span>
            <span className="text-slate-700">/</span>
            <span>stream {botStreamState}</span>
          </div>
        </div>

        <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-center">
          <label className="qt-ops-panel-muted flex min-w-0 items-center gap-2 px-3 py-2.5 text-slate-200 focus-within:border-white/14">
            <Search className="size-3.5 shrink-0 text-slate-600" />
            <input
              type="search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Filter by bot, strategy, status, or run id"
              className="min-w-0 flex-1 bg-transparent text-sm text-slate-200 placeholder:text-slate-600 focus:outline-none"
            />
          </label>
          <p className="text-sm text-slate-500 lg:text-right">
            Filter the fleet without leaving the operational surface.
          </p>
        </div>
      </div>
    </section>
  )
}

export function BotsFleetPanel({
  botStreamState,
  filteredBots,
  fleetState,
  fleetSummary,
  handleDelete,
  handleOpenCreate,
  handleStart,
  handleStop,
  handleViewReport,
  nowEpochMs,
  pendingDelete,
  pendingStart,
  pendingStop,
  manualRefreshFleet,
  refreshingFleet,
  runtimeCapacity,
  search,
  setDiagnosticsBotId,
  setLensBotId,
  setSearch,
  sortedBots,
  strategyLookup,
}) {
  return (
    <section className="space-y-4">
      <FleetHeader
        botStreamState={botStreamState}
        filteredBots={filteredBots}
        fleetSummary={fleetSummary}
        handleOpenCreate={handleOpenCreate}
        manualRefreshFleet={manualRefreshFleet}
        refreshingFleet={refreshingFleet}
        runtimeCapacity={runtimeCapacity}
        search={search}
        setSearch={setSearch}
        sortedBots={sortedBots}
      />

      <section className="space-y-2.5">
        <div className="flex items-end justify-between gap-3">
          <div>
            <p className="qt-ops-kicker">Fleet List</p>
            <h3 className="mt-1 text-base font-semibold text-slate-100">Operator-ready bot inventory</h3>
          </div>
        </div>

        {fleetState.mode === 'ready' ? (
          <div className="space-y-2.5">
            {filteredBots.map((bot) => (
              <BotFleetCard
                key={bot.id}
                bot={bot}
                strategyLookup={strategyLookup}
                nowEpochMs={nowEpochMs}
                onStart={handleStart}
                onStop={handleStop}
                onDelete={handleDelete}
                onOpenLens={(selectedBot) => setLensBotId(selectedBot?.id || null)}
                onOpenDiagnostics={(selectedBot) => setDiagnosticsBotId(selectedBot?.id || null)}
                onViewReport={handleViewReport}
                pendingStart={pendingStart}
                pendingStop={pendingStop}
                pendingDelete={pendingDelete}
              />
            ))}
          </div>
        ) : (
          <div className="qt-ops-shell px-5 py-10 text-center">
            <p className="text-base font-semibold text-slate-100">{fleetState.title}</p>
            <p className="mt-2 text-sm text-slate-400">{fleetState.detail}</p>
          </div>
        )}
      </section>
    </section>
  )
}
