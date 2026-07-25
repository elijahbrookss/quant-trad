import { BotCreateModal } from '../create/BotCreateModal.jsx'
import { BotConfigModal } from '../config/BotConfigModal.jsx'
import { BotDiagnosticsModal } from '../diagnostics/BotDiagnosticsModal.jsx'
import { BotLensRuntimeContainer } from '../botlens/BotLensRuntimeContainer.jsx'
import { buildBotsPageViewModel } from './buildBotsPageViewModel.js'
import { BotsFleetPanel } from './components/BotsFleetPanel.jsx'

export function BotsPageView({
  botStreamState,
  closeCreateModal,
  createError,
  createOpen,
  configBot,
  configBotActive,
  configSaving,
  diagnosticsBot,
  error,
  filteredBots,
  fleetSummary,
  form,
  handleBacktestRangeChange,
  handleChange,
  handleCreate,
  handleDelete,
  handleOpenConfig,
  handleSaveConfig,
  handleOpenCreate,
  handleStart,
  handleStop,
  handleViewReport,
  handleStrategySelect,
  handleVariantSelect,
  handleWalletBalanceAdd,
  handleWalletBalanceChange,
  handleWalletBalanceRemove,
  hasFleetSnapshot,
  lensBot,
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
  sortedStrategies,
  strategiesLoading,
  strategyError,
  strategyLookup,
  walletError,
}) {
  const viewModel = buildBotsPageViewModel({
    botStreamState,
    filteredBots,
    hasFleetSnapshot,
    lensBot,
    refreshingFleet,
    search,
    sortedBots,
  })

  return (
    <section className="mx-auto max-w-[1380px] space-y-5">
      {error ? (
        <div className="qt-ops-panel border-rose-900/60 bg-rose-950/20 px-4 py-3 text-sm text-rose-300">
          {error}
        </div>
      ) : null}

      <BotsFleetPanel
        botStreamState={botStreamState}
        filteredBots={filteredBots}
        fleetState={viewModel.fleetState}
        fleetSummary={fleetSummary}
        handleDelete={handleDelete}
        handleOpenCreate={handleOpenCreate}
        handleOpenConfig={handleOpenConfig}
        handleStart={handleStart}
        handleStop={handleStop}
        handleViewReport={handleViewReport}
        pendingStop={pendingStop}
        refreshingFleet={refreshingFleet}
        nowEpochMs={nowEpochMs}
        pendingDelete={pendingDelete}
        pendingStart={pendingStart}
        manualRefreshFleet={manualRefreshFleet}
        runtimeCapacity={runtimeCapacity}
        search={search}
        setDiagnosticsBotId={setDiagnosticsBotId}
        setLensBotId={setLensBotId}
        setSearch={setSearch}
        sortedBots={sortedBots}
        strategyLookup={strategyLookup}
      />

      <BotConfigModal
        active={configBotActive}
        bot={configBot}
        canUpdate={Boolean(handleSaveConfig)}
        onClose={() => handleOpenConfig(null)}
        onSave={handleSaveConfig}
        open={Boolean(configBot)}
        saving={configSaving}
        strategy={configBot?.strategy_id ? strategyLookup.get(configBot.strategy_id) : null}
      />
      <BotDiagnosticsModal bot={diagnosticsBot} open={Boolean(diagnosticsBot)} onClose={() => setDiagnosticsBotId(null)} />
      {lensBot ? <BotLensRuntimeContainer bot={lensBot} open={Boolean(lensBot)} onClose={() => setLensBotId(null)} /> : null}
      <BotCreateModal
        open={createOpen}
        onClose={closeCreateModal}
        form={form}
        strategies={sortedStrategies}
        strategiesLoading={strategiesLoading}
        strategyError={strategyError}
        walletError={walletError}
        onSubmit={handleCreate}
        onChange={handleChange}
        onBacktestRangeChange={handleBacktestRangeChange}
        onStrategySelect={handleStrategySelect}
        onVariantSelect={handleVariantSelect}
        onWalletBalanceChange={handleWalletBalanceChange}
        onWalletBalanceAdd={handleWalletBalanceAdd}
        onWalletBalanceRemove={handleWalletBalanceRemove}
        error={createError}
      />
    </section>
  )
}
