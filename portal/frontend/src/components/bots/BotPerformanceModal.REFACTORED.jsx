/**
 * REFACTORED BotPerformanceModal - Decision Trace Focused
 *
 * This is the refactored version emphasizing decision trace over performance stats.
 *
 * Layout hierarchy (top to bottom):
 * 1. Header (minimal - bot name + status)
 * 2. Chart (price context, ~300px height)
 * 3. Decision Trace (PRIMARY FOCUS - chronological decision ledger)
 * 4. Strategy Configuration (collapsible, de-emphasized)
 *
 * Instructions for integration:
 * 1. Import DecisionTrace at top of file
 * 2. Replace the modal body section (after header, before closing div)
 * 3. Remove PerformanceStats component
 * 4. Simplify PlaybackControls (keep only pause/resume)
 */

// ADD THIS IMPORT AT TOP OF FILE:
// import DecisionTrace from './DecisionTrace';

// REPLACE THE MODAL BODY SECTION WITH THIS:

{/* Modal Body - Refactored Layout */}
<div className="flex flex-1 flex-col gap-4 overflow-auto">
  {/* Minimal Status Indicators */}
  <BotStatusChips
    statusDisplay={statusDisplay}
    progressDisplay={progressDisplay}
    streamStatus={streamStatus}
  />

  {/* Minimal Playback Controls (pause/resume only) */}
  {(canPause || canResume) && (
    <div className="flex items-center justify-between gap-4 rounded-2xl border border-white/10 bg-white/5 px-4 py-2">
      <div className="flex items-center gap-2">
        {canResume && (
          <button
            type="button"
            onClick={handleResume}
            disabled={action === 'resuming'}
            className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-sm text-emerald-200 hover:border-emerald-500/50 hover:bg-emerald-500/20 disabled:opacity-50"
          >
            {action === 'resuming' ? 'Resuming...' : 'Resume'}
          </button>
        )}
        {canPause && (
          <button
            type="button"
            onClick={handlePause}
            disabled={action === 'pausing'}
            className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-sm text-amber-200 hover:border-amber-500/50 hover:bg-amber-500/20 disabled:opacity-50"
          >
            {action === 'pausing' ? 'Pausing...' : 'Pause'}
          </button>
        )}
      </div>
      {simTimeLabel && (
        <span className="text-xs text-slate-400">{simTimeLabel}</span>
      )}
    </div>
  )}

  {/* Symbol Tabs (if multi-instrument) */}
  {seriesSymbols.length > 1 && (
    <div className="flex flex-wrap items-center gap-2">
      {seriesSymbols.map((symbol) => (
        <button
          key={`bot-series-${symbol}`}
          type="button"
          onClick={() => setActiveSymbol(symbol)}
          className={`rounded-full border px-3 py-1 text-[11px] uppercase tracking-[0.3em] ${
            symbol === activeSymbol
              ? 'border-[color:var(--accent-alpha-60)] bg-[color:var(--accent-alpha-20)] text-white'
              : 'border-white/10 bg-black/20 text-slate-300 hover:border-white/30 hover:text-white'
          }`}
        >
          {symbol}
        </button>
      ))}
    </div>
  )}

  {/* CHART SECTION - Supporting Context (reduced height) */}
  <section className="chart-section">
    <div className="relative min-h-[300px]">
      <div
        className={`absolute inset-0 z-10 flex items-center justify-center transition-opacity duration-300 ${
          bootOverlayVisible ? 'opacity-100' : 'pointer-events-none opacity-0'
        }`}
      >
        <div className="rounded-full border border-white/10 bg-white/5 px-4 py-3 text-base font-semibold text-slate-100 shadow-sm animate-pulse">
          {bootLineDisplay}
        </div>
      </div>
      <div
        className={`transition-opacity duration-300 ${
          bootOverlayVisible ? 'pointer-events-none opacity-0' : 'opacity-100'
        }`}
      >
        {!bootOverlayVisible && loading ? <LoadingOverlay label={loadingLabel} /> : null}
        {error ? (
          <div className="rounded-2xl border border-rose-500/40 bg-rose-500/5 p-4 text-sm text-rose-200">{error}</div>
        ) : showInactiveState ? (
          <div className="flex h-[300px] items-center justify-center rounded-2xl border border-dashed border-white/10 bg-black/30 p-6 text-center text-sm text-slate-400">
            {idleMessage}
          </div>
        ) : chartHasData ? (
          <BotLensChart
            chartId={activeChartId}
            candles={activeSeries?.candles || []}
            trades={activeTrades}
            overlays={activeSeries?.overlays || []}
            playbackSpeed={playbackDraft}
          />
        ) : (
          <div className="flex h-[300px] items-center justify-center rounded-2xl border border-dashed border-white/10 bg-black/30 p-6 text-center text-sm text-slate-400">
            Awaiting the first candle…
          </div>
        )}
      </div>
    </div>
  </section>

  {/* PRIMARY FOCUS: DECISION TRACE */}
  <section className="decision-trace-section">
    <DecisionTrace
      decisions={payload?.decisions || []}
      trades={activeTrades}
      onEventClick={(barTime) => {
        // Focus chart on this timestamp
        const chart = getChart(activeChartId);
        if (chart?.api) {
          try {
            const timestamp = new Date(barTime).getTime() / 1000;
            chart.api.timeScale().scrollToPosition(timestamp, true);
          } catch (err) {
            console.warn('Failed to focus chart on decision:', err);
          }
        }
      }}
    />
  </section>

  {/* DE-EMPHASIZED: Strategy Configuration (collapsible) */}
  {strategies.length > 0 && (
    <details className="group rounded-3xl border border-white/5 bg-black/30">
      <summary className="cursor-pointer p-4 hover:bg-white/5">
        <div className="flex items-center justify-between">
          <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">
            Strategy Configuration
          </p>
          <span className="text-xs text-slate-400 group-open:hidden">
            Click to expand
          </span>
        </div>
      </summary>
      <div className="space-y-3 px-4 pb-4">
        {strategies.map((strategy) => {
          const summarySymbols = symbolsFromInstruments(strategy.instruments).join(', ');
          const timeframeLabel = strategy.timeframe || '—';
          const datasourceLabel = strategy.datasource || '—';
          const exchangeLabel = strategy.exchange || '—';
          const primaryInstrument = strategy.instruments?.[0];
          const contractSize = primaryInstrument?.contract_size ?? strategy.atm_template?.contract_size ?? '—';
          const rrDisplay = formatRiskReward(strategy.atm_metrics);

          return (
            <article key={strategy.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <h4 className="text-lg font-semibold text-white">{strategy.name || 'Unnamed strategy'}</h4>
                  <p className="font-mono text-[11px] uppercase tracking-[0.3em] text-slate-500">{strategy.id}</p>
                </div>
              </div>
              <dl className="mt-3 grid gap-3 text-xs text-slate-400 sm:grid-cols-4">
                <div>
                  <dt className="uppercase tracking-[0.3em]">Symbols</dt>
                  <dd className="text-sm text-white">{summarySymbols}</dd>
                </div>
                <div>
                  <dt className="uppercase tracking-[0.3em]">Timeframe</dt>
                  <dd className="text-sm text-white">{timeframeLabel}</dd>
                </div>
                <div>
                  <dt className="uppercase tracking-[0.3em]">Datasource / Exch.</dt>
                  <dd className="text-sm text-white">{datasourceLabel} / {exchangeLabel}</dd>
                </div>
                <div>
                  <dt className="uppercase tracking-[0.3em]">Contract & R:R</dt>
                  <dd className="text-sm text-white">
                    {contractSize} / {rrDisplay}
                  </dd>
                </div>
              </dl>

              {/* ATM Template (if available) */}
              {strategy.atm_template && (
                <div className="mt-4 border-t border-white/10 pt-4">
                  <ATMTemplateSummary template={strategy.atm_template} />
                </div>
              )}
            </article>
          );
        })}
      </div>
    </details>
  )}

  {/* Trade Log (kept for reference, but de-emphasized) */}
  <details className="group rounded-3xl border border-white/5 bg-black/30">
    <summary className="cursor-pointer p-4 hover:bg-white/5">
      <div className="flex items-center justify-between">
        <p className="text-[11px] uppercase tracking-[0.35em] text-[color:var(--accent-text-kicker)]">
          Execution Log
        </p>
        <span className="text-xs text-slate-400 group-open:hidden">
          {logs.length} events
        </span>
      </div>
    </summary>
    <div className="px-4 pb-4">
      <TradeLogList
        logs={logs}
        activeTab={logTab}
        onTabChange={setLogTab}
        quoteCurrency={quoteCurrency}
        onLogClick={(log) => {
          const chart = getChart(activeChartId);
          if (chart?.api && log.bar_time) {
            try {
              const timestamp = new Date(log.bar_time).getTime() / 1000;
              chart.api.timeScale().scrollToPosition(timestamp, true);
            } catch (err) {
              console.warn('Failed to focus chart on log:', err);
            }
          }
        }}
      />
    </div>
  </details>
</div>
