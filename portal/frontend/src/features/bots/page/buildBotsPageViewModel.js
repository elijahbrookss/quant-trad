export function buildBotsPageViewModel({
  botStreamState,
  filteredBots,
  hasFleetSnapshot,
  lensBot,
  refreshingFleet,
  search,
  sortedBots,
}) {
  let fleetState = {
    mode: 'ready',
    title: '',
    detail: '',
  }

  const awaitingFleetSnapshot = !hasFleetSnapshot && sortedBots.length === 0

  if (refreshingFleet && sortedBots.length === 0) {
    fleetState = {
      mode: 'loading',
      title: 'Refreshing fleet…',
      detail: 'Manual refresh is fetching a fresh fleet snapshot over HTTP.',
    }
  } else if (awaitingFleetSnapshot) {
    fleetState = {
      mode: 'loading',
      title:
        botStreamState === 'error'
          ? 'Fleet stream unavailable.'
          : botStreamState === 'open'
            ? 'Waiting for live fleet state…'
            : 'Connecting to live fleet state…',
      detail:
        botStreamState === 'error'
          ? 'Fleet state has not been hydrated yet. Use manual refresh to resync once the backend is reachable.'
          : botStreamState === 'open'
            ? 'The stream is connected and waiting for the first fleet snapshot.'
            : 'Fleet state will populate from the bots stream as soon as the first snapshot arrives.',
    }
  } else if (filteredBots.length === 0) {
    fleetState = {
      mode: 'empty',
      title: search.trim() ? 'No bots match your filter.' : 'No bots configured.',
      detail: search.trim()
        ? 'Adjust the fleet filter or clear it to inspect available bots.'
        : 'Create your first bot to begin running a strategy.',
    }
  }

  const runtimeState = lensBot
    ? {
        mode: 'selected',
        title: lensBot.name || 'Selected runtime',
        detail: 'Runtime panels stay scoped to the selected bot run.',
      }
    : {
        mode: sortedBots.length ? 'idle' : awaitingFleetSnapshot ? 'loading' : 'empty',
        title: sortedBots.length
          ? 'Select a bot runtime'
          : awaitingFleetSnapshot
            ? 'Waiting for fleet state'
            : 'No runtime available',
        detail: sortedBots.length
          ? `Use “Open Runtime” on a bot row to inspect one run. Fleet stream is ${botStreamState}.`
          : awaitingFleetSnapshot
            ? 'The runtime workspace unlocks after the fleet stream publishes its first snapshot.'
            : 'Create and start a bot before opening the runtime workspace.',
      }

  return { fleetState, runtimeState }
}
