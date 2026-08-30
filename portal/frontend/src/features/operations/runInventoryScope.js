export function buildRunInventoryScopeKey(definitions = []) {
  return definitions
    .map((definition) => [
      definition?.id,
      definition?.active_run_id,
      definition?.latest_run_id,
    ].join(':'))
    .sort()
    .join('|')
}
