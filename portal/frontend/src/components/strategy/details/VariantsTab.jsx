const formatVariantSummary = (variant) => {
  const filters = Array.isArray(variant?.output_filters) ? variant.output_filters : []
  return filters.map((filter, index) => {
    const scope = filter?.scope?.intent
      ? ` ${Array.isArray(filter.scope.intent) ? filter.scope.intent.join(',') : filter.scope.intent}`
      : ''
    return {
      id: `${variant?.id || variant?.name || 'variant'}-${index}`,
      label: `${filter?.indicator_id || '?'}:${filter?.output_name || '?'}.${filter?.field || '?'} ${filter?.operator || 'equals'} ${JSON.stringify(filter?.value)}${scope}`,
    }
  })
}

export const VariantsTab = ({
  strategy,
  onAddVariant,
  onEditVariant,
  onDeleteVariant,
  ActionButton,
}) => {
  const variants = Array.isArray(strategy?.variants) ? strategy.variants : []

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-white">Saved variants</h3>
          <p className="mt-1 text-xs text-slate-400">
            Decision filters over attached indicator outputs. ATM and risk are selected outside variants.
          </p>
        </div>
        <ActionButton onClick={onAddVariant}>
          New Variant
        </ActionButton>
      </div>

      {!variants.length ? (
        <div className="rounded-lg border border-dashed border-white/10 bg-black/20 px-4 py-6 text-center">
          <p className="text-sm font-medium text-slate-300">No variants saved</p>
          <p className="mt-1 text-xs text-slate-500">Create a variant to store output filters for this strategy.</p>
        </div>
      ) : (
        <div className="space-y-3">
          {variants.map((variant) => {
            const summary = formatVariantSummary(variant)

            return (
              <div
                key={variant.id || variant.name}
                className="rounded-xl border border-white/10 bg-black/20 p-4"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <h4 className="text-sm font-semibold text-white">{variant.name || 'Unnamed variant'}</h4>
                      {variant.is_default ? (
                        <span className="rounded-full bg-emerald-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-emerald-300">
                          Default
                        </span>
                      ) : null}
                    </div>
                    {variant.description ? (
                      <p className="mt-1 text-sm text-slate-300">{variant.description}</p>
                    ) : (
                      <p className="mt-1 text-sm text-slate-500">No description</p>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    <ActionButton variant="ghost" onClick={() => onEditVariant?.(variant)}>
                      Edit
                    </ActionButton>
                    <ActionButton
                      variant="danger"
                      onClick={() => onDeleteVariant?.(variant)}
                      disabled={Boolean(variant.is_default)}
                      title={variant.is_default ? 'Default variant cannot be deleted' : 'Delete variant'}
                      className={variant.is_default ? 'cursor-not-allowed opacity-50' : ''}
                    >
                      Delete
                    </ActionButton>
                  </div>
                </div>

                <div className="mt-3 rounded-lg border border-white/8 bg-white/[0.03] p-3">
                  <p className="text-[10px] font-semibold uppercase tracking-[0.28em] text-slate-500">
                    Output Filters
                  </p>
                  {!summary.length ? (
                    <p className="mt-2 text-xs text-slate-500">No additional decision filters.</p>
                  ) : (
                    <div className="mt-2 flex flex-wrap gap-2">
                      {summary.map((item) => (
                        <span
                          key={item.id}
                          className="rounded-md border border-white/10 bg-black/30 px-2 py-1 text-xs text-slate-300"
                        >
                          {item.label}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

export default VariantsTab
