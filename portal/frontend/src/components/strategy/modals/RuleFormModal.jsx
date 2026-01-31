import DropdownSelect from '../../ChartComponent/DropdownSelect.jsx'
import ActionButton from '../ui/ActionButton.jsx'
import useRuleForm from '../../../hooks/strategy/useRuleForm.js'

function RuleFormModal({
  open,
  indicators,
  ensureIndicatorMeta,
  initialValues,
  onSubmit,
  onCancel,
  submitting,
}) {
  const {
    form,
    indicatorMap,
    canSubmit,
    handleSubmit,
    handleFieldChange,
    addCondition,
    removeCondition,
    handleConditionIndicatorChange,
    handleConditionRuleChange,
    handleConditionDirectionChange,
  } = useRuleForm({
    open,
    indicators,
    ensureIndicatorMeta,
    initialValues,
    onSubmit,
  })

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 px-4 py-8">
      <div className="w-full max-w-4xl space-y-6 rounded-2xl border border-white/10 bg-[#14171f] p-6 text-slate-100 shadow-xl">
        <header className="space-y-1">
          <h3 className="text-lg font-semibold">
            {initialValues ? 'Edit rule' : 'Create rule'}
          </h3>
          <p className="text-sm text-slate-400">
            Build the decision logic that turns indicator signals into actionable entries.
          </p>
        </header>

        <form className="space-y-5" onSubmit={handleSubmit}>
          <div className="grid gap-4 md:grid-cols-[1.4fr_1fr]">
            <div className="space-y-4">
              <div>
                <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                  Name
                </label>
                <input
                  className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  value={form.name}
                  onChange={handleFieldChange('name')}
                  required
                />
              </div>

              <div>
                <label className="block text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                  Description
                </label>
                <textarea
                  className="mt-2 w-full rounded-lg border border-white/10 bg-black/40 px-3 py-2 text-sm focus:border-[color:var(--accent-alpha-40)] focus:outline-none"
                  rows={3}
                  value={form.description}
                  onChange={handleFieldChange('description')}
                />
              </div>
            </div>

            <div className="space-y-4 rounded-xl border border-white/10 bg-black/30 p-4">
              <p className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Rule Controls
              </p>
              <div className="grid gap-3">
                <DropdownSelect
                  label="Action"
                  value={form.action}
                  onChange={handleFieldChange('action')}
                  options={[
                    { value: 'buy', label: 'Buy' },
                    { value: 'sell', label: 'Sell' },
                  ]}
                />
                <DropdownSelect
                  label="Match"
                  value={form.match}
                  onChange={handleFieldChange('match')}
                  options={[
                    { value: 'all', label: 'All conditions' },
                    { value: 'any', label: 'Any condition' },
                  ]}
                />
              </div>
              <label className="inline-flex items-center gap-2 text-xs text-slate-300">
                <input
                  type="checkbox"
                  className="h-4 w-4 rounded border-white/20 bg-black/40"
                  checked={form.enabled}
                  onChange={handleFieldChange('enabled')}
                />
                Enabled
              </label>
            </div>
          </div>

          <div className="space-y-4 rounded-xl border border-white/10 bg-black/30 p-4">
            <div className="flex items-center justify-between">
              <h4 className="text-xs font-semibold uppercase tracking-[0.3em] text-slate-400">
                Conditions
              </h4>
              <ActionButton type="button" variant="ghost" onClick={addCondition}>
                Add condition
              </ActionButton>
            </div>

            {form.conditions.map((condition, index) => {
              const indicatorMeta = indicatorMap.get(condition.indicator_id)
              const ruleOptions = Array.isArray(indicatorMeta?.signal_rules)
                ? indicatorMeta.signal_rules
                : []
              const selectedRule = ruleOptions.find((rule) => rule.id === condition.rule_id)
              const directionOptions = Array.isArray(selectedRule?.directions)
                ? selectedRule.directions
                : []

              return (
                <div
                  key={`condition-${index}`}
                  className="space-y-3 rounded-xl border border-white/10 bg-black/50 p-4"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex-1 space-y-3">
                      <DropdownSelect
                        label="Indicator"
                        value={condition.indicator_id}
                        onChange={handleConditionIndicatorChange(index)}
                        placeholder="Select indicator"
                        options={indicators.map((indicator) => {
                          const hasSignals = Array.isArray(indicator.signal_rules)
                            && indicator.signal_rules.length > 0
                          const label = indicator.name || indicator.type
                          return {
                            value: indicator.id,
                            label: hasSignals ? label : `${label} (no signals)`,
                            disabled: !hasSignals,
                          }
                        })}
                      />

                      {condition.indicator_id && (
                        <div className="space-y-2">
                          <DropdownSelect
                            label="Signal"
                            value={condition.rule_id || condition.signal_type}
                            onChange={handleConditionRuleChange(index)}
                            placeholder="Select signal"
                            options={ruleOptions.map((rule) => ({
                              value: rule.id,
                              label: rule.label || rule.signal_type,
                            }))}
                          />
                          {selectedRule ? (
                            <p className="text-xs text-slate-400">
                              Selected signal:&nbsp;
                              <span className="text-slate-200">
                                {selectedRule.label || selectedRule.signal_type}
                              </span>
                            </p>
                          ) : null}
                        </div>
                      )}

                      {directionOptions.length > 0 && (
                        <DropdownSelect
                          label="Direction"
                          value={condition.direction}
                          onChange={handleConditionDirectionChange(index)}
                          placeholder="Select direction"
                          options={directionOptions.map((direction) => ({
                            value: direction.id,
                            label: direction.label || direction.id,
                          }))}
                        />
                      )}
                    </div>

                    <ActionButton
                      type="button"
                      variant="subtle"
                      onClick={() => removeCondition(index)}
                    >
                      Remove
                    </ActionButton>
                  </div>
                </div>
              )
            })}
          </div>

          <footer className="flex items-center justify-end gap-2">
            <ActionButton type="button" variant="ghost" onClick={onCancel}>
              Cancel
            </ActionButton>
            <ActionButton type="submit" disabled={submitting || !canSubmit}>
              {submitting ? 'Saving…' : 'Save rule'}
            </ActionButton>
          </footer>
        </form>
      </div>
    </div>
  )
}

export default RuleFormModal
