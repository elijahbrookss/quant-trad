import test from 'node:test'
import assert from 'node:assert/strict'

import { addConditionRow, removeConditionRow, updateConditionRow } from '../src/components/strategy/conditions/conditionRowUtils.js'
import { buildFilterPreview, createEmptyPredicate } from '../src/components/strategy/filters/filterUtils.js'

test('condition row helpers add and remove rows', () => {
  const draft = {
    groupMode: 'all',
    predicates: [createEmptyPredicate()],
  }

  const added = addConditionRow(draft.predicates, () => ({ ...createEmptyPredicate(), path: '$.confidence' }))
  assert.equal(added.length, 2)

  const removed = removeConditionRow(added, 0, () => createEmptyPredicate())
  assert.equal(removed.length, 1)
  assert.equal(removed[0].path, '$.confidence')
})

test('condition row helpers update operator/value and preview updates', () => {
  const draft = {
    name: '',
    description: '',
    enabled: true,
    groupMode: 'all',
    predicates: [
      {
        source: 'regime_stats',
        path: '$.structure.state',
        operator: 'eq',
        value: 'bull',
        missing_data_policy: 'fail',
        stats_version: '',
        regime_version: '',
        fieldMode: 'preset',
      },
    ],
  }

  const initialPreview = buildFilterPreview(draft)
  assert.match(initialPreview, /Regime\.Trend state = bull/)

  const updatedPredicates = updateConditionRow(draft.predicates, 0, { operator: 'gt', value: 5 })
  const updatedPreview = buildFilterPreview({ ...draft, predicates: updatedPredicates })
  assert.match(updatedPreview, /Regime\.Trend state > 5/)
})
