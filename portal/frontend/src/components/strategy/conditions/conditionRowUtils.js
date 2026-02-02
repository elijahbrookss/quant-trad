export const addConditionRow = (rows, createRow) => [...rows, createRow()]

export const removeConditionRow = (rows, index, createRow) => {
  const next = rows.filter((_, idx) => idx !== index)
  return next.length ? next : [createRow()]
}

export const updateConditionRow = (rows, index, updates) =>
  rows.map((row, idx) => (idx === index ? { ...row, ...updates } : row))
