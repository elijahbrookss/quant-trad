import { ChevronLeft, ChevronRight } from 'lucide-react'

export function paginateRows(rows = [], page = 1, pageSize = 12) {
  const pageCount = Math.max(1, Math.ceil(rows.length / pageSize))
  const safePage = Math.min(Math.max(1, page), pageCount)
  const start = (safePage - 1) * pageSize
  return {
    rows: rows.slice(start, start + pageSize),
    page: safePage,
    pageCount,
    start: rows.length ? start + 1 : 0,
    end: Math.min(start + pageSize, rows.length),
    total: rows.length,
  }
}

export function Pagination({ page, pageCount, start, end, total, onChange }) {
  if (total <= 0) return null
  return (
    <div className="qt2-pagination" aria-label="Inventory pagination">
      <span>{start}–{end} of {total}</span>
      <div>
        <button type="button" disabled={page <= 1} onClick={() => onChange(page - 1)} aria-label="Previous page"><ChevronLeft size={14} /></button>
        <span>Page {page} of {pageCount}</span>
        <button type="button" disabled={page >= pageCount} onClick={() => onChange(page + 1)} aria-label="Next page"><ChevronRight size={14} /></button>
      </div>
    </div>
  )
}
