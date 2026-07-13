export const DEFAULT_PAGE_SIZE = 50
export const PAGE_SIZE_OPTIONS = [25, 50, 100, 250]

export function defaultQueryState() {
  return { page_number: 1, page_size: DEFAULT_PAGE_SIZE, sort_by: 'score', sort_direction: 'desc' }
}

export function normalizeQueryState(value = {}) {
  const pageSize = PAGE_SIZE_OPTIONS.includes(Number(value.page_size)) ? Number(value.page_size) : DEFAULT_PAGE_SIZE
  const pageNumber = Math.max(1, Number(value.page_number) || 1)
  const sortDirection = value.sort_direction === 'asc' ? 'asc' : 'desc'
  return {
    page_number: pageNumber,
    page_size: pageSize,
    sort_by: String(value.sort_by || 'score'),
    sort_direction: sortDirection,
  }
}

export function resultRange(pageInfo = {}, totalSize = 0) {
  const total = Number(totalSize) || 0
  const page = Math.max(1, Number(pageInfo.page_number) || 1)
  const size = Math.max(1, Number(pageInfo.page_size) || DEFAULT_PAGE_SIZE)
  const count = Math.max(0, Number(pageInfo.record_count) || 0)
  if (!total || !count) return { start: 0, end: 0, total }
  const start = ((page - 1) * size) + 1
  return { start, end: start + count - 1, total }
}

export function serverFields(result, fallback = []) {
  const fields = Array.isArray(result?.object_info?.fields) ? result.object_info.fields : []
  if (!fields.length) return fallback
  return fields.map(field => ({
    accessor: field.name,
    label: field.label || field.name,
    group: field.group || 'Other',
    type: field.type || 'string',
    sortable: field.sortable !== false,
    filterable: field.filterable !== false,
    nillable: field.nillable !== false,
  }))
}

export function queryPayload({ date, component, filters, query, includeMetadata = true }) {
  const normalized = normalizeQueryState(query)
  return {
    date,
    component,
    filters,
    page_size: normalized.page_size,
    page_number: normalized.page_number,
    sort_by: normalized.sort_by,
    sort_direction: normalized.sort_direction,
    include_metadata: includeMetadata,
  }
}
