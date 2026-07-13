import test from 'node:test'
import assert from 'node:assert/strict'

import { normalizeQueryState, queryPayload, resultRange, serverFields } from './dashboardQueryState.mjs'

test('normalizes invalid query state', () => {
  assert.deepEqual(normalizeQueryState({ page_number: 0, page_size: 999, sort_direction: 'sideways' }), {
    page_number: 1,
    page_size: 50,
    sort_by: 'score',
    sort_direction: 'desc',
  })
})

test('calculates visible result range from server page info', () => {
  assert.deepEqual(resultRange({ page_number: 2, page_size: 50, record_count: 31 }, 81), { start: 51, end: 81, total: 81 })
})

test('uses server-owned field metadata', () => {
  const fields = serverFields({ object_info: { fields: [{ name: 'metrics.xwOBA', label: 'xwOBA', type: 'double', sortable: true, group: 'Metrics' }] } })
  assert.equal(fields[0].accessor, 'metrics.xwOBA')
  assert.equal(fields[0].sortable, true)
})

test('builds the complete solver query payload', () => {
  assert.deepEqual(queryPayload({ date: '2026-07-13', component: 'hitters', filters: { team: 'CHC' }, query: { page_number: 3, page_size: 100, sort_by: 'entity_name', sort_direction: 'asc' } }), {
    date: '2026-07-13',
    component: 'hitters',
    filters: { team: 'CHC' },
    page_size: 100,
    page_number: 3,
    sort_by: 'entity_name',
    sort_direction: 'asc',
    include_metadata: true,
  })
})
