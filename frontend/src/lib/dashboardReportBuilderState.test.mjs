import test from 'node:test'
import assert from 'node:assert/strict'

import {
  buildReportRequest,
  defaultFieldsForObject,
  initialFieldsByObject,
  normalizeCanonicalPage,
} from './dashboardReportBuilderState.mjs'

const query = { page_number: 1, page_size: 50, sort_by: 'score', sort_direction: 'desc' }

test('unfiltered hitters query the complete canonical active-player report', () => {
  const request = buildReportRequest({ objectKey: 'hitters', activeLineupsOnly: false, date: '2026-07-16', cleanedFilters: {}, query })
  assert.equal(request.path, '/my-dashboard/reports/query')
  assert.equal(request.payload.report_type, 'all_active_hitters')
  assert.deepEqual(request.payload.filters, {})
  assert.deepEqual(request.payload.weights, {})
  assert.equal(request.payload.sort_by, 'adjusted_score')
  assert.equal('date' in request.payload, false)
})

test('weights rerank canonical players without becoming filter criteria', () => {
  const request = buildReportRequest({
    objectKey: 'pitchers',
    activeLineupsOnly: false,
    date: '2026-07-16',
    cleanedFilters: { team: 'CHC', weights: { 'K%': 1.6 } },
    query,
  })
  assert.deepEqual(request.payload.filters, { team: 'CHC' })
  assert.deepEqual(request.payload.weights, { 'K%': 1.6 })
})

test('legacy and confirmed-lineup objects preserve their existing routes', () => {
  assert.equal(buildReportRequest({ objectKey: 'teams', activeLineupsOnly: false, date: '2026-07-16', cleanedFilters: {}, query }).path, '/my-dashboard/solver')
  assert.equal(buildReportRequest({ objectKey: 'hitters', activeLineupsOnly: true, date: '2026-07-16', cleanedFilters: {}, query }).path, '/my-dashboard/solver/active-lineups')
})

test('primary objects own independent default column selections', () => {
  assert.deepEqual(defaultFieldsForObject('hitters'), ['rank', 'full_name', 'team_name', 'model_score', 'confidence'])
  assert.deepEqual(defaultFieldsForObject('teams'), ['rank', 'entity_name', 'team', 'opponent', 'score', 'confidence'])
  const fields = initialFieldsByObject([{ key: 'hitters' }, { key: 'pitchers' }], {
    activeObject: 'hitters',
    selectedFields: ['rank', 'full_name'],
  })
  assert.deepEqual(fields.hitters, ['rank', 'full_name', 'team_name', 'model_score', 'confidence'])
  assert.deepEqual(fields.pitchers, ['rank', 'full_name', 'team_name', 'model_score', 'confidence'])
  fields.hitters.push('xwoba')
  assert.equal(fields.pitchers.includes('xwoba'), false)
})

test('canonical pagination is adapted to the Report Workspace contract', () => {
  const result = normalizeCanonicalPage({ totalSize: 125, records: Array(50).fill({}), page_info: { has_next_page: true } }, query)
  assert.equal(result.page_info.page_count, 3)
  assert.equal(result.page_info.record_count, 50)
  assert.equal(result.page_info.has_next, true)
  assert.equal(result.page_info.has_previous, false)
})
