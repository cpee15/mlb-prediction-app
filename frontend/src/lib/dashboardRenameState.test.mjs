import test from 'node:test'
import assert from 'node:assert/strict'

import {
  DASHBOARD_NAME_MAX_LENGTH,
  dashboardRenameRequest,
  normalizeDashboardName,
  renameKeyboardAction,
} from './dashboardRenameState.mjs'

test('builds an authenticated folder rename request without changing other folder fields', () => {
  const request = dashboardRenameRequest('folder', 12, '  Scouting Notes  ')
  assert.equal(request.path, '/my-dashboard/folders/12')
  assert.equal(request.options.method, 'PATCH')
  assert.deepEqual(JSON.parse(request.options.body), { folder_name: 'Scouting Notes' })
})

test('builds a report-title-only rename request', () => {
  const request = dashboardRenameRequest('item', 31, 'Best Hitters')
  assert.equal(request.path, '/my-dashboard/items/31')
  assert.deepEqual(JSON.parse(request.options.body), { title: 'Best Hitters' })
})

test('rejects blank and oversized names before sending a request', () => {
  assert.throws(() => normalizeDashboardName('   '), /required/)
  assert.throws(() => normalizeDashboardName('x'.repeat(DASHBOARD_NAME_MAX_LENGTH + 1)), /255/)
})

test('maps Enter to save and Escape to cancel', () => {
  assert.equal(renameKeyboardAction('Enter'), 'save')
  assert.equal(renameKeyboardAction('Escape'), 'cancel')
  assert.equal(renameKeyboardAction('Tab'), null)
})
